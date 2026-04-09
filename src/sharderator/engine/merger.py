"""Merge pipeline — multi-index reindex for v2."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.client.queries import (
    enrich_index_info,
    get_data_stream_membership,
    get_warm_hot_nodes,
)
from sharderator.engine.merge_analyzer import MergeGroup, union_mappings
from sharderator.engine.restorer import InsufficientDiskError
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)

RESTORE_PREFIX = "sharderator-restore-"
MERGED_PREFIX = "sharderator-merged-"
RESTORE_BATCH_SIZE = 3


def analyze_merge_group(
    client: Elasticsearch,
    group: MergeGroup,
    job: JobRecord,
) -> list[IndexInfo]:
    """Enrich all source indices in the merge group."""
    job.transition(JobState.ANALYZING)

    ds_membership = get_data_stream_membership(client)

    enriched = []
    total_docs = 0
    for info in group.source_indices:
        info = enrich_index_info(client, info, ds_membership)
        enriched.append(info)
        total_docs += info.doc_count

    job.expected_doc_count = total_docs
    job.source_frozen_indices = [i.name for i in enriched]

    repos = {i.repository_name for i in enriched if i.repository_name}
    if not repos:
        raise RuntimeError("No source snapshots found for merge group")
    job.repository_name = next(iter(repos))

    job.enriched_metadata = [
        {
            "name": i.name,
            "shard_count": i.shard_count,
            "doc_count": i.doc_count,
            "store_size_bytes": i.store_size_bytes,
            "snapshot_name": i.snapshot_name,
            "repository_name": i.repository_name,
            "original_index_name": i.original_index_name,
            "is_data_stream_member": i.is_data_stream_member,
            "data_stream_name": i.data_stream_name,
            "has_custom_routing": i.has_custom_routing,
            "mappings": i.mappings,
            "settings": i.settings,
        }
        for i in enriched
    ]

    log.info(
        "merge_analysis_complete",
        group=group.base_pattern,
        bucket=group.time_bucket,
        indices=len(enriched),
        total_docs=total_docs,
    )
    return enriched


def _check_merge_disk_space(
    client: Elasticsearch,
    enriched: list[IndexInfo],
    safety_margin: float = 0.30,
) -> None:
    """HARD GATE: Refuse if working tier can't hold sources + merged target (~2x)."""
    nodes = get_warm_hot_nodes(client)
    if not nodes:
        raise InsufficientDiskError("No warm/hot/content nodes found in cluster")

    total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
    total_avail = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)

    source_total = sum(i.store_size_bytes for i in enriched)
    needed = source_total * 2

    avail_after = total_avail - needed
    pct_free_after = avail_after / total_disk if total_disk > 0 else 0

    if pct_free_after < safety_margin:
        raise InsufficientDiskError(
            f"Merge of {len(enriched)} indices needs ~{needed / (1024**3):.1f} GB "
            f"(2x source size) but working tier only has "
            f"{total_avail / (1024**3):.1f} GB free. "
            f"After operation, free space would be {pct_free_after:.0%} "
            f"(safety margin requires {safety_margin:.0%} free). "
            f"Select fewer merge groups or add warm/hot capacity."
        )

    log.info(
        "merge_disk_check_passed",
        needed_gb=round(needed / (1024**3), 1),
        avail_gb=round(total_avail / (1024**3), 1),
    )


def restore_merge_sources(
    client: Elasticsearch,
    enriched: list[IndexInfo],
    group: MergeGroup,
    job: JobRecord,
    working_tier: str,
    timeout_minutes: int = 30,
    progress_callback=None,
    restore_batch_size: int = RESTORE_BATCH_SIZE,
    safety_margin: float = 0.30,
) -> list[str]:
    """Restore all source indices in batches to avoid overwhelming the cluster."""
    job.transition(JobState.RESTORING)

    # HARD GATE
    _check_merge_disk_space(client, enriched, safety_margin)

    bucket_slug = group.time_bucket.replace(".", "")

    restore_plan: list[tuple[IndexInfo, str]] = []
    all_restore_names: list[str] = []
    for info in enriched:
        restore_name = f"{RESTORE_PREFIX}{bucket_slug}-{info.original_index_name}"
        all_restore_names.append(restore_name)
        restore_plan.append((info, restore_name))

    job.restore_indices = all_restore_names

    total_batches = (len(restore_plan) + restore_batch_size - 1) // restore_batch_size

    for batch_num in range(total_batches):
        batch_start = batch_num * restore_batch_size
        batch = restore_plan[batch_start : batch_start + restore_batch_size]

        log.info(
            "restore_batch_start",
            batch=batch_num + 1,
            total_batches=total_batches,
            indices=len(batch),
        )

        batch_names = []
        for info, restore_name in batch:
            if client.indices.exists(index=restore_name):
                log.warning("restore_exists_deleting", index=restore_name)
                client.indices.delete(index=restore_name)

            body = {
                "indices": info.original_index_name,
                "rename_pattern": "(.+)",
                "rename_replacement": restore_name,
                "index_settings": {
                    "index.routing.allocation.include._tier_preference": working_tier,
                    "index.number_of_replicas": 0,
                },
                "ignore_index_settings": [
                    "index.routing.allocation.include._tier_preference",
                ],
            }
            client.snapshot.restore(
                repository=info.repository_name,
                snapshot=info.snapshot_name,
                body=body,
                wait_for_completion=False,
            )
            batch_names.append(restore_name)

        # Wait for this batch to go green before starting the next
        _wait_for_batch_recovery(client, batch_names, timeout_minutes)

        if progress_callback:
            completed = batch_start + len(batch)
            progress_callback(completed / len(restore_plan) * 100)

    job.transition(JobState.AWAITING_RECOVERY)
    log.info("all_merge_sources_recovered", count=len(all_restore_names))
    return all_restore_names


def _wait_for_batch_recovery(
    client: Elasticsearch,
    index_names: list[str],
    timeout_minutes: int = 30,
) -> None:
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        all_green = True
        for name in index_names:
            try:
                health = client.cluster.health(
                    index=name, wait_for_status="green", timeout="5s"
                )
                if health.get("status") != "green":
                    all_green = False
            except Exception:
                all_green = False
        if all_green:
            return
        time.sleep(10)
    raise RuntimeError(
        f"Batch recovery timeout after {timeout_minutes}m for: {index_names}"
    )


def merge_reindex(
    client: Elasticsearch,
    restore_names: list[str],
    enriched: list[IndexInfo],
    group: MergeGroup,
    job: JobRecord,
    progress_callback=None,
    requests_per_second: float = -1,
) -> str:
    """Reindex all restored indices into a single merged index."""
    job.transition(JobState.MERGING)

    merged_name = f"{MERGED_PREFIX}{group.base_pattern}-{group.time_bucket}"
    job.shrunk_index = merged_name

    all_mappings = [i.mappings for i in enriched]
    merged_mapping = union_mappings(all_mappings)
    merged_analysis = _union_analysis_settings(enriched)

    create_body: dict = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "codec": "best_compression",
        },
        "mappings": merged_mapping,
    }
    if merged_analysis:
        create_body["settings"]["analysis"] = merged_analysis

    if client.indices.exists(index=merged_name):
        log.warning("merged_index_exists_deleting", index=merged_name)
        client.indices.delete(index=merged_name)

    client.indices.create(index=merged_name, body=create_body)

    log.info("merge_reindex_start", sources=len(restore_names), target=merged_name)
    rps = requests_per_second if requests_per_second > 0 else None
    resp = client.reindex(
        body={
            "source": {"index": restore_names},
            "dest": {"index": merged_name},
        },
        wait_for_completion=False,
        requests_per_second=rps,
        request_timeout=30,
    )
    task_id = resp.get("task", "")
    job.merge_task_id = task_id

    job.transition(JobState.AWAITING_MERGE)
    _wait_for_task(client, task_id, progress_callback)

    log.info("merge_reindex_complete", target=merged_name)
    return merged_name


def _wait_for_task(
    client: Elasticsearch,
    task_id: str,
    progress_callback=None,
    timeout_minutes: int = 120,
) -> None:
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        try:
            task = client.tasks.get(task_id=task_id)
            if task.get("completed"):
                error = task.get("error")
                if error:
                    if isinstance(error, dict):
                        reason = error.get("reason", "")
                        err_type = error.get("type", "unknown")
                        caused_by = error.get("caused_by", {})
                        caused_reason = (
                            caused_by.get("reason", "")
                            if isinstance(caused_by, dict)
                            else ""
                        )
                        detail = f"{err_type}: {reason}"
                        if caused_reason:
                            detail += f" (caused by: {caused_reason})"
                    else:
                        detail = str(error)
                    raise RuntimeError(f"Reindex task failed: {detail}")

                response = task.get("response", {})
                failures = response.get("failures", [])
                if failures:
                    sample = failures[0]
                    cause = sample.get("cause", {})
                    raise RuntimeError(
                        f"Reindex completed with {len(failures)} doc failures. "
                        f"First: {cause.get('type', '?')}: {cause.get('reason', '?')}"
                    )
                return

            if progress_callback:
                status = task.get("task", {}).get("status", {})
                total = status.get("total", 0)
                created = status.get("created", 0)
                if total > 0:
                    progress_callback(created / total * 100)
        except RuntimeError:
            raise
        except Exception:
            pass

        time.sleep(10)

    raise RuntimeError(f"Reindex task timeout after {timeout_minutes}m")


def _union_analysis_settings(enriched: list[IndexInfo]) -> dict:
    merged: dict = {}
    for info in enriched:
        analysis = info.settings.get("analysis", {})
        if not analysis:
            continue
        for section in ("normalizer", "analyzer", "tokenizer", "filter", "char_filter"):
            if section not in analysis:
                continue
            if section not in merged:
                merged[section] = {}
            for name, definition in analysis[section].items():
                if name not in merged[section]:
                    merged[section][name] = definition
    return merged
