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
from sharderator.engine.sizing import compute_dynamic_batch
from sharderator.engine.task_waiter import wait_for_task, TransientReindexError
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)

RESTORE_PREFIX = "sharderator-restore-"
MERGED_PREFIX = "sharderator-merged-"
RESTORE_BATCH_SIZE = 3
MAX_REINDEX_RETRIES = 3


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

    # Dynamic batching: use disk budget instead of fixed count
    nodes = get_warm_hot_nodes(client)
    total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
    total_avail = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)
    batches = compute_dynamic_batch(restore_plan, total_avail, safety_margin, total_disk)

    # Fall back to fixed batch size if dynamic produces fewer batches than expected
    if len(batches) == 1 and len(restore_plan) > restore_batch_size:
        # Dynamic says everything fits, but respect the fixed cap too
        fixed_batches = []
        for i in range(0, len(restore_plan), restore_batch_size):
            fixed_batches.append(restore_plan[i : i + restore_batch_size])
        if len(fixed_batches) > len(batches):
            batches = fixed_batches

    total_batches = len(batches)
    completed_count = 0

    for batch_num, batch in enumerate(batches):
        log.info(
            "restore_batch_start",
            batch=batch_num + 1,
            total_batches=total_batches,
            indices=len(batch),
        )

        # Recheck disk before each batch (not just the first)
        if batch_num > 0:
            _check_merge_disk_space_incremental(
                client,
                [info for info, _ in batch],
                safety_margin,
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
                "include_aliases": False,
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
            completed_count += len(batch)
            progress_callback(completed_count / len(restore_plan) * 100)

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
    max_retries: int = MAX_REINDEX_RETRIES,
) -> str:
    """Reindex all restored indices into a single merged index.

    On transient failures (unavailable shards, node disconnects), waits for
    the target index to recover and retries with conflicts:proceed to fill
    in the gaps. Permanent failures (mapping errors, etc.) fail immediately.
    """
    job.transition(JobState.MERGING)

    merged_name = f"{MERGED_PREFIX}{group.base_pattern}-{group.time_bucket}"
    job.shrunk_index = merged_name

    all_mappings = [i.mappings for i in enriched]
    merged_mapping, mapping_conflicts = union_mappings(all_mappings)
    merged_analysis, analysis_conflicts = _union_analysis_settings(enriched)

    # Log any field type conflicts so the operator knows what was silently resolved
    for conflict in mapping_conflicts:
        log.warning(
            "mapping_type_conflict",
            group=group.base_pattern,
            bucket=group.time_bucket,
            conflict=conflict,
        )
    for conflict in analysis_conflicts:
        log.warning(
            "analysis_settings_conflict",
            group=group.base_pattern,
            bucket=group.time_bucket,
            conflict=conflict,
        )

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

    rps = requests_per_second if requests_per_second > 0 else None

    # Item 3: Recheck disk before reindex — working tier now holds restored
    # data and the target index is about to grow to match
    _check_merge_disk_space_incremental(client, enriched, safety_margin=0.15)

    last_error = None

    for attempt in range(1, max_retries + 1):
        is_retry = attempt > 1

        if is_retry:
            # Wait for target index to be green before retrying
            log.info(
                "reindex_retry_waiting_for_green",
                index=merged_name,
                attempt=attempt,
            )
            _wait_for_green(client, merged_name, timeout_minutes=10)

        log.info(
            "merge_reindex_start",
            sources=len(restore_names),
            target=merged_name,
            attempt=attempt,
            retry=is_retry,
        )

        body: dict = {
            "source": {
                "index": restore_names,
            },
            "dest": {"index": merged_name},
        }

        if is_retry:
            # On retry, use conflicts:proceed so already-indexed docs are
            # skipped (409 version conflict) rather than failing the task.
            body["conflicts"] = "proceed"

        resp = client.reindex(
            body=body,
            wait_for_completion=False,
            requests_per_second=rps,
            request_timeout=30,
            scroll="30m",
        )
        task_id = resp.get("task", "")
        job.merge_task_id = task_id

        if attempt == 1:
            job.transition(JobState.AWAITING_MERGE)

        try:
            wait_for_task(client, task_id, progress_callback)
            # Success — verify doc count on the target
            _verify_reindex_count(client, merged_name, job.expected_doc_count)
            log.info("merge_reindex_complete", target=merged_name, attempts=attempt)
            return merged_name
        except TransientReindexError as e:
            last_error = e
            log.warning(
                "reindex_transient_failure",
                attempt=attempt,
                max_retries=max_retries,
                failures=e.failure_count,
                created=e.created,
                total=e.total,
            )
            if attempt < max_retries:
                # Brief pause for cluster to stabilize
                log.info("reindex_retry_backoff", seconds=30 * attempt)
                time.sleep(30 * attempt)
                continue
            else:
                raise RuntimeError(
                    f"Reindex failed after {max_retries} attempts due to transient "
                    f"errors. Last: {e}"
                )

    # Should not be reachable, but just in case
    raise RuntimeError(f"Reindex failed after {max_retries} attempts. Last: {last_error}")


def _verify_reindex_count(
    client: Elasticsearch,
    index: str,
    expected: int,
) -> None:
    """Quick sanity check on the merged index doc count after reindex.

    Allows a small tolerance (0.01%) because conflicts:proceed retries may
    see slightly different counts due to timing. A large mismatch still fails.
    """
    client.indices.refresh(index=index)
    count_resp = client.count(index=index)
    actual = count_resp.get("count", 0)
    if actual == expected:
        return
    diff_pct = abs(actual - expected) / max(expected, 1) * 100
    if diff_pct > 0.01:
        log.warning(
            "reindex_count_mismatch",
            index=index,
            expected=expected,
            actual=actual,
            diff_pct=round(diff_pct, 3),
        )
        # Don't fail — the verifier will do the authoritative check later.
        # But log it so the operator knows.


def _wait_for_green(
    client: Elasticsearch, index: str, timeout_minutes: int = 10
) -> None:
    """Wait for an index to return to green health after a transient failure."""
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        try:
            health = client.cluster.health(
                index=index, wait_for_status="green", timeout="30s"
            )
            if health.get("status") == "green":
                log.info("index_green_after_recovery", index=index)
                return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"Index {index} did not return to green after {timeout_minutes}m")


def _union_analysis_settings(enriched: list[IndexInfo]) -> tuple[dict, list[str]]:
    """Build superset of analysis settings across all source indices.

    Returns (merged_analysis, conflicts). First-seen-wins on name collisions.
    Conflicts are logged so the operator knows what was silently resolved.
    """
    merged: dict = {}
    conflicts: list[str] = []

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
                elif merged[section][name] != definition:
                    conflicts.append(
                        f"{section} '{name}': config from {info.name} "
                        f"differs from previously seen definition, kept first"
                    )

    return merged, conflicts


def _check_merge_disk_space_incremental(
    client: Elasticsearch,
    indices: list[IndexInfo],
    safety_margin: float = 0.15,
) -> None:
    """Continuous disk check before each restore batch and before reindex.

    Uses a lower threshold than the initial gate (15% vs 30%) since we're
    already committed and just need to avoid hitting ES watermarks.
    """
    nodes = get_warm_hot_nodes(client)
    if not nodes:
        return  # Can't check — let the operation proceed

    total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
    total_avail = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)

    needed = sum(i.store_size_bytes for i in indices)
    avail_after = total_avail - needed
    pct_free_after = avail_after / total_disk if total_disk > 0 else 0

    if pct_free_after < safety_margin:
        raise InsufficientDiskError(
            f"Disk recheck: need {needed / (1024**3):.1f} GB but only "
            f"{total_avail / (1024**3):.1f} GB free. "
            f"After: {pct_free_after:.0%} free (need {safety_margin:.0%}). "
            f"Previous operations may not have freed enough space yet."
        )

    log.info(
        "disk_recheck_passed",
        needed_gb=round(needed / (1024**3), 1),
        avail_gb=round(total_avail / (1024**3), 1),
        pct_free=round(pct_free_after * 100, 1),
    )
