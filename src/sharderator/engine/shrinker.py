"""SHRINKING state — shrink or reindex to target shard count."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.client.queries import get_warm_hot_nodes
from sharderator.engine.task_waiter import wait_for_task, TransientReindexError
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)

SHRUNK_PREFIX = "sharderator-shrunk-"
MAX_REINDEX_RETRIES = 3


def shrink(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
    restore_name: str,
    requests_per_second: float = -1,
) -> str:
    """Shrink (or reindex) the restored index. Returns the shrunk index name."""
    job.transition(JobState.SHRINKING)

    shrunk_name = f"{SHRUNK_PREFIX}{info.original_index_name}"
    job.shrunk_index = shrunk_name

    if info.needs_reindex:
        log.info("using_reindex_fallback", index=restore_name, target=shrunk_name)
        task_id = _reindex_async(client, info, restore_name, shrunk_name, requests_per_second)
        job.merge_task_id = task_id  # reuse field for the reindex task
    else:
        log.info("using_shrink", index=restore_name, target=shrunk_name)
        _shrink(client, info, restore_name, shrunk_name)

    return shrunk_name


def wait_for_shrink(
    client: Elasticsearch,
    shrunk_name: str,
    job: JobRecord,
    timeout_minutes: int = 30,
) -> None:
    """Wait for the shrunk index to be green (after shrink API)."""
    job.transition(JobState.AWAITING_SHRINK)
    _wait_for_green(client, shrunk_name, timeout_minutes)
    log.info("shrink_complete", index=shrunk_name)


def wait_for_reindex(
    client: Elasticsearch,
    job: JobRecord,
    timeout_minutes: int = 120,
    progress_callback=None,
    max_retries: int = MAX_REINDEX_RETRIES,
) -> None:
    """Wait for async reindex task to complete, with retry on transient failures.

    On transient failures (scroll expiry, unavailable shards, node disconnects),
    waits for the target index to recover and retries with conflicts:proceed to
    fill in the gaps. Permanent failures (mapping errors, etc.) propagate immediately.
    """
    job.transition(JobState.AWAITING_REINDEX)

    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            wait_for_task(client, job.merge_task_id, progress_callback, timeout_minutes)
            log.info("reindex_complete", index=job.shrunk_index, attempts=attempt)
            return
        except TransientReindexError as e:
            last_error = e
            log.warning(
                "shrink_reindex_transient_failure",
                index=job.shrunk_index,
                attempt=attempt,
                max_retries=max_retries,
                failures=e.failure_count,
                created=e.created,
                total=e.total,
            )
            if attempt >= max_retries:
                raise RuntimeError(
                    f"Shrink reindex failed after {max_retries} attempts due to "
                    f"transient errors. Last: {e}"
                )

            # Wait for target index to stabilize before retrying
            log.info("shrink_reindex_retry_backoff", seconds=30 * attempt)
            time.sleep(30 * attempt)
            _wait_for_green(client, job.shrunk_index, timeout_minutes=10)

            # Retry with conflicts:proceed — already-indexed docs are skipped
            # via 409 version conflict rather than failing the task.
            log.info(
                "shrink_reindex_retry",
                index=job.shrunk_index,
                attempt=attempt + 1,
            )
            rps = None  # Use whatever the original was — we don't have it here,
            # but the retry is filling gaps so unlimited is fine
            resp = client.reindex(
                body={
                    "source": {"index": job.restore_index},
                    "dest": {
                        "index": job.shrunk_index,
                        "op_type": "create",
                    },
                    "conflicts": "proceed",
                },
                wait_for_completion=False,
                request_timeout=30,
                scroll="30m",
            )
            job.merge_task_id = resp.get("task", "")

    # Should not be reachable
    raise RuntimeError(f"Shrink reindex failed after {max_retries} attempts. Last: {last_error}")


def _shrink(
    client: Elasticsearch,
    info: IndexInfo,
    source: str,
    target: str,
) -> None:
    """Use the shrink API to consolidate shards."""
    nodes = get_warm_hot_nodes(client)
    if not nodes:
        raise RuntimeError("No warm/hot nodes available for shrink allocation")

    # Clean up stale shrunk index from a previous failed run
    if client.indices.exists(index=target):
        log.warning("shrunk_exists_deleting", index=target)
        client.indices.delete(index=target)

    best_node = max(nodes, key=lambda n: int(n.get("disk.avail", 0) or 0))
    node_name = best_node["name"]

    client.indices.put_settings(
        index=source,
        settings={
            "index.routing.allocation.require._name": node_name,
            "index.blocks.write": True,
        },
    )

    _wait_for_colocation(client, source, node_name)

    client.indices.shrink(
        index=source,
        target=target,
        settings={
            "index.number_of_shards": info.target_shard_count,
            "index.number_of_replicas": 0,
            "index.routing.allocation.require._name": None,
            "index.blocks.write": None,
            "index.codec": "best_compression",
        },
    )


def _reindex_async(
    client: Elasticsearch,
    info: IndexInfo,
    source: str,
    target: str,
    requests_per_second: float = -1,
) -> str:
    """Async reindex fallback. Returns task ID for polling."""
    # Clean up stale shrunk index from a previous failed run
    if client.indices.exists(index=target):
        log.warning("shrunk_exists_deleting", index=target)
        client.indices.delete(index=target)

    client.indices.create(
        index=target,
        settings={
            "index.number_of_shards": info.target_shard_count,
            "index.number_of_replicas": 0,
            "index.codec": "best_compression",
        },
        mappings=info.mappings,
    )

    rps = requests_per_second if requests_per_second > 0 else None
    resp = client.reindex(
        body={
            "source": {"index": source},
            "dest": {"index": target, "op_type": "create"},
        },
        wait_for_completion=False,
        requests_per_second=rps,
        request_timeout=30,
        scroll="30m",
    )
    return resp.get("task", "")


def _wait_for_colocation(
    client: Elasticsearch,
    index: str,
    target_node: str,
    timeout_minutes: int = 30,
) -> None:
    """Wait until all primary shards are on the target node."""
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        shards = client.cat.shards(index=index, format="json", h="node,prirep")
        primaries = [s for s in shards if s.get("prirep") == "p"]
        if primaries and all(s.get("node") == target_node for s in primaries):
            log.info("colocation_complete", index=index, node=target_node)
            return
        time.sleep(5)
    raise RuntimeError(
        f"Timed out waiting for all shards of {index} to relocate to {target_node}"
    )


def _wait_for_green(
    client: Elasticsearch, index: str, timeout_minutes: int = 30
) -> None:
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        try:
            health = client.cluster.health(
                index=index, wait_for_status="green", timeout="30s"
            )
            if health.get("status") == "green":
                return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"Timed out waiting for {index} to be green")
