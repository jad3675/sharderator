"""SNAPSHOTTING state — create a new snapshot of the consolidated index."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def snapshot(
    client: Elasticsearch,
    shrunk_name: str,
    repo: str,
    job: JobRecord,
    original_name: str,
    progress_callback=None,
) -> str:
    """Force-merge then snapshot the shrunk index. Returns snapshot name."""
    # Force merge with duplicate detection and timeout recovery
    job.transition(JobState.FORCE_MERGING)
    _force_merge_safe(client, shrunk_name)

    # Create snapshot
    job.transition(JobState.SNAPSHOTTING)
    ts = int(time.time())
    snap_name = f"sharderator-{original_name}-{ts}"
    job.new_snapshot_name = snap_name

    log.info("snapshotting", snapshot=snap_name, repo=repo)
    client.snapshot.create(
        repository=repo,
        snapshot=snap_name,
        body={"indices": shrunk_name, "include_global_state": False},
        wait_for_completion=False,
    )

    # Wait for snapshot completion
    job.transition(JobState.AWAITING_SNAPSHOT)
    _wait_for_snapshot(client, repo, snap_name, progress_callback)
    return snap_name


def _force_merge_safe(
    client: Elasticsearch, index: str, timeout_minutes: int = 120
) -> None:
    """Force merge with duplicate detection and timeout recovery.

    If a force merge is already running (from a previous timed-out attempt),
    wait for it instead of starting a second one that doubles I/O load.
    """
    # Check if a force merge is already running on this index
    try:
        tasks = client.tasks.list(actions="*forcemerge*", detailed=True)
        for node_tasks in tasks.get("nodes", {}).values():
            for task_info in node_tasks.get("tasks", {}).values():
                if index in task_info.get("description", ""):
                    log.warning("force_merge_already_running", index=index)
                    _wait_for_forcemerge_completion(client, index, timeout_minutes)
                    return
    except Exception:
        pass  # If task listing fails, proceed with the merge

    log.info("force_merging", index=index)
    try:
        client.indices.forcemerge(
            index=index, max_num_segments=1, request_timeout=3600
        )
    except Exception as e:
        if "timeout" in str(e).lower() or "timed out" in str(e).lower():
            log.warning("force_merge_http_timeout", index=index)
            _wait_for_forcemerge_completion(client, index, timeout_minutes)
        else:
            raise

    segments = client.cat.segments(index=index, format="json")
    log.info(
        "segments_after_merge", index=index, count=len(segments) if segments else 0
    )


def _wait_for_forcemerge_completion(
    client: Elasticsearch, index: str, timeout_minutes: int = 120
) -> None:
    """Poll until no force merge tasks are running for this index."""
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        try:
            tasks = client.tasks.list(actions="*forcemerge*", detailed=True)
            still_running = any(
                index in task_info.get("description", "")
                for node_tasks in tasks.get("nodes", {}).values()
                for task_info in node_tasks.get("tasks", {}).values()
            )
            if not still_running:
                return
        except Exception:
            pass
        time.sleep(15)
    raise RuntimeError(
        f"Force merge for {index} still running after {timeout_minutes}m"
    )


def _wait_for_snapshot(
    client: Elasticsearch,
    repo: str,
    snap_name: str,
    progress_callback=None,
    timeout_minutes: int = 60,
) -> None:
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        try:
            status = client.snapshot.status(repository=repo, snapshot=snap_name)
            snaps = status.get("snapshots", [])
            if snaps:
                state = snaps[0].get("state", "")
                if state == "SUCCESS":
                    log.info("snapshot_complete", snapshot=snap_name)
                    return
                if state == "FAILED":
                    raise RuntimeError(f"Snapshot {snap_name} failed")
                if progress_callback:
                    stats = snaps[0].get("stats", {})
                    total = stats.get("total", {}).get("size_in_bytes", 0)
                    done = stats.get("processed", {}).get("size_in_bytes", 0)
                    if total > 0:
                        progress_callback(done / total * 100)
        except Exception as e:
            if "FAILED" in str(e):
                raise
        time.sleep(10)

    raise RuntimeError(f"Snapshot timeout after {timeout_minutes}m for {snap_name}")
