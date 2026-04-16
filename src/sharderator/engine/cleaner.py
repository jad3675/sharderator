"""CLEANING_UP state — remove intermediate artifacts."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState, JobType
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def cleanup(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
    delete_old_snapshot: bool = False,
) -> None:
    """Remove old frozen mount, intermediate indices, and optionally old snapshot."""
    if job.job_type != JobType.SHRINK:
        raise ValueError(f"cleanup() called on {job.job_type.value} job")
    job.transition(JobState.CLEANING_UP)

    # 1. Delete old frozen mount
    _safe_delete_index(client, info.name)

    # 2. Delete intermediate restored index
    if job.restore_index:
        _safe_delete_index(client, job.restore_index)

    # 3. Delete intermediate shrunk index
    if job.shrunk_index:
        _safe_delete_index(client, job.shrunk_index)

    # 4. Optionally delete old snapshot
    if delete_old_snapshot and job.original_snapshot_name and job.repository_name:
        _safe_delete_snapshot(client, job.repository_name, job.original_snapshot_name)

    log.info("cleanup_complete", index=info.name)


def cleanup_merge(
    client: Elasticsearch,
    job: JobRecord,
    source_indices: list[IndexInfo],
    delete_old_snapshot: bool = False,
) -> None:
    """Clean up after a merge operation — delete all source frozen mounts + intermediates."""
    if job.job_type != JobType.MERGE:
        raise ValueError(f"cleanup_merge() called on {job.job_type.value} job")
    job.transition(JobState.CLEANING_UP)

    # Delete all source frozen mounts
    for idx in source_indices:
        _safe_delete_index(client, idx.name)

    # Delete all intermediate restored indices
    for restore_name in job.restore_indices:
        _safe_delete_index(client, restore_name)

    # Delete the merged intermediate index
    if job.shrunk_index:
        _safe_delete_index(client, job.shrunk_index)

    log.info("merge_cleanup_complete", sources=len(source_indices))


def _safe_delete_index(client: Elasticsearch, index: str) -> None:
    """Delete an index, ignoring 404."""
    try:
        client.indices.delete(index=index, ignore=[404])
        log.info("deleted_index", index=index)
    except Exception as e:
        log.warning("delete_index_failed", index=index, error=str(e))


def _safe_delete_snapshot(
    client: Elasticsearch, repo: str, snapshot: str
) -> None:
    """Delete a snapshot only if no other mounted indices reference it."""
    try:
        snap_info = client.snapshot.get(repository=repo, snapshot=snapshot)
        snaps = snap_info.get("snapshots", [])
        if not snaps:
            return

        snap_indices = snaps[0].get("indices", [])

        for idx_name in snap_indices:
            # Check standard mount prefixes
            for prefix in ("partial-", ".partial-", "restored-", ".restored-"):
                mounted_name = f"{prefix}{idx_name}"
                if client.indices.exists(index=mounted_name):
                    log.warning(
                        "snapshot_still_referenced",
                        snapshot=snapshot,
                        by_index=mounted_name,
                    )
                    return

            # Check for Sharderator-suffixed mounts.
            # A snapshot containing "sharderator-shrunk-.ds-foo-2026.04.15-000001"
            # may have been mounted as "partial-.ds-foo-2026.04.15-000001-sharderated".
            for prefix in ("partial-", ".partial-"):
                # Strip the sharderator working prefix to reconstruct the original name
                original = idx_name
                for work_prefix in ("sharderator-shrunk-", "sharderator-restore-", "sharderator-merged-"):
                    if original.startswith(work_prefix):
                        original = original[len(work_prefix):]
                        break
                sharderated_name = f"{prefix}{original}-sharderated"
                if client.indices.exists(index=sharderated_name):
                    log.warning(
                        "snapshot_still_referenced",
                        snapshot=snapshot,
                        by_index=sharderated_name,
                    )
                    return

            # Also check bare index name for manual mounts
            try:
                if client.indices.exists(index=idx_name):
                    settings = client.indices.get_settings(index=idx_name)
                    snap_meta = (
                        settings.get(idx_name, {})
                        .get("settings", {})
                        .get("index", {})
                        .get("store", {})
                        .get("snapshot", {})
                    )
                    if snap_meta.get("snapshot_name") == snapshot:
                        log.warning(
                            "snapshot_still_referenced",
                            snapshot=snapshot,
                            by_index=idx_name,
                        )
                        return
            except Exception:
                pass

        client.snapshot.delete(repository=repo, snapshot=snapshot)
        log.info("deleted_snapshot", snapshot=snapshot, repo=repo)
    except Exception as e:
        log.warning("delete_snapshot_failed", snapshot=snapshot, error=str(e))
