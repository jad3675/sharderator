"""REMOUNTING state — mount the new snapshot as a frozen searchable snapshot.

Safety: This module ONLY mounts and strips ILM. Data stream backing index
swaps are handled in swapper.py AFTER verification passes.
"""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def remount(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
    snap_name: str,
    repo: str,
) -> str:
    """Mount the new snapshot as a partially mounted (frozen) index. Returns new index name."""
    job.transition(JobState.REMOUNTING)

    temp_mount_name = f"partial-sharderator-{info.original_index_name}"

    # Clean up stale mount from a previous failed run
    if client.indices.exists(index=temp_mount_name):
        log.warning("mount_exists_deleting", index=temp_mount_name)
        client.indices.delete(index=temp_mount_name)

    log.info("remounting", snapshot=snap_name, as_index=temp_mount_name)

    client.searchable_snapshots.mount(
        repository=repo,
        snapshot=snap_name,
        body={
            "index": job.shrunk_index,
            "renamed_index": temp_mount_name,
            "index_settings": {
                "index.number_of_replicas": 0,
                "index.routing.allocation.include._tier_preference": "data_frozen",
            },
        },
        storage="shared_cache",
    )

    # Strip ILM to prevent re-processing
    try:
        client.indices.put_settings(
            index=temp_mount_name,
            settings={"index.lifecycle.indexing_complete": True},
        )
    except Exception as e:
        log.warning("ilm_strip_failed", index=temp_mount_name, error=str(e))

    job.new_frozen_index = temp_mount_name
    return temp_mount_name


def remount_merged(
    client: Elasticsearch,
    job: JobRecord,
    snap_name: str,
    repo: str,
    merged_index_name: str,
    final_name: str,
) -> str:
    """Mount a merged snapshot as a frozen searchable snapshot. Returns new index name.

    Data stream swaps are NOT done here — they happen in swapper.py after verification.
    """
    job.transition(JobState.REMOUNTING)

    log.info("remounting_merged", snapshot=snap_name, as_index=final_name)

    # Clean up stale mount from a previous failed run
    if client.indices.exists(index=final_name):
        log.warning("mount_exists_deleting", index=final_name)
        client.indices.delete(index=final_name)

    client.searchable_snapshots.mount(
        repository=repo,
        snapshot=snap_name,
        body={
            "index": merged_index_name,
            "renamed_index": final_name,
            "index_settings": {
                "index.number_of_replicas": 0,
                "index.routing.allocation.include._tier_preference": "data_frozen",
            },
        },
        storage="shared_cache",
    )

    # Strip ILM
    try:
        client.indices.put_settings(
            index=final_name,
            settings={"index.lifecycle.indexing_complete": True},
        )
    except Exception as e:
        log.warning("ilm_strip_failed", index=final_name, error=str(e))

    job.new_frozen_index = final_name
    return final_name
