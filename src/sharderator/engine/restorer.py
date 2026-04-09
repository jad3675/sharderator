"""RESTORING state — restore frozen index from snapshot to writable tier."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.client.queries import get_warm_hot_nodes
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)

RESTORE_PREFIX = "sharderator-restore-"


class InsufficientDiskError(Exception):
    """Raised when the working tier cannot safely accommodate a restore."""
    pass


def restore(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
    working_tier: str,
    safety_margin: float = 0.30,
) -> str:
    """Restore the index from its snapshot to a writable tier. Returns restored index name."""
    job.transition(JobState.RESTORING)

    restore_name = f"{RESTORE_PREFIX}{info.original_index_name}"
    job.restore_index = restore_name

    # HARD GATE: refuse if working tier can't hold the restore safely
    _check_disk_space(client, info, safety_margin)

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

    log.info("restoring", index=info.name, restore_as=restore_name)
    client.snapshot.restore(
        repository=info.repository_name,
        snapshot=info.snapshot_name,
        body=body,
        wait_for_completion=False,
    )
    return restore_name


def wait_for_recovery(
    client: Elasticsearch,
    restore_name: str,
    job: JobRecord,
    timeout_minutes: int = 30,
    progress_callback=None,
) -> None:
    """Poll until the restored index is green."""
    job.transition(JobState.AWAITING_RECOVERY)
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        try:
            health = client.cluster.health(
                index=restore_name, wait_for_status="green", timeout="30s"
            )
            if health.get("status") == "green":
                log.info("recovery_complete", index=restore_name)
                return
        except Exception:
            pass

        if progress_callback:
            try:
                recovery = client.cat.recovery(index=restore_name, format="json")
                if recovery:
                    pct = _avg_recovery_pct(recovery)
                    progress_callback(pct)
            except Exception:
                pass

        time.sleep(10)

    raise RuntimeError(f"Recovery timeout after {timeout_minutes}m for {restore_name}")


def _check_disk_space(
    client: Elasticsearch,
    info: IndexInfo,
    safety_margin: float = 0.30,
) -> None:
    """HARD GATE: Refuse to restore if working tier doesn't have enough space.

    Stays well below the ES high watermark (85%) and flood-stage watermark (95%).
    """
    nodes = get_warm_hot_nodes(client)
    if not nodes:
        raise InsufficientDiskError("No warm/hot/content nodes found in cluster")

    total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
    total_avail = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)

    if total_disk == 0:
        raise InsufficientDiskError(
            "Cannot determine disk capacity of working tier nodes"
        )

    needed = info.store_size_bytes
    avail_after = total_avail - needed
    pct_free_after = avail_after / total_disk if total_disk > 0 else 0

    if pct_free_after < safety_margin:
        raise InsufficientDiskError(
            f"Restore of {info.name} needs {needed / (1024**3):.1f} GB but working tier "
            f"only has {total_avail / (1024**3):.1f} GB free across {len(nodes)} nodes. "
            f"After restore, free space would be {pct_free_after:.0%} "
            f"(safety margin requires {safety_margin:.0%} free). "
            f"Reduce batch size or add warm/hot capacity."
        )

    log.info(
        "disk_space_check_passed",
        index=info.name,
        needed_gb=round(needed / (1024**3), 1),
        avail_gb=round(total_avail / (1024**3), 1),
        pct_free_after=round(pct_free_after * 100, 1),
    )


def _avg_recovery_pct(recovery_data: list[dict]) -> float:
    """Calculate average recovery percentage across shards."""
    pcts = []
    for shard in recovery_data:
        bytes_total = int(shard.get("bytes_total", 0) or 0)
        bytes_recovered = int(shard.get("bytes_recovered", 0) or 0)
        if bytes_total > 0:
            pcts.append(bytes_recovered / bytes_total * 100)
    return sum(pcts) / len(pcts) if pcts else 0.0
