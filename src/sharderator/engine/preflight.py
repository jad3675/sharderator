"""Pre-flight cluster health checks."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.util.logging import get_logger

log = get_logger(__name__)


class ClusterNotHealthyError(Exception):
    """Raised when the cluster is not in a safe state for operations."""
    pass


def check_cluster_health(
    client: Elasticsearch, allow_yellow: bool = True
) -> None:
    """Refuse to start on a sick cluster."""
    health = client.cluster.health()
    status = health.get("status", "red")
    relocating = health.get("relocating_shards", 0)
    initializing = health.get("initializing_shards", 0)
    unassigned = health.get("unassigned_shards", 0)

    if status == "red":
        raise ClusterNotHealthyError(
            f"Cluster health is RED ({unassigned} unassigned shards). "
            f"Resolve cluster health issues before running Sharderator."
        )

    if status == "yellow" and not allow_yellow:
        raise ClusterNotHealthyError(
            f"Cluster health is YELLOW ({unassigned} unassigned shards). "
            f"Enable 'Allow yellow cluster' in Settings to proceed."
        )

    if relocating > 10:
        raise ClusterNotHealthyError(
            f"Cluster has {relocating} relocating shards. "
            f"Wait for relocation to settle before running Sharderator."
        )

    if initializing > 10:
        raise ClusterNotHealthyError(
            f"Cluster has {initializing} initializing shards. "
            f"Wait for initialization to complete."
        )

    log.info("cluster_health_ok", status=status, relocating=relocating)


def check_circuit_breakers(client: Elasticsearch) -> None:
    """Refuse to start if any circuit breaker is above 80% capacity."""
    nodes_stats = client.nodes.stats(metric="breaker")
    for node_id, node_data in nodes_stats.get("nodes", {}).items():
        breakers = node_data.get("breakers", {})
        for breaker_name, breaker_data in breakers.items():
            limit = breaker_data.get("limit_size_in_bytes", 0)
            estimated = breaker_data.get("estimated_size_in_bytes", 0)
            if limit > 0 and estimated / limit > 0.80:
                node_name = node_data.get("name", node_id)
                raise ClusterNotHealthyError(
                    f"Circuit breaker '{breaker_name}' on node '{node_name}' "
                    f"is at {estimated / limit:.0%}. "
                    f"Cluster is under memory pressure. Wait for load to subside."
                )


def check_cluster_health_mid_operation(
    client: Elasticsearch, pause_timeout_minutes: int = 15
) -> None:
    """Mid-operation health check. If cluster goes red, pause and wait.
    Yellow is fine — most Elastic Cloud clusters run yellow normally.
    """
    health = client.cluster.health()
    status = health.get("status", "red")
    if status != "red":
        return

    log.warning(
        "cluster_red_during_operation",
        unassigned=health.get("unassigned_shards", 0),
    )

    deadline = time.time() + pause_timeout_minutes * 60
    while time.time() < deadline:
        time.sleep(30)
        health = client.cluster.health()
        if health.get("status") != "red":
            log.info("cluster_recovered", status=health.get("status"))
            return

    raise ClusterNotHealthyError(
        f"Cluster has been RED for {pause_timeout_minutes} minutes during operation. "
        f"Stopping to avoid further damage. Job can be resumed after recovery."
    )


def run_dry_run_preflight(
    client: Elasticsearch,
    allow_yellow: bool = True,
    disk_safety_margin: float = 0.30,
    needed_bytes: int = 0,
    ignore_circuit_breakers: bool = False,
) -> list[str]:
    """Run all safety checks without mutating anything. Returns list of issues found.

    An empty list means all checks passed. Each string is a human-readable
    description of a check that failed or warned.
    """
    from sharderator.client.queries import get_warm_hot_nodes

    issues: list[str] = []

    # 1. Cluster health
    try:
        check_cluster_health(client, allow_yellow=allow_yellow)
    except ClusterNotHealthyError as e:
        issues.append(f"BLOCKED — {e}")

    # 2. Circuit breakers
    if ignore_circuit_breakers:
        try:
            check_circuit_breakers(client)
        except ClusterNotHealthyError as e:
            issues.append(f"WARNING (overridden) — {e}")
    else:
        try:
            check_circuit_breakers(client)
        except ClusterNotHealthyError as e:
            issues.append(f"BLOCKED — {e}")

    # 3. Disk space (if we know how much we need)
    if needed_bytes > 0:
        try:
            nodes = get_warm_hot_nodes(client)
            if not nodes:
                issues.append(
                    "BLOCKED — No warm/hot/content nodes found in cluster"
                )
            else:
                total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
                total_avail = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)

                if total_disk > 0:
                    avail_after = total_avail - needed_bytes
                    pct_free_after = avail_after / total_disk

                    if pct_free_after < disk_safety_margin:
                        issues.append(
                            f"BLOCKED — Working tier needs {needed_bytes / (1024**3):.1f} GB "
                            f"but only has {total_avail / (1024**3):.1f} GB free "
                            f"across {len(nodes)} nodes. "
                            f"After operation: {pct_free_after:.0%} free "
                            f"(safety margin: {disk_safety_margin:.0%})"
                        )
                    else:
                        log.info(
                            "dry_run_disk_ok",
                            needed_gb=round(needed_bytes / (1024**3), 1),
                            avail_gb=round(total_avail / (1024**3), 1),
                            pct_free_after=round(pct_free_after * 100, 1),
                        )
        except Exception as e:
            issues.append(f"WARNING — Disk space check failed: {e}")

    return issues
