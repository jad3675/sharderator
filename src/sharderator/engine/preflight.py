"""Pre-flight cluster health checks with adaptive backpressure."""

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


# ---------------------------------------------------------------------------
# Circuit breaker helpers
# ---------------------------------------------------------------------------

def _get_hot_breakers(
    client: Elasticsearch,
    threshold: float = 0.80,
) -> list[tuple[str, str, float]]:
    """Scan all nodes for circuit breakers above the given threshold.

    Returns a list of (breaker_name, node_name, ratio) tuples.
    An empty list means all breakers are below threshold.
    """
    hot: list[tuple[str, str, float]] = []
    nodes_stats = client.nodes.stats(metric="breaker")
    for node_id, node_data in nodes_stats.get("nodes", {}).items():
        node_name = node_data.get("name", node_id)
        for breaker_name, breaker_data in node_data.get("breakers", {}).items():
            limit = breaker_data.get("limit_size_in_bytes", 0)
            estimated = breaker_data.get("estimated_size_in_bytes", 0)
            if limit > 0:
                ratio = estimated / limit
                if ratio > threshold:
                    hot.append((breaker_name, node_name, ratio))
    return hot


def check_circuit_breakers(
    client: Elasticsearch,
    wait_timeout_minutes: int = 10,
) -> None:
    """Wait for circuit breakers to drop below 80%, or raise after timeout.

    On first detection of pressure, logs a warning and enters a linear
    backoff loop (30s, 60s, 90s… capped at 300s). If pressure clears,
    logs recovery and returns normally. If the timeout expires, raises
    ClusterNotHealthyError with the worst breaker.

    Set wait_timeout_minutes=0 for fail-fast behavior (no waiting).
    """
    hot = _get_hot_breakers(client, threshold=0.80)
    if not hot:
        return

    worst = max(hot, key=lambda b: b[2])

    # Fail-fast mode — no waiting
    if wait_timeout_minutes <= 0:
        raise ClusterNotHealthyError(
            f"Circuit breaker '{worst[0]}' on node '{worst[1]}' "
            f"is at {worst[2]:.0%}. "
            f"Cluster is under memory pressure. Wait for load to subside."
        )

    # Pressure detected — wait with backoff
    log.warning(
        "circuit_breaker_pressure_detected",
        breaker=worst[0],
        node=worst[1],
        ratio=f"{worst[2]:.0%}",
        wait_timeout_minutes=wait_timeout_minutes,
    )

    deadline = time.time() + wait_timeout_minutes * 60
    wait_interval = 30

    while time.time() < deadline:
        time.sleep(min(wait_interval, 300))
        wait_interval += 30
        hot = _get_hot_breakers(client, threshold=0.80)
        if not hot:
            log.info("circuit_breaker_pressure_resolved")
            return
        worst = max(hot, key=lambda b: b[2])
        log.info(
            "circuit_breaker_still_hot",
            breaker=worst[0],
            node=worst[1],
            ratio=f"{worst[2]:.0%}",
            remaining_seconds=int(deadline - time.time()),
        )

    # Timed out — raise with the worst offender
    worst = max(hot, key=lambda b: b[2])
    raise ClusterNotHealthyError(
        f"Circuit breaker '{worst[0]}' on node '{worst[1]}' "
        f"is at {worst[2]:.0%} after waiting {wait_timeout_minutes} minutes. "
        f"Cluster is under sustained memory pressure."
    )


def check_circuit_breakers_instant(client: Elasticsearch) -> list[str]:
    """Non-waiting breaker check for dry-run / preflight reports.

    Returns list of issue strings (empty = all clear).
    """
    issues = []
    hot = _get_hot_breakers(client, threshold=0.80)
    for breaker_name, node_name, ratio in hot:
        issues.append(
            f"Circuit breaker '{breaker_name}' on node '{node_name}' "
            f"is at {ratio:.0%}"
        )
    return issues


# ---------------------------------------------------------------------------
# Mid-operation health check
# ---------------------------------------------------------------------------

def check_cluster_health_mid_operation(
    client: Elasticsearch, pause_timeout_minutes: int = 15
) -> None:
    """Mid-operation health check with deeper pressure indicators.

    Checks: red health, thread pool rejections, pending cluster tasks,
    JVM heap pressure, circuit breaker ratios. If any are hot, waits
    with linear backoff.
    """
    # Quick red check first
    health = client.cluster.health()
    status = health.get("status", "red")

    if status == "red":
        log.warning("cluster_red_during_operation", unassigned=health.get("unassigned_shards", 0))
        _wait_for_not_red(client, pause_timeout_minutes)
        return

    # Deeper pressure checks — warn and wait if hot
    issues = _check_cluster_pressure(client)
    if not issues:
        return

    log.warning("cluster_pressure_detected", issues=issues)
    deadline = time.time() + pause_timeout_minutes * 60
    wait_interval = 30  # linear backoff: 30s, 60s, 90s... up to 300s

    while time.time() < deadline:
        time.sleep(min(wait_interval, 300))
        wait_interval += 30
        issues = _check_cluster_pressure(client)
        if not issues:
            log.info("cluster_pressure_resolved")
            return

    # Don't fail — just log and continue. The operator chose to run.
    log.warning("cluster_pressure_persists", issues=issues)


def _wait_for_not_red(client: Elasticsearch, timeout_minutes: int) -> None:
    """Wait for cluster to leave red status."""
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        time.sleep(30)
        health = client.cluster.health()
        if health.get("status") != "red":
            log.info("cluster_recovered", status=health.get("status"))
            return
    raise ClusterNotHealthyError(
        f"Cluster has been RED for {timeout_minutes} minutes. "
        f"Stopping to avoid further damage. Job can be resumed after recovery."
    )


def _check_cluster_pressure(client: Elasticsearch) -> list[str]:
    """Check deeper cluster pressure indicators. Returns list of issues.

    Uses a single nodes.stats call for thread_pool and jvm metrics,
    plus a separate breaker check via _get_hot_breakers.
    """
    issues: list[str] = []

    try:
        # Pending cluster tasks
        pending = client.cluster.pending_tasks()
        task_count = len(pending.get("tasks", []))
        if task_count > 50:
            issues.append(f"High pending cluster tasks: {task_count}")

        # Single call for both thread pool and JVM stats
        stats = client.nodes.stats(metric="thread_pool,jvm")
        for node_id, node_data in stats.get("nodes", {}).items():
            node_name = node_data.get("name", node_id)

            # Thread pool queue depths
            pools = node_data.get("thread_pool", {})
            for pool_name in ("write", "search", "snapshot"):
                pool = pools.get(pool_name, {})
                queue = pool.get("queue", 0)
                if queue > 100:
                    issues.append(f"Thread pool '{pool_name}' queue={queue} on {node_name}")

            # JVM heap pressure
            heap = node_data.get("jvm", {}).get("mem", {})
            heap_used = heap.get("heap_used_in_bytes", 0)
            heap_max = heap.get("heap_max_in_bytes", 0)
            if heap_max > 0 and heap_used / heap_max > 0.85:
                issues.append(f"JVM heap at {heap_used / heap_max:.0%} on {node_name}")

        # Circuit breaker pressure — threshold at 75%, intentionally below
        # the hard gate at 80% so mid-operation pauses kick in before the
        # preflight gate would reject the next index.
        hot_breakers = _get_hot_breakers(client, threshold=0.75)
        for breaker_name, node_name, ratio in hot_breakers:
            issues.append(
                f"Circuit breaker '{breaker_name}' at {ratio:.0%} on {node_name}"
            )

        # Relocating/initializing shards
        health = client.cluster.health()
        relocating = health.get("relocating_shards", 0)
        initializing = health.get("initializing_shards", 0)
        if relocating > 10:
            issues.append(f"Relocating shards: {relocating}")
        if initializing > 10:
            issues.append(f"Initializing shards: {initializing}")

    except Exception as e:
        log.warning("pressure_check_failed", error=str(e))

    return issues


# ---------------------------------------------------------------------------
# Inter-index backpressure for batch loops
# ---------------------------------------------------------------------------

def wait_for_cluster_ready(
    client: Elasticsearch,
    allow_yellow: bool = True,
    ignore_circuit_breakers: bool = False,
    wait_timeout_minutes: int = 15,
) -> None:
    """Block until the cluster is ready for the next batch item.

    Called between indices/groups in batch loops. Combines:
    - Cluster health (red = wait, yellow = configurable)
    - Circuit breaker pressure (wait with backoff if above 75%)
    - General pressure indicators (thread pools, JVM heap, pending tasks)

    If all checks pass immediately, returns with no delay.
    If pressure is detected, waits with linear backoff up to the timeout.
    Raises ClusterNotHealthyError only if the cluster stays red beyond timeout.
    For non-red pressure, logs a warning and proceeds after timeout
    (the per-index preflight gate is the hard stop).
    """
    if wait_timeout_minutes <= 0:
        return  # Backpressure disabled

    # Quick check — if everything is fine, return immediately (no delay)
    health = client.cluster.health()
    status = health.get("status", "red")

    if status == "red":
        log.warning("batch_wait_cluster_red")
        _wait_for_not_red(client, wait_timeout_minutes)

    # Check for general pressure including circuit breakers
    issues = _check_cluster_pressure(client)
    if not issues:
        return  # All clear — no inter-index delay

    log.info("batch_wait_pressure_detected", issues=issues)
    deadline = time.time() + wait_timeout_minutes * 60
    wait_interval = 30

    while time.time() < deadline:
        time.sleep(min(wait_interval, 300))
        wait_interval += 30
        issues = _check_cluster_pressure(client)
        if not issues:
            log.info("batch_wait_pressure_resolved")
            return
        log.info(
            "batch_wait_pressure_persists",
            issues=issues,
            remaining_seconds=int(deadline - time.time()),
        )

    # Don't hard-fail — the per-index preflight is the real gate.
    # Just log and let the next index try (it may fail on its own preflight).
    log.warning(
        "batch_wait_timeout_proceeding",
        issues=issues,
        timeout_minutes=wait_timeout_minutes,
    )


# ---------------------------------------------------------------------------
# Dry-run preflight
# ---------------------------------------------------------------------------

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

    # 2. Circuit breakers (instant check, no waiting)
    breaker_issues = check_circuit_breakers_instant(client)
    if breaker_issues:
        prefix = "WARNING (overridden)" if ignore_circuit_breakers else "BLOCKED"
        for issue in breaker_issues:
            issues.append(f"{prefix} — {issue}")

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
