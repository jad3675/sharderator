"""Cluster status and ILM monitoring for the test suite."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch


def get_cluster_status(client: Elasticsearch) -> dict:
    """Return a summary of the cluster's frozen shard state."""
    info = client.info()
    health = client.cluster.health()

    # Count frozen indices and shards
    try:
        indices = client.cat.indices(
            index="partial-*test*,test-daily-*,test-oversharded-*",
            format="json",
            h="index,status,pri,docs.count,dataset.size",
            bytes="b",
            expand_wildcards="all",
        )
    except Exception:
        indices = []

    frozen = [i for i in indices if i.get("index", "").startswith("partial-")]
    hot_or_warm = [i for i in indices if not i.get("index", "").startswith("partial-")]

    frozen_shards = sum(int(i.get("pri", 0)) for i in frozen)
    hot_shards = sum(int(i.get("pri", 0)) for i in hot_or_warm)

    # Get frozen shard limit — the settings structure varies by ES version.
    # Try cluster.max_shards_per_node.frozen first (explicit frozen limit),
    # then fall back to cluster.max_shards_per_node (general limit), then 3000.
    frozen_limit = 3000
    try:
        settings = client.cluster.get_settings(include_defaults=True)
        for scope in ("transient", "persistent", "defaults"):
            scope_data = settings.get(scope, {})
            cluster_data = scope_data.get("cluster", {})
            msn = cluster_data.get("max_shards_per_node", {})
            if isinstance(msn, dict):
                val = msn.get("frozen")
                if val is not None:
                    frozen_limit = int(val)
                    break
            elif isinstance(msn, str) and msn:
                # General limit — use it as a proxy
                frozen_limit = int(msn)
                break
    except Exception:
        pass

    # Check tracker
    tracker_count = 0
    try:
        result = client.count(index="sharderator-tracker")
        tracker_count = result.get("count", 0)
    except Exception:
        pass

    return {
        "cluster_name": info["cluster_name"],
        "health": health["status"],
        "frozen_indices": len(frozen),
        "frozen_shards": frozen_shards,
        "frozen_limit": frozen_limit,
        "frozen_pct": frozen_shards / frozen_limit * 100 if frozen_limit else 0,
        "hot_indices": len(hot_or_warm),
        "hot_shards": hot_shards,
        "tracker_jobs": tracker_count,
    }


def print_status(client: Elasticsearch, console=None) -> dict:
    """Print a formatted status summary."""
    _log = console.print if console else print
    status = get_cluster_status(client)

    _log(f"\nCluster:        {status['cluster_name']}")
    _log(f"Health:         {status['health']}")
    _log(f"Frozen shards:  {status['frozen_shards']:,} / {status['frozen_limit']:,} ({status['frozen_pct']:.1f}%)")
    _log(f"Frozen indices: {status['frozen_indices']:,}")
    _log(f"Hot/warm:       {status['hot_indices']:,} indices, {status['hot_shards']:,} shards")
    _log(f"Tracker jobs:   {status['tracker_jobs']}")

    if status["frozen_pct"] >= 100:
        msg = "⛔ OVER LIMIT — ILM will stall"
        _log(f"[red]{msg}[/red]" if console else msg)
    elif status["frozen_pct"] >= 90:
        msg = "⚠ Near limit — run Sharderator soon"
        _log(f"[yellow]{msg}[/yellow]" if console else msg)

    return status


def watch_ilm(client: Elasticsearch, interval: int = 30, console=None) -> None:
    """Poll ILM status until all test indices are frozen."""
    _log = console.print if console else print
    _log(f"\nWatching ILM progress (polling every {interval}s, Ctrl+C to stop)...\n")

    while True:
        status = get_cluster_status(client)
        ts = time.strftime("%H:%M:%S")
        _log(
            f"[{ts}] frozen={status['frozen_shards']:,}/{status['frozen_limit']:,} "
            f"hot={status['hot_shards']:,} tracker={status['tracker_jobs']}"
        )
        if status["hot_shards"] == 0 and status["frozen_shards"] > 0:
            _log("\n✓ All test indices appear to be frozen.")
            break
        time.sleep(interval)
