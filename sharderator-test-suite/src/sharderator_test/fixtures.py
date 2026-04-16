"""ILM policies, index templates, and cluster settings for the test suite."""

from __future__ import annotations

from elasticsearch import Elasticsearch

ILM_POLICY_OVERSHARDED = "sharderator-test-oversharded"
ILM_POLICY_DAILY = "sharderator-test-daily-direct"

OVERSHARDED_POLICY = {
    "policy": {
        "phases": {
            "hot": {
                "min_age": "0ms",
                "actions": {
                    "rollover": {"max_age": "1m"}
                },
            },
            "frozen": {
                "min_age": "1m",
                "actions": {
                    "searchable_snapshot": {
                        "snapshot_repository": "found-snapshots"
                    }
                },
            },
        }
    }
}

DAILY_POLICY = {
    "policy": {
        "phases": {
            "hot": {"min_age": "0ms", "actions": {}},
            "frozen": {
                "min_age": "1m",
                "actions": {
                    "searchable_snapshot": {
                        "snapshot_repository": "found-snapshots"
                    }
                },
            },
        }
    }
}

COMMON_MAPPINGS = {
    "properties": {
        "@timestamp": {"type": "date"},
        "host": {
            "properties": {
                "name": {"type": "keyword"}
            }
        },
        "cpu": {
            "properties": {
                "pct": {"type": "float"}
            }
        },
        "memory": {
            "properties": {
                "pct": {"type": "float"}
            }
        },
        "message": {"type": "text"},
    }
}

DAILY_MAPPINGS = {
    "properties": {
        "@timestamp": {"type": "date"},
        "host": {
            "properties": {
                "name": {"type": "keyword"}
            }
        },
        "value": {"type": "float"},
        "metric": {
            "properties": {
                "type": {"type": "keyword"}
            }
        },
        "tags": {"type": "keyword"},
    }
}

OVERSHARDED_STREAMS = [
    ("test-oversharded-10-metrics", 10),
    ("test-oversharded-10-logs", 10),
    ("test-oversharded-20-metrics", 20),
    ("test-oversharded-20-logs", 20),
    ("test-oversharded-50-metrics", 50),
]

METRIC_TYPES = [
    "cpu", "memory", "disk", "network-in", "network-out",
    "load-1m", "load-5m", "load-15m", "swap", "filesystem",
    "process-cpu", "process-memory", "process-fd", "process-threads",
    "socket-summary", "entropy", "uptime", "users",
    "docker-cpu", "docker-memory", "docker-network", "docker-diskio",
    "k8s-pod-cpu", "k8s-pod-memory", "k8s-pod-network",
    "k8s-node-cpu", "k8s-node-memory", "k8s-node-fs",
    "nginx-access", "nginx-error", "apache-access", "apache-error",
    "mysql-status", "mysql-galera", "redis-info", "redis-keyspace",
    "postgresql-stats", "mongodb-status", "rabbitmq-queue", "kafka-partition",
]


def setup_cluster(client: Elasticsearch, console=None) -> None:
    """Create ILM policies, index templates, and speed up ILM poll interval."""
    _log = console.print if console else print

    # Speed up ILM for testing
    client.cluster.put_settings(
        body={"transient": {"indices.lifecycle.poll_interval": "30s"}}
    )
    _log("[green]✓[/green] ILM poll interval set to 30s" if console else "✓ ILM poll interval set to 30s")

    # Create ILM policies
    client.ilm.put_lifecycle(name=ILM_POLICY_OVERSHARDED, body=OVERSHARDED_POLICY)
    _log(f"[green]✓[/green] Created ILM policy: {ILM_POLICY_OVERSHARDED}" if console else f"✓ Created ILM policy: {ILM_POLICY_OVERSHARDED}")

    client.ilm.put_lifecycle(name=ILM_POLICY_DAILY, body=DAILY_POLICY)
    _log(f"[green]✓[/green] Created ILM policy: {ILM_POLICY_DAILY}" if console else f"✓ Created ILM policy: {ILM_POLICY_DAILY}")

    # Create index templates for over-sharded streams
    for stream_name, shard_count in OVERSHARDED_STREAMS:
        # Template name must be unique per stream — use the full stream name
        template_name = f"sharderator-test-{stream_name}"
        # Pattern matches the stream name exactly (data streams don't have a trailing suffix)
        pattern = stream_name
        client.indices.put_index_template(
            name=template_name,
            body={
                "index_patterns": [pattern],
                "data_stream": {},
                "priority": 500,  # High priority so it beats built-in templates
                "template": {
                    "settings": {
                        "number_of_shards": shard_count,
                        "number_of_replicas": 0,
                        "index.lifecycle.name": ILM_POLICY_OVERSHARDED,
                    },
                    "mappings": COMMON_MAPPINGS,
                },
            },
        )
        _log(f"[green]✓[/green] Template: {template_name} ({shard_count} shards, pattern={pattern})" if console else f"✓ Template: {template_name} ({shard_count} shards, pattern={pattern})")

    # Explicitly create the data streams so the template is applied immediately.
    # If we just write docs, ES may auto-create the stream before our template
    # is matched, resulting in default settings (1 shard).
    for stream_name, shard_count in OVERSHARDED_STREAMS:
        try:
            client.indices.create_data_stream(name=stream_name)
            _log(f"[green]✓[/green] Created data stream: {stream_name}" if console else f"✓ Created data stream: {stream_name}")
        except Exception as e:
            if "already exists" in str(e).lower() or "resource_already_exists" in str(e).lower():
                _log(f"[yellow]⚠[/yellow] Data stream already exists: {stream_name}" if console else f"⚠ Data stream already exists: {stream_name}")
            else:
                raise

    _log("\n[bold]Setup complete.[/bold] Ready to ingest data." if console else "\nSetup complete. Ready to ingest data.")


def freeze_ilm(client: Elasticsearch, console=None) -> None:
    """Update ILM policies to prevent further rollovers.

    Changes the oversharded policy rollover from max_age: 1m to max_age: 365d
    and the daily policy frozen min_age from 1m to 365d. This locks the frozen
    tier in place so Sharderator can operate on a stable set of indices.
    """
    _log = console.print if console else print

    # Update oversharded policy — stop rollovers
    frozen_oversharded = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {"max_age": "365d"}
                    },
                },
                "frozen": {
                    "min_age": "1m",
                    "actions": {
                        "searchable_snapshot": {
                            "snapshot_repository": "found-snapshots"
                        }
                    },
                },
            }
        }
    }
    client.ilm.put_lifecycle(name=ILM_POLICY_OVERSHARDED, body=frozen_oversharded)
    _log(f"[green]✓[/green] {ILM_POLICY_OVERSHARDED}: rollover changed to max_age=365d" if console
         else f"✓ {ILM_POLICY_OVERSHARDED}: rollover changed to max_age=365d")

    # Update daily policy — stop new indices from freezing
    frozen_daily = {
        "policy": {
            "phases": {
                "hot": {"min_age": "0ms", "actions": {}},
                "frozen": {
                    "min_age": "365d",
                    "actions": {
                        "searchable_snapshot": {
                            "snapshot_repository": "found-snapshots"
                        }
                    },
                },
            }
        }
    }
    client.ilm.put_lifecycle(name=ILM_POLICY_DAILY, body=frozen_daily)
    _log(f"[green]✓[/green] {ILM_POLICY_DAILY}: frozen min_age changed to 365d" if console
         else f"✓ {ILM_POLICY_DAILY}: frozen min_age changed to 365d")

    _log("\n[bold]ILM locked down.[/bold] No further rollovers or freezing will occur." if console
         else "\nILM locked down. No further rollovers or freezing will occur.")
    _log("The frozen tier is now stable for Sharderator testing." if console
         else "The frozen tier is now stable for Sharderator testing.")


def cleanup_cluster(client: Elasticsearch, dry_run: bool = False, console=None) -> None:
    """Remove all test artifacts from the cluster."""
    _log = console.print if console else print

    patterns_to_delete = [
        "test-daily-*",
        "partial-*test*",
        "sharderator-restore-*",
        "sharderator-shrunk-*",
        "sharderator-merged-*",
        "sharderator-new-*",
        "sharderator-tracker",
    ]

    # Delete data streams first (they can't be deleted as plain indices)
    data_streams_to_delete = [s for s, _ in OVERSHARDED_STREAMS]
    for ds in data_streams_to_delete:
        if dry_run:
            _log(f"[yellow]DRY RUN[/yellow] Would delete data stream: {ds}" if console else f"DRY RUN: Would delete data stream: {ds}")
        else:
            try:
                client.indices.delete_data_stream(name=ds)
                _log(f"[green]✓[/green] Deleted data stream: {ds}" if console else f"✓ Deleted data stream: {ds}")
            except Exception as e:
                _log(f"[yellow]⚠[/yellow] {ds}: {e}" if console else f"⚠ {ds}: {e}")

    for pattern in patterns_to_delete:
        if dry_run:
            _log(f"[yellow]DRY RUN[/yellow] Would delete indices: {pattern}" if console else f"DRY RUN: Would delete indices: {pattern}")
        else:
            try:
                client.indices.delete(index=pattern, ignore_unavailable=True, expand_wildcards="all")
                _log(f"[green]✓[/green] Deleted: {pattern}" if console else f"✓ Deleted: {pattern}")
            except Exception as e:
                _log(f"[yellow]⚠[/yellow] {pattern}: {e}" if console else f"⚠ {pattern}: {e}")

    ilm_policies = [ILM_POLICY_OVERSHARDED, ILM_POLICY_DAILY]
    for policy in ilm_policies:
        if dry_run:
            _log(f"[yellow]DRY RUN[/yellow] Would delete ILM policy: {policy}" if console else f"DRY RUN: Would delete ILM policy: {policy}")
        else:
            try:
                client.ilm.delete_lifecycle(name=policy)
                _log(f"[green]✓[/green] Deleted ILM policy: {policy}" if console else f"✓ Deleted ILM policy: {policy}")
            except Exception as e:
                _log(f"[yellow]⚠[/yellow] {policy}: {e}" if console else f"⚠ {policy}: {e}")

    if not dry_run:
        try:
            client.indices.delete_index_template(name="sharderator-test-*")
            _log("[green]✓[/green] Deleted index templates" if console else "✓ Deleted index templates")
        except Exception as e:
            _log(f"[yellow]⚠[/yellow] templates: {e}" if console else f"⚠ templates: {e}")

        # Restore ILM poll interval
        client.cluster.put_settings(
            body={"transient": {"indices.lifecycle.poll_interval": None}}
        )
        _log("[green]✓[/green] Restored ILM poll interval" if console else "✓ Restored ILM poll interval")

    _log("\n[bold]Cleanup complete.[/bold]" if console else "\nCleanup complete.")
