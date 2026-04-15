"""Data generators for both test scenarios."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from elasticsearch import Elasticsearch, helpers

from sharderator_test.fixtures import (
    DAILY_MAPPINGS,
    ILM_POLICY_DAILY,
    METRIC_TYPES,
    OVERSHARDED_STREAMS,
)

HOSTS = [f"host-{i:03d}" for i in range(50)]
DAILY_HOSTS = [f"host-{i:03d}" for i in range(20)]


def ingest_oversharded(
    client: Elasticsearch,
    docs_per_stream: int = 500_000,
    batch_size: int = 5_000,
    console=None,
) -> None:
    """Ingest data into over-sharded data streams.

    Each stream gets enough data to trigger multiple rollovers (50MB threshold),
    producing several frozen indices with 10/20/50 primary shards each.
    """
    _log = console.print if console else print

    _log(f"\nIngesting {docs_per_stream:,} docs into {len(OVERSHARDED_STREAMS)} streams...")

    for stream_name, shard_count in OVERSHARDED_STREAMS:
        _log(f"  [{shard_count} shards] {stream_name}...")
        success, errors = helpers.bulk(
            client,
            _generate_oversharded_docs(stream_name, docs_per_stream),
            chunk_size=batch_size,
            request_timeout=120,
            raise_on_error=False,
        )
        if errors:
            # Show the first error so the operator knows what's wrong
            first_err = errors[0]
            err_info = first_err.get("index", first_err.get("create", {}))
            err_reason = err_info.get("error", {}).get("reason", str(first_err))
            _log(f"    [yellow]⚠ {len(errors)} errors — first: {err_reason[:200]}[/yellow]" if console
                 else f"    ⚠ {len(errors)} errors — first: {err_reason[:200]}")
        _log(f"    [green]✓[/green] {success:,} docs" if console else f"    ✓ {success:,} docs")

    _log("\n[bold]Oversharded ingest complete.[/bold]" if console else "\nOversharded ingest complete.")
    _log("Wait for ILM to roll over and freeze indices.")
    _log("Monitor: [cyan]GET _cat/indices/partial-*test*?v&s=index[/cyan]" if console else "Monitor: GET _cat/indices/partial-*test*?v&s=index")


def _generate_oversharded_docs(stream: str, count: int):
    now = datetime.now(timezone.utc)
    for i in range(count):
        ts = now - timedelta(seconds=random.randint(0, 86400))
        yield {
            "_index": stream,
            "_op_type": "create",  # data streams require op_type=create
            "_source": {
                "@timestamp": ts.isoformat(),
                "host": {"name": random.choice(HOSTS)},  # nested, not dotted
                "cpu": {"pct": round(random.uniform(0.0, 1.0), 4)},
                "memory": {"pct": round(random.uniform(0.3, 0.95), 4)},
                "message": f"Synthetic test data for {stream} doc {i}",
            },
        }


def ingest_daily(
    client: Elasticsearch,
    days: int = 90,
    docs_per_index: int = 10_000,
    batch_size: int = 5_000,
    console=None,
) -> None:
    """Create daily indices for each metric type over N days.

    Creates days × len(METRIC_TYPES) indices, each with 1 shard.
    After ILM freezes them, Sharderator merge mode can consolidate
    them into monthly rollups.
    """
    _log = console.print if console else print

    total = days * len(METRIC_TYPES)
    _log(f"\nCreating {total:,} daily indices ({days} days × {len(METRIC_TYPES)} metric types)...")
    _log(f"Docs per index: {docs_per_index:,}")

    today = datetime.now(timezone.utc).date()
    created = 0
    skipped = 0

    for day_offset in range(days):
        date = today - timedelta(days=days - day_offset)
        date_str = date.strftime("%Y.%m.%d")
        dt = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)

        for metric in METRIC_TYPES:
            index_name = f"test-daily-{metric}-{date_str}"

            if not client.indices.exists(index=index_name):
                client.indices.create(
                    index=index_name,
                    settings={
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "index.lifecycle.name": ILM_POLICY_DAILY,
                    },
                    mappings=DAILY_MAPPINGS,
                )
                helpers.bulk(
                    client,
                    _generate_daily_docs(index_name, metric, dt, docs_per_index),
                    chunk_size=batch_size,
                    request_timeout=60,
                    raise_on_error=False,
                )
                created += 1
            else:
                skipped += 1

            if (created + skipped) % 100 == 0:
                _log(f"  [{created + skipped}/{total}] {index_name}" if not console else
                     f"  [[cyan]{created + skipped}/{total}[/cyan]] {index_name}")

    _log(f"\n[bold]Daily ingest complete.[/bold]" if console else "\nDaily ingest complete.")
    _log(f"Created: {created:,}  Skipped (already exist): {skipped:,}")
    _log(f"Total shards: {created:,}")
    _log("Wait for ILM to freeze them.")


def _generate_daily_docs(index_name: str, metric: str, date: datetime, count: int):
    for _ in range(count):
        ts = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        yield {
            "_index": index_name,
            "_source": {
                "@timestamp": ts.isoformat(),
                "host": {"name": random.choice(DAILY_HOSTS)},
                "value": round(random.uniform(0.0, 100.0), 4),
                "metric": {"type": metric},
                "tags": random.sample(
                    ["production", "staging", "dev", "us-east", "eu-west"], 2
                ),
            },
        }
