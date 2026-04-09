# Sharderator

A PyQt6 GUI tool for consolidating frozen-tier indices in Elasticsearch. Two modes: shrink over-sharded indices down to 1 shard, or merge dozens of small daily indices into monthly rollups. Frees up your frozen shard budget without losing data.

## The Problem

Elastic Cloud enforces a hard ceiling of 3,000 shards per frozen data node (`cluster.max_shards_per_node.frozen`). Frozen indices are partially mounted searchable snapshots — read-only by design. You can't shrink, reindex, or forcemerge them in place.

Two common patterns eat through the budget:

1. Indices that arrived in frozen with too many primary shards (no shrink step in the ILM warm phase)
2. Daily index patterns that create one index per day per metric type, each with a single shard — 30 days × 40 metric types = 1,200 shards for data that could fit in 40

Sharderator fixes both. The core pipeline is restore → mutate → snapshot → re-mount, adapted from the approach used by Elastic's [PII redaction tool](https://github.com/elastic/pii-tool).

## Requirements

- Python 3.10+
- Elasticsearch 8.7+ (8.12+ recommended)
- Elastic Cloud deployment or self-managed cluster with an object storage snapshot repository (S3/GCS/Azure)

## Installation

```bash
cd sharderator
pip install -e .
```

Or without installing:

```bash
pip install PyQt6 elasticsearch es_client pyyaml keyring structlog
python -m sharderator
```

## Quick Start

```bash
sharderator
```

1. Click Connect — enter Cloud ID + API Key, or Host + Basic Auth
2. The frozen index table populates automatically
3. Pick a mode:
   - Shrink Mode — select over-sharded indices, click Dry Run, then Execute
   - Merge Mode — click Analyze Merges, check the groups you want, Dry Run, then Execute Merge

## Modes

### Shrink Mode

For indices with too many primary shards. Shrinks N shards down to 1 using the Elasticsearch Shrink API (hard-link based, fast). Falls back to reindex if the shard count isn't divisible.

Pipeline: `ANALYZING → RESTORING → AWAITING_RECOVERY → SHRINKING → AWAITING_SHRINK → FORCE_MERGING → SNAPSHOTTING → AWAITING_SNAPSHOT → REMOUNTING → VERIFYING → SWAPPING → CLEANING_UP → COMPLETED`

### Merge Mode

For clusters with too many small indices (the daily index pattern problem). Groups indices by base pattern and time bucket, then reindexes all sources into a single merged index.

Grouping options: monthly (default), quarterly, yearly.

Pipeline: `ANALYZING → RESTORING → AWAITING_RECOVERY → MERGING → AWAITING_MERGE → FORCE_MERGING → SNAPSHOTTING → AWAITING_SNAPSHOT → REMOUNTING → VERIFYING → SWAPPING → CLEANING_UP → COMPLETED`

Restores happen in configurable batches (default 3 at a time) to avoid saturating cluster I/O.

## Safety Features

Sharderator is designed to run against production clusters with live traffic.

- Dry run exercises real pre-flight checks (cluster health, circuit breakers, disk space) without touching data
- Hard disk space gate — refuses to restore if the working tier would drop below the safety margin (default 30% free)
- Pre-flight cluster health check — blocks on red, warns on yellow (configurable), checks circuit breaker pressure
- Mid-operation health monitoring — if the cluster goes red during execution, pauses and waits up to 15 minutes before aborting
- Restore batching — merge mode restores in batches of N (default 3) instead of blasting all at once
- Reindex throttle — configurable `requests_per_second` (default 5000) to avoid starving production ingest
- Data stream swaps happen AFTER verification — if doc count or mapping checks fail, the data stream still points at the old (working) index
- Force merge deduplication — checks for already-running force merges before starting, recovers from HTTP timeouts
- Snapshot deletion checks frozen, cold, and manually mounted indices before removing
- Old data is never deleted until verification passes
- Job state persisted to `sharderator-tracker` index for crash recovery
- Cancellable at any state boundary

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Target shard count | 1 | Shard target for shrink mode |
| Snapshot repository | auto-detected | Which repo to snapshot into |
| Recovery timeout | 30 min | Timeout for restore/shrink operations |
| Delete old snapshots | Off | Remove original snapshot after verification |
| Disk safety margin | 0.30 | Keep 30% disk free on working tier |
| Restore batch size | 3 | Max concurrent snapshot restores in merge mode |
| Reindex throttle | 5000 docs/sec | Rate limit for reindex operations (-1 = unlimited) |
| Allow yellow cluster | On | Permit operations when cluster health is yellow |
| Ignore circuit breakers | Off | Override circuit breaker safety check |

Settings persist to `~/.sharderator/config.yaml`. Credentials stored via OS keyring.

## Data Stream Handling

Both modes handle data stream backing indices:

- Detects membership during analysis
- Shrink mode: swaps one backing index after verification
- Merge mode: atomic swap of all source backing indices in a single `_data_stream/_modify` call
- Verifies each source is still a data stream member before attempting removal (guards against parallel ILM actions)

## Project Structure

```
sharderator/
├── pyproject.toml
├── README.md
├── technical-design.md
└── src/sharderator/
    ├── __main__.py
    ├── gui/
    │   ├── main_window.py        # Main window with Shrink/Merge tabs
    │   ├── connection_dialog.py   # Auth configuration
    │   ├── settings_dialog.py     # Operation + safety settings
    │   ├── confirm_dialog.py      # Shrink + merge confirmation popups
    │   ├── index_table.py         # Frozen index table model/view
    │   ├── merge_tree.py          # Merge candidate tree with lazy validation
    │   ├── job_log.py             # Log viewer
    │   └── progress.py            # Progress bar
    ├── engine/
    │   ├── orchestrator.py        # Shrink pipeline state machine
    │   ├── merge_orchestrator.py  # Merge pipeline state machine
    │   ├── preflight.py           # Cluster health + disk + breaker checks
    │   ├── analyzer.py            # Index analysis
    │   ├── restorer.py            # Snapshot restore + disk gate
    │   ├── shrinker.py            # Shrink API + reindex fallback
    │   ├── merger.py              # Multi-source restore + reindex
    │   ├── merge_analyzer.py      # Index grouping + validation
    │   ├── snapshotter.py         # Force-merge + snapshot
    │   ├── mounter.py             # Frozen re-mount (no data stream swap)
    │   ├── swapper.py             # Data stream swap (post-verification)
    │   ├── verifier.py            # Doc count + mapping + health checks
    │   └── cleaner.py             # Cleanup with cold-tier mount checks
    ├── client/
    │   ├── connection.py          # ES client + cluster discovery
    │   ├── queries.py             # Reusable queries + data stream cache
    │   └── tracker.py             # Job persistence to ES tracking index
    ├── models/
    │   ├── index_info.py          # Index metadata dataclass
    │   └── job.py                 # Job state machine + type discriminator
    └── util/
        ├── logging.py             # Structured logging (structlog)
        └── config.py              # YAML config + keyring secrets
```

## Troubleshooting

- "Elasticsearch X.Y.Z is below minimum 8.7.0" — Requires ES 8.7+ for `_mount` with `storage=shared_cache` and `_data_stream/_modify`
- "Cannot determine source snapshot" — Index was manually mounted without `index.store.snapshot.*` metadata
- "InsufficientDiskError" — Working tier doesn't have enough free space. Reduce batch size, process fewer groups, or add warm/hot capacity
- "ClusterNotHealthyError" — Cluster is red, has too many relocating shards, or circuit breakers are tripped. Wait for the cluster to stabilize, or enable the override in Settings
- Recovery/reindex timeouts — Increase the timeout in Settings, or ensure the working tier has adequate resources

## License

MIT
