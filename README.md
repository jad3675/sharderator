# Sharderator

A PyQt6 GUI tool (with a headless CLI) for consolidating frozen-tier indices in Elasticsearch. Two modes: shrink over-sharded indices down to 1 shard, or merge dozens of small daily indices into monthly/quarterly/yearly rollups. Frees up your frozen shard budget without losing data.

## The Problem

Elastic Cloud enforces a hard ceiling of 3,000 shards per frozen data node (`cluster.max_shards_per_node.frozen`). Frozen indices are partially mounted searchable snapshots — read-only by design. You can't shrink, reindex, or forcemerge them in place.

Two common patterns eat through the budget:

1. Indices that arrived in frozen with too many primary shards (no shrink step in the ILM warm phase)
2. Daily index patterns that create one index per day per metric type, each with a single shard — 30 days × 40 metric types = 1,200 shards for data that could fit in 40

Sharderator fixes both. The core pipeline is restore → mutate → snapshot → re-mount, adapted from the approach used by Elastic's [PII redaction tool](https://github.com/elastic/pii-tool).

## Requirements

- Python 3.10+
- Elasticsearch 8.7+ (8.12+ recommended for full feature set)
- Elastic Cloud deployment or self-managed cluster with an object storage snapshot repository (S3/GCS/Azure)

## Installation

```bash
# GUI + CLI
pip install "sharderator[gui]"

# CLI only (no PyQt6 required)
pip install sharderator
```

Or from source:

```bash
cd sharderator
pip install -e ".[gui]"   # GUI + CLI
pip install -e .           # CLI only
```

## Quick Start

### GUI

```bash
sharderator
```

1. Click **Connect** — enter Cloud ID + API Key, or Host + Basic Auth
2. The frozen index table populates automatically, sorted by shard count
3. Pick a mode:
   - **Shrink Mode** — select over-sharded indices, click Dry Run, then Execute
   - **Merge Mode** — click Analyze Merges, check the groups you want, Dry Run, then Execute Merge
   - **Job Tracker** — view in-flight/failed jobs, resume or delete them
4. Press **F5** or click **Refresh** to reload cluster state after operations complete

The defrag visualization grid at the bottom shows real-time progress — each shard gets its own 10×10 cell in a continuous grid, colored by pipeline state. Completed indices collapse: target cells turn green, excess cells go dark ("freed"), showing the shard budget being reclaimed. Hover any cell for index name, shard number, state, and progress.

### CLI

```bash
# List all frozen indices
sharderator-cli list --cloud-id $CLOUD_ID --api-key $ES_API_KEY

# Show cluster status and shard budget
sharderator-cli status --cloud-id $CLOUD_ID --api-key $ES_API_KEY

# Dry run shrink — shows sizing report and preflight results
sharderator-cli shrink --cloud-id $CLOUD_ID --api-key $ES_API_KEY --min-shards 5 --dry-run

# Execute shrink, smallest indices first
sharderator-cli shrink --cloud-id $CLOUD_ID --api-key $ES_API_KEY --min-shards 5 --order smallest-first

# Dry run merge with monthly granularity
sharderator-cli merge --cloud-id $CLOUD_ID --api-key $ES_API_KEY --granularity monthly --dry-run

# Execute merge, prioritize specific patterns first
sharderator-cli merge --cloud-id $CLOUD_ID --api-key $ES_API_KEY \
  --priority "metrics-host.*,metrics-network.*" \
  --order smallest-first

# Use environment variables for credentials
export ES_API_KEY=your-api-key
sharderator-cli list --cloud-id $CLOUD_ID
```

The CLI includes a terminal defrag visualization (ANSI-colored block characters that wrap to terminal width), signal handling for graceful cancellation (Ctrl+C), pre-run sizing reports, and all the same safety checks as the GUI.

## Modes

### Shrink Mode

For indices with too many primary shards. Uses the Elasticsearch Shrink API (hard-link based, fast). Falls back to async reindex if the shard count isn't divisible by the target.

Pipeline:
```
ANALYZING → RESTORING → AWAITING_RECOVERY → SHRINKING →
AWAITING_SHRINK (or AWAITING_REINDEX) → FORCE_MERGING →
SNAPSHOTTING → AWAITING_SNAPSHOT → REMOUNTING → VERIFYING →
SWAPPING → CLEANING_UP → COMPLETED
```

### Merge Mode

For clusters with too many small indices (the daily index pattern problem). Groups indices by base pattern and time bucket, then reindexes all sources into a single merged index.

Grouping options: monthly (default), quarterly, yearly.

Pipeline:
```
ANALYZING → RESTORING → AWAITING_RECOVERY → MERGING →
AWAITING_MERGE → FORCE_MERGING → SNAPSHOTTING → AWAITING_SNAPSHOT →
REMOUNTING → VERIFYING → SWAPPING → CLEANING_UP → COMPLETED
```

Restores happen in dynamic batches sized by available disk budget (not a fixed count) to avoid saturating cluster I/O.

**Mapping conflict transparency:** When source indices have fields with different types (e.g., `status` is `keyword` in some indices and `text` in others), `union_mappings` resolves the conflict by keeping the first definition it encounters. This is logged at WARNING level during execution and shown in the CLI dry-run output and GUI merge tree tooltip so the operator knows before committing. The merge is not blocked — yellow warning, not red.

## Safety Features

Sharderator is designed to run against production clusters with live traffic.

**Pre-flight checks (run before every operation and during dry runs):**
- Cluster health gate — blocks on red, configurable on yellow
- Circuit breaker check — blocks if any breaker is above 80% capacity (overridable)
- Disk space gate — refuses to restore if working tier would drop below safety margin (default 30% free)

**Mid-operation monitoring (checked between every pipeline stage):**
- Cluster health — if cluster goes red, pauses up to 15 minutes waiting for recovery
- Thread pool queue depths (write, search, snapshot) — warns and backs off if queues are deep
- Pending cluster tasks — warns if master node is saturated
- JVM heap pressure — warns if any working tier node is above 85% heap

**Data integrity:**
- Data stream swaps happen AFTER verification — if doc count or mapping checks fail, the data stream still points at the old (working) index
- Atomic multi-DS swap — all add/remove actions across all data streams in a single API call (no partial-failure window)
- Add-before-remove ordering — partial failure leaves both old and new indices, never a missing time range
- Old data is never deleted until verification passes

**Operational safety:**
- Force merge deduplication — checks for already-running force merges before starting, recovers from HTTP timeouts
- Snapshot deletion checks frozen, cold, and manually mounted indices before removing
- Reindex throttle — configurable `requests_per_second` (default 5000) to avoid starving production ingest
- Continuous disk monitoring — rechecks disk before each restore batch and before reindex
- Thread-safe cancellation using `threading.Event`
- Graceful shutdown — closing the GUI during a running pipeline prompts for confirmation, persists job state for resume

**Adaptive timeouts by index size:**

| Tier | Size | Recovery Timeout | Reindex Throttle |
|------|------|-----------------|-----------------|
| Small | < 1 GB | 10 min | Unlimited |
| Medium | 1–50 GB | 30 min | 10,000 docs/sec |
| Large | 50–500 GB | 2 hours | 5,000 docs/sec |
| Huge | > 500 GB | 6 hours | 2,000 docs/sec |

Timeouts and throttles are automatically applied based on index size. Config values serve as minimums.

**Crash recovery:**
- Job state persisted to `sharderator-tracker` Elasticsearch index after every state transition
- If the tool crashes, restart it and the pipeline resumes from the last committed state
- Cancellable at any state boundary

## Export

Three export types for change ticket documentation and operational record-keeping:

**Frozen index list** — Export button on the Shrink Mode tab saves the current frozen index table as CSV or JSON. CLI: `sharderator-cli list --csv` or `--json`.

**Dry-run sizing report** — "Export Dry Run" button on the Merge Mode tab saves the pre-run sizing report plus per-group detail and preflight results as JSON. CLI: `sharderator-cli shrink --dry-run --output json` or `sharderator-cli merge --dry-run --output json`.

**Job history** — Export button on the Job Tracker tab saves all tracked jobs as JSON. Right-click any row → "Export Job" to save a single job record. CLI: `sharderator-cli report` (all jobs) or `sharderator-cli report --index <name>` (single job), with optional `--state COMPLETED|FAILED|all` filter.

## Job Tracker

The third tab shows the contents of the `sharderator-tracker` index — all in-flight, failed, and interrupted jobs. Color-coded: red for failed, yellow for awaiting states.

Right-click context menu:
- **Resume** — re-queue a stuck job from its last committed state (both SHRINK and MERGE jobs supported)
- **Delete** — remove the tracker record (for clean restarts)
- **Copy Error** — copy the error message to clipboard
- **View History** — popup showing the full state transition timeline with timestamps

## Refresh

The **Refresh** button in the toolbar (also bound to **F5**) reloads cluster metadata (health, shard count), the frozen index list, and the tracker tab in one shot. Also fires automatically after every batch completion.

## Pre-Run Sizing Report

Before any operation executes, a sizing report is generated showing:
- Total source data size and largest single index
- Working tier capacity and estimated peak disk usage
- Whether everything fits within the safety margin
- Projected shard count before and after
- Shards reclaimed

Available in both GUI dry runs and CLI `--dry-run`.

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Target shard count | 1 | Shard target for shrink mode |
| Snapshot repository | auto-detected | Which repo to snapshot into |
| Recovery timeout | 30 min | Minimum timeout (tier profile may increase) |
| Delete old snapshots | Off | Remove original snapshot after verification |
| Disk safety margin | 0.30 | Keep 30% disk free on working tier |
| Restore batch size | 3 | Max concurrent snapshot restores (dynamic batching may reduce) |
| Reindex throttle | 5000 docs/sec | Rate limit (-1 = unlimited; tier profile may reduce) |
| Allow yellow cluster | On | Permit operations when cluster health is yellow |
| Ignore circuit breakers | Off | Override circuit breaker safety check |

Settings persist to `~/.sharderator/config.yaml`. Credentials stored via OS keyring.

## CLI Reference

### Connection flags (all subcommands)

| Flag | Description |
|------|-------------|
| `--cloud-id` | Elastic Cloud deployment ID |
| `--hosts` | Elasticsearch host URLs |
| `--api-key` | API key (or `ES_API_KEY` env var) |
| `--username` / `--password` | Basic auth (or `ES_PASSWORD` env var) |
| `--no-verify-certs` | Disable TLS verification |

### `sharderator-cli shrink`

| Flag | Default | Description |
|------|---------|-------------|
| `--index` / `-i` | all eligible | Specific index names |
| `--min-shards` | 2 | Only process indices with ≥ N shards |
| `--max-indices` | 0 (all) | Limit number of indices |
| `--order` | smallest-first | `smallest-first`, `largest-first`, `as-is` |
| `--dry-run` | — | Preflight checks + sizing report only |

### `sharderator-cli merge`

| Flag | Default | Description |
|------|---------|-------------|
| `--granularity` | monthly | `monthly`, `quarterly`, `yearly` |
| `--pattern` | all | Filter by base pattern substring |
| `--max-groups` | 0 (all) | Limit merge groups |
| `--order` | smallest-first | `smallest-first`, `largest-first`, `as-is` |
| `--priority` | — | Comma-separated patterns to process first |
| `--dry-run` | — | Preflight checks + sizing report only |

## Data Stream Handling

Both modes handle data stream backing indices:
- Detects membership during analysis (cached — one query for all indices in a group)
- Shrink mode: swaps one backing index after verification
- Merge mode: atomic swap of all source backing indices in a single `_data_stream/_modify` call
- Verifies each source is still a data stream member before attempting removal

## Project Structure

```
sharderator/
├── pyproject.toml
├── README.md
├── technical-design.md
├── tests/
│   ├── test_job.py               # State machine + serialization
│   ├── test_index_info.py        # IndexInfo properties
│   ├── test_merge_analyzer.py    # Grouping + mapping union
│   ├── test_preflight.py         # Cluster health + breaker checks
│   ├── test_defrag_widget.py     # Defrag visualization
│   └── test_sizing.py            # Size tiers + ordering + batching
└── src/sharderator/
    ├── __main__.py               # GUI entry point
    ├── cli.py                    # CLI entry point (no PyQt6 required)
    ├── gui/
    │   ├── main_window.py        # Main window (Shrink/Merge/Tracker tabs)
    │   ├── defrag_widget.py      # Per-shard cell grid visualization
    │   ├── qt_events.py          # Qt signal adapter for PipelineEvents
    │   ├── connection_dialog.py  # Auth configuration
    │   ├── settings_dialog.py    # Operation + safety settings
    │   ├── confirm_dialog.py     # Shrink + merge confirmation popups
    │   ├── index_table.py        # Frozen index table model/view
    │   ├── merge_tree.py         # Merge candidate tree with lazy validation
    │   ├── tracker_table.py      # Job tracker table + context menu
    │   ├── job_history_dialog.py # State transition timeline popup
    │   ├── job_log.py            # Log viewer
    │   └── progress.py           # Progress bar
    ├── engine/
    │   ├── orchestrator.py       # Shrink pipeline state machine
    │   ├── merge_orchestrator.py # Merge pipeline state machine
    │   ├── events.py             # PipelineEvents protocol + NullEvents
    │   ├── preflight.py          # Cluster health + disk + breaker checks
    │   ├── sizing.py             # Size tiers, ordering, dynamic batching, sizing reports
    │   ├── task_waiter.py        # Shared async task polling
    │   ├── analyzer.py           # Index analysis
    │   ├── restorer.py           # Snapshot restore + disk gate
    │   ├── shrinker.py           # Shrink API + async reindex fallback
    │   ├── merger.py             # Multi-source batched restore + reindex
    │   ├── merge_analyzer.py     # Index grouping + validation
    │   ├── snapshotter.py        # Force-merge (with dedup) + snapshot
    │   ├── mounter.py            # Frozen re-mount (mount only, no DS swap)
    │   ├── swapper.py            # Atomic data stream swap with retry
    │   ├── verifier.py           # Doc count + mapping + health checks
    │   └── cleaner.py            # Cleanup with cold-tier mount checks
    ├── client/
    │   ├── connection.py         # ES client + cluster discovery
    │   ├── queries.py            # Reusable queries + DS membership cache
    │   └── tracker.py            # Job persistence to ES tracking index
    ├── models/
    │   ├── index_info.py         # Index metadata dataclass
    │   └── job.py                # Job state machine + type discriminator
    └── util/
        ├── logging.py            # Structured logging (structlog)
        └── config.py             # YAML config + keyring secrets
```

## Troubleshooting

- **"Elasticsearch X.Y.Z is below minimum 8.7.0"** — Requires ES 8.7+ for `_mount` with `storage=shared_cache` and `_data_stream/_modify`
- **"Cannot determine source snapshot"** — Index was manually mounted without `index.store.snapshot.*` metadata
- **"InsufficientDiskError"** — Working tier doesn't have enough free space. Reduce batch size, process fewer groups, or add warm/hot capacity
- **"ClusterNotHealthyError"** — Cluster is red, has too many relocating shards, or circuit breakers are tripped. Wait for the cluster to stabilize, or enable the override in Settings
- **Recovery/reindex timeouts** — Increase the timeout in Settings, or ensure the working tier has adequate resources. Large indices (>50GB) automatically get longer timeouts via size tier profiles
- **Merge dry run shows "BLOCKED — Circuit breaker..."** — Enable "Ignore circuit breakers" in Settings to override. The tiebreaker node often has a small heap that trips the 80% threshold

## License

MIT
