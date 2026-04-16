# Sharderator

A PyQt6 GUI tool (with a headless CLI) for consolidating frozen-tier indices in Elasticsearch. Two modes: shrink over-sharded indices down to 1 shard, or merge dozens of small daily indices into monthly/quarterly/yearly rollups. Frees up your frozen shard budget without losing data.

## The Problem

Elastic Cloud enforces a hard ceiling on shards per frozen data node (`cluster.max_shards_per_node.frozen` — 3,000 default, 1,000 on trial clusters). Frozen indices are partially mounted searchable snapshots — read-only by design. You can't shrink, reindex, or forcemerge them in place.

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
cd es-shard-defrag
pip install -e ".[gui]"   # GUI + CLI
pip install -e .           # CLI only
```

## Quick Start

### GUI

```bash
sharderator
```

1. Click **Connect** — enter Cloud ID + API Key, or Host + Basic Auth
2. The **Frozen Analyze** tab populates automatically with a shard budget bar, over-sharded indices, mergeable patterns, and recommendations
3. Pick a mode:
   - **Shrink Mode** — select over-sharded indices (filter + Select All for bulk selection), click Dry Run, then Execute
   - **Merge Mode** — click Analyze Merges, check the groups you want (filter + Select All), Dry Run, then Execute Merge
   - **Job Tracker** — view in-flight/failed jobs, resume or delete them
4. Press **F5** or click **Refresh** to reload cluster state after operations complete

The defrag visualization grid at the bottom shows real-time progress — each shard gets its own 10×10 cell in a continuous grid, colored by pipeline state. Completed indices collapse: target cells turn green, excess cells go dark ("freed"), showing the shard budget being reclaimed. Hover any cell for index name, shard number, state, and progress.

### CLI

```bash
# Analyze frozen tier health — shard budget, over-sharded indices, merge opportunities
sharderator-cli analyze --cloud-id $CLOUD_ID --api-key $ES_API_KEY

# Same as JSON for automation
sharderator-cli analyze --cloud-id $CLOUD_ID --api-key $ES_API_KEY --json

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

## Frozen Analyze

The first tab in the GUI (and the `analyze` CLI subcommand) provides a frozen tier health report — answering "what does my frozen tier look like and where are the problems?" before you commit to any action.

**Shard budget bar** — color-coded progress bar showing total frozen shards vs. the cluster limit. Green (<75%), yellow (75-90%), orange (90-100%), red (over limit).

**Summary cards** — four at-a-glance metrics:
- **Over-Sharded** — indices with ≥2 shards that are candidates for shrink mode, with total reclaimable shards
- **Mergeable** — base patterns with multiple daily indices that can be consolidated, with total reclaimable shards
- **Already Optimal** — indices with 1 shard that need no action
- **Processed** — indices already handled by Sharderator (count of `-sharderated` and `-merged` suffixed indices)

**Over-sharded table** — sortable table of shrink candidates, sorted by shard count descending (biggest offenders first). Shows index name, current shards, target, savings, and size.

**Mergeable patterns table** — sortable table of merge candidates, sorted by monthly savings descending. Shows base pattern, index count, current shards, projected shards after monthly merge, savings, and total size.

**Recommendation bar** — bottom-line summary: total reclaimable shards, breakdown by shrink vs. merge, and projected budget after consolidation.

The analysis auto-populates on connect and refreshes with F5. CLI: `sharderator-cli analyze` (text) or `sharderator-cli analyze --json`.

**Export:** The Export button on the Analyze tab saves the report as HTML (opens in any browser, prints to PDF via Ctrl+P) or JSON. The HTML report includes a color-coded budget bar, summary cards, sortable tables, and recommendations — professional enough for a change ticket attachment.

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
- Circuit breaker check — waits with adaptive backoff if any breaker is above 80% capacity (configurable timeout, overridable)
- Disk space gate — refuses to restore if working tier would drop below safety margin (default 30% free)

**Circuit breaker adaptive backpressure:**
- The per-index preflight gate (`check_circuit_breakers`) now waits with linear backoff (30s→60s→90s… capped at 300s) instead of failing instantly. Configurable via `--breaker-wait` (default 10 minutes, 0 = fail-fast)
- Mid-operation pressure checks include circuit breaker ratios at a 75% threshold — 5 points below the hard gate — so pauses kick in before the next index's preflight would reject
- Between batch items, `wait_for_cluster_ready` combines health + breaker + pressure checks with adaptive waiting. Configurable via `--backpressure-wait` (default 15 minutes, 0 = no inter-index waiting)
- If all checks pass immediately, there is zero overhead — no artificial delay on healthy clusters

**Mid-operation monitoring (checked between every pipeline stage):**
- Cluster health — if cluster goes red, pauses up to 15 minutes waiting for recovery
- Circuit breaker ratios — pauses if any breaker exceeds 75%
- Thread pool queue depths (write, search, snapshot) — warns and backs off if queues are deep
- Pending cluster tasks — warns if master node is saturated
- JVM heap pressure — warns if any working tier node is above 85%

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
- Orphaned intermediate index cleanup — restorer and shrinker detect and remove stale `sharderator-restore-*` and `sharderator-shrunk-*` indices from previous failed runs
- Mount name collision handling — `remount()` and `remount_merged()` check for and delete stale mounts before mounting

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

Five export types for change ticket documentation and operational record-keeping:

**Frozen tier analysis** — Export button on the Frozen Analyze tab saves the full health report as HTML (printable to PDF via Ctrl+P) or JSON. Includes budget bar, over-sharded indices, mergeable patterns, and recommendations. CLI: `sharderator-cli analyze --json`.

**Post-operation change report** — After every batch completes, the completion dialog includes a "Save Report" button. Saves an HTML or JSON report documenting what changed: before/after shard counts, shards reclaimed, list of completed and failed items with error details. Designed for change control ticket attachments.

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
| Circuit breaker wait | 10 min | How long to wait for breaker pressure to subside (0 = fail immediately) |
| Batch backpressure wait | 15 min | How long to pause between batch items under pressure (0 = no wait) |

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
| `--output` | table | `table` or `json` (dry-run format) |
| `--breaker-wait` | 10 | Minutes to wait for circuit breaker pressure (0 = fail immediately) |
| `--backpressure-wait` | 15 | Minutes to wait between batch items under pressure (0 = no wait) |
| `--ignore-circuit-breakers` | — | Skip circuit breaker checks entirely |

### `sharderator-cli merge`

| Flag | Default | Description |
|------|---------|-------------|
| `--granularity` | monthly | `monthly`, `quarterly`, `yearly` |
| `--pattern` | all | Filter by base pattern substring |
| `--max-groups` | 0 (all) | Limit merge groups |
| `--order` | smallest-first | `smallest-first`, `largest-first`, `as-is` |
| `--priority` | — | Comma-separated patterns to process first |
| `--dry-run` | — | Preflight checks + sizing report only |
| `--output` | table | `table` or `json` (dry-run format) |
| `--breaker-wait` | 10 | Minutes to wait for circuit breaker pressure (0 = fail immediately) |
| `--backpressure-wait` | 15 | Minutes to wait between batch items under pressure (0 = no wait) |
| `--ignore-circuit-breakers` | — | Skip circuit breaker checks entirely |

### `sharderator-cli analyze`

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | — | Output as JSON for automation |
| `--min-shards` | 2 | Minimum shard count to flag as over-sharded |

### `sharderator-cli report`

| Flag | Default | Description |
|------|---------|-------------|
| `--index` | — | Specific job by index name |
| `--state` | all | `COMPLETED`, `FAILED`, or `all` |

## Data Stream Handling

Both modes handle data stream backing indices:
- Detects membership during analysis (cached — one query for all indices in a group)
- Shrink mode: swaps one backing index after verification
- Merge mode: atomic swap of all source backing indices in a single `_data_stream/_modify` call
- Verifies each source is still a data stream member before attempting removal

## End-to-End Test Suite

A separate `sharderator-test-suite/` package provides end-to-end validation against a real Elastic Cloud trial deployment. It creates two realistic frozen-tier shard pressure scenarios and runs Sharderator's shrink and merge modes against them.

See [`sharderator-test-suite/README.md`](../sharderator-test-suite/README.md) for full documentation.

```bash
cd sharderator-test-suite
pip install -e .

# Configure cluster, ingest test data, watch ILM freeze it
sharderator-test setup
sharderator-test ingest-oversharded
sharderator-test ingest-daily --days 7
sharderator-test status --watch

# Run validation phases
sharderator-test validate --phase read-only
sharderator-test validate --phase dry-run
sharderator-test validate --phase shrink
sharderator-test validate --phase merge
sharderator-test validate --phase export
```

**Scenarios:**
- **Scenario A** — Over-sharded data streams (10/20/50 primary shards) pushed to frozen via ILM with `max_age: 1m` rollover
- **Scenario B** — Daily index explosion (N days × 40 metric types) creating single-shard indices

**Validation phases:** read-only (list/status), dry-run (shrink + merge JSON export), shrink (single + batch), merge (single group + batch), crash-recovery (interrupt + resume), export (CSV/JSON).

## Project Structure

```
es-shard-defrag/                      # Main Sharderator package
├── pyproject.toml
├── tests/
│   ├── test_job.py                   # State machine + serialization
│   ├── test_index_info.py            # IndexInfo properties
│   ├── test_merge_analyzer.py        # Grouping + mapping union
│   ├── test_preflight.py             # Health gates + circuit breaker backpressure
│   ├── test_defrag_widget.py         # Defrag visualization
│   ├── test_sizing.py                # Size tiers + ordering + batching
│   ├── test_snapshotter.py           # Force merge deduplication
│   └── test_task_waiter.py           # Transient failure classification + retry
└── src/sharderator/
    ├── __main__.py                   # GUI entry point
    ├── cli.py                        # CLI entry point (no PyQt6 required)
    ├── gui/
    │   ├── main_window.py            # Main window (Analyze/Shrink/Merge/Tracker tabs)
    │   ├── analyze_widget.py         # Frozen tier health report tab
    │   ├── defrag_widget.py          # Per-shard cell grid visualization
    │   ├── qt_events.py              # Qt signal adapter for PipelineEvents
    │   ├── connection_dialog.py      # Auth configuration
    │   ├── settings_dialog.py        # Operation + safety settings
    │   ├── confirm_dialog.py         # Shrink + merge confirmation popups
    │   ├── index_table.py            # Frozen index table model/view + filter proxy
    │   ├── merge_tree.py             # Merge candidate tree with lazy validation
    │   ├── tracker_table.py          # Job tracker table + context menu
    │   ├── job_history_dialog.py     # State transition timeline popup
    │   ├── job_log.py                # Log viewer
    │   └── progress.py               # Progress bar
    ├── engine/
    │   ├── orchestrator.py           # Shrink pipeline state machine
    │   ├── merge_orchestrator.py     # Merge pipeline state machine
    │   ├── frozen_analyzer.py        # Frozen tier health analysis
    │   ├── events.py                 # PipelineEvents protocol + NullEvents
    │   ├── preflight.py              # Health + disk + breaker checks + backpressure
    │   ├── sizing.py                 # Size tiers, ordering, dynamic batching, reports
    │   ├── task_waiter.py            # Shared async task polling
    │   ├── analyzer.py               # Index analysis
    │   ├── restorer.py               # Snapshot restore + disk gate
    │   ├── shrinker.py               # Shrink API + async reindex fallback
    │   ├── merger.py                 # Multi-source batched restore + reindex
    │   ├── merge_analyzer.py         # Index grouping + validation
    │   ├── snapshotter.py            # Force-merge (with dedup) + snapshot
    │   ├── mounter.py                # Frozen re-mount (mount only, no DS swap)
    │   ├── swapper.py                # Atomic data stream swap with retry
    │   ├── verifier.py               # Doc count + mapping + health checks
    │   └── cleaner.py                # Cleanup with cold-tier mount checks
    ├── client/
    │   ├── connection.py             # ES client + cluster discovery
    │   ├── queries.py                # Reusable queries + DS membership cache
    │   └── tracker.py                # Job persistence to ES tracking index
    ├── models/
    │   ├── index_info.py             # Index metadata dataclass
    │   └── job.py                    # Job state machine + type discriminator
    └── util/
        ├── logging.py                # Structured logging (structlog)
        └── config.py                 # YAML config + keyring secrets

sharderator-test-suite/               # End-to-end test harness (separate package)
├── pyproject.toml
├── README.md
├── .env.example
└── src/sharderator_test/
    ├── cli.py                        # Click CLI (setup, ingest, status, validate, cleanup)
    ├── connection.py                 # ES client factory with .env loading
    ├── fixtures.py                   # ILM policies, index templates, cluster config
    ├── ingest.py                     # Data generators (over-sharded + daily)
    ├── status.py                     # Cluster status + ILM watch mode
    └── validate.py                   # Validation phases (read-only, dry-run, shrink, merge, export)
```

## Troubleshooting

- **"Elasticsearch X.Y.Z is below minimum 8.7.0"** — Requires ES 8.7+ for `_mount` with `storage=shared_cache` and `_data_stream/_modify`
- **"Cannot determine source snapshot"** — Index was manually mounted without `index.store.snapshot.*` metadata
- **"InsufficientDiskError"** — Working tier doesn't have enough free space. Reduce batch size, process fewer groups, or add warm/hot capacity
- **"ClusterNotHealthyError"** — Cluster is red, has too many relocating shards, or circuit breakers are tripped. Wait for the cluster to stabilize, or enable the override in Settings
- **"sustained memory pressure" after waiting** — Circuit breakers stayed above 80% for the full wait timeout. Increase `--breaker-wait`, reduce batch size, or wait for the cluster to settle manually
- **Recovery/reindex timeouts** — Increase the timeout in Settings, or ensure the working tier has adequate resources. Large indices (>50GB) automatically get longer timeouts via size tier profiles
- **Merge dry run shows "BLOCKED — Circuit breaker..."** — Enable "Ignore circuit breakers" in Settings to override. The tiebreaker node often has a small heap that trips the 80% threshold
- **Mount name collision on retry** — Handled automatically. `remount()` checks for and deletes stale mounts before mounting
- **Frozen stats not available during verification** — `_wait_for_doc_count()` polls with a 60s timeout for frozen index stats to become available after mount

## License

MIT
