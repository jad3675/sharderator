# Sharderator — Technical Design Document

Date: April 2026
Version: 0.5.0

---

## 1. Background

Elastic Cloud's frozen tier stores data as partially mounted searchable snapshots backed by object storage (S3, GCS, Azure Blob). Each frozen data node has a hard shard limit (`cluster.max_shards_per_node.frozen` — 3,000 default, 1,000 on trial clusters). In managed environments with hundreds of ILM-driven indices, this budget fills up fast — either because indices arrive with too many primary shards, or because daily index patterns create one shard per day per metric type.

Frozen indices are immutable. You cannot shrink, reindex, or forcemerge them in place. The only way to restructure them is to restore a full copy from the backing snapshot to a writable tier, mutate it, re-snapshot, and re-mount.

Sharderator automates this workflow with a PyQt6 GUI, a headless CLI, two operational modes (shrink and merge), and safety hardening for production clusters.

## 2. Prior Art: Elastic PII Tool

The design draws heavily from Elastic's [PII redaction tool](https://github.com/elastic/pii-tool) (`elastic/pii-tool`), which solves a related problem: mutating document content in frozen-tier indices.

**Patterns borrowed:**
- Restore from snapshot to writable tier using `restore_settings` with `_tier_preference` override to route to warm/hot nodes
- Job tracking in a dedicated Elasticsearch index (`redactions-tracker` → `sharderator-tracker`) for crash recovery and resume
- Force-merge after mutation (`max_num_segments: 1`) before re-snapshotting
- Handling `partial-*` prefix conventions for frozen indices

**Where Sharderator diverges:**
- The PII tool modifies documents in-place via `update_by_query`. Sharderator never touches document content — it restructures shard layout via the Shrink API or multi-source reindex.
- The PII tool is CLI-driven with YAML job configuration. Sharderator replaces this with a GUI that auto-discovers frozen indices, proposes merge candidates, and provides interactive selection with real-time progress.
- The PII tool processes one index per job. Sharderator's merge mode processes N indices as a group, requiring batched restores, union mapping computation, and atomic data stream swaps.
- Sharderator's dry-run exercises real pre-flight safety checks against the live cluster. The PII tool's `--dry-run` skips mutations only.

## 3. Architecture

```
+---------------------------------------------------+
|                   PyQt6 GUI                        |
|  +-------------+ +-----------+ +----------------+ |
|  | Shrink Mode | | Merge Mode| | Job Tracker    | |
|  | Tab         | | Tab       | | Tab            | |
|  +------+------+ +-----+-----+ +-------+--------+ |
|         |              |               |           |
|  +------v--------------v---------------v--------+  |
|  |            Main Window                       |  |
|  |  (worker threads, signal routing, log)       |  |
|  +-------------------+--------------------------+  |
|                      |                             |
|  +-------------------v--------------------------+  |
|  |  Orchestrator / MergeOrchestrator            |  |
|  |  (state machine, preflight, tier profiles)   |  |
|  +-------------------+--------------------------+  |
|                      |                             |
|  +-------------------v--------------------------+  |
|  |         Engine Modules                       |  |
|  |  preflight | sizing | analyzer | restorer    |  |
|  |  shrinker | merger | snapshotter | mounter   |  |
|  |  swapper | verifier | cleaner | task_waiter  |  |
|  +-------------------+--------------------------+  |
|                      |                             |
|  +-------------------v--------------------------+  |
|  |       Client Layer                           |  |
|  |  connection | queries | tracker              |  |
|  |  (elasticsearch-py 8.x)                      |  |
|  +----------------------------------------------+  |
+---------------------------------------------------+
```

### Threading Model

The main thread runs the Qt event loop and handles all GUI updates. Pipeline execution runs in a `QThread` (`ShrinkWorkerThread` or `MergeWorkerThread`). Communication between the worker and GUI uses the `PipelineEvents` protocol — the GUI uses `QtPipelineEvents` which emits Qt signals; the CLI uses `CliEvents` which prints to stdout.

Cancellation uses `threading.Event` (thread-safe, future-proof for free-threaded Python 3.13+).

### Event Decoupling

The engine modules never import PyQt6. `Orchestrator` and `MergeOrchestrator` accept a `PipelineEvents` protocol object:

```python
class PipelineEvents(Protocol):
    def on_progress(self, index_name: str, pct: float) -> None: ...
    def on_state_changed(self, index_name: str, state: str) -> None: ...
    def on_log(self, message: str) -> None: ...
    def on_completed(self, index_name: str) -> None: ...
    def on_failed(self, index_name: str, error: str) -> None: ...
```

This means `pip install sharderator` (without `[gui]`) works on headless servers.

## 4. State Machine

Both pipelines share a unified `JobState` enum with valid transitions enforced at runtime. The shrink pipeline uses the SHRINKING/AWAITING_SHRINK/AWAITING_REINDEX path; the merge pipeline uses the MERGING/AWAITING_MERGE path. Both converge at FORCE_MERGING.

```
PENDING
  |
  v
ANALYZING ---- gather metadata, validate snapshot source, capture doc count
  |
  v
RESTORING ---- restore from snapshot to warm/hot tier (disk gate, tier-adaptive timeout)
  |
  v
AWAITING_RECOVERY ---- poll until all restored indices are green
  |
  +---> SHRINKING (shrink mode) ---- shrink API or async reindex fallback
  |       |
  |       +---> AWAITING_SHRINK ---- wait for shrunk index to be green
  |       |
  |       +---> AWAITING_REINDEX ---- poll tasks API until reindex completes
  |
  +---> MERGING (merge mode) ---- multi-source async reindex
  |       |
  |       v
  |     AWAITING_MERGE ---- poll tasks API until reindex completes
  |
  v
FORCE_MERGING ---- merge to 1 segment/shard (with duplicate detection)
  |
  v
SNAPSHOTTING ---- create new snapshot
  |
  v
AWAITING_SNAPSHOT ---- poll until snapshot is SUCCESS
  |
  v
REMOUNTING ---- mount as shared_cache (frozen) searchable snapshot
  |
  v
VERIFYING ---- compare doc count, shard count, mapping, health
  |
  v
SWAPPING ---- atomic data stream backing index swap (AFTER verification)
  |
  v
CLEANING_UP ---- delete old mount, intermediates, optionally old snapshot
  |
  v
COMPLETED
```

Any state can transition to FAILED. Failed jobs are persisted to the tracking index. On retry, a FAILED job is treated as a fresh start (the stale record is deleted).

`JobRecord.to_dict()` uses `dataclasses.asdict()` so new fields are automatically included in serialization — no manual field list to maintain.

## 5. Shrink Mode Pipeline Detail

### 5.1 Analyzing

Reads `index.store.snapshot.*` settings to identify the source snapshot and repository. Retrieves mappings, detects custom routing (`_routing.required`), checks data stream membership (cached via `get_data_stream_membership` — one query for all indices in a group). Calculates target shard count (1 unless doc count exceeds the Lucene 2.1B limit). Stores the authoritative doc count on the job record for later verification.

After analysis, classifies the index into a size tier (SMALL/MEDIUM/LARGE/HUGE) and pulls the `TierProfile` to override recovery timeout and reindex throttle for this specific job.

### 5.2 Restoring

Hard disk space gate: calculates post-restore free percentage on warm/hot/content nodes only (not all nodes — frozen nodes with terabytes free can't be used for restores). If free space would drop below the safety margin (default 30%), the operation is refused with `InsufficientDiskError`.

Restores from the backing snapshot with `_tier_preference` override to route to the working tier, zero replicas, and a `sharderator-restore-` prefix to avoid name collisions. Detects and cleans up orphaned intermediate indices from previous failed runs before restoring.

### 5.3 Shrinking

Prefers the Shrink API (hard-link based, dramatically faster than reindex). Selects the warm/hot node with the most available disk, relocates all primaries there, waits for colocation confirmation by polling `_cat/shards` (not just green health — green doesn't guarantee all primaries are on the target node). Falls back to async reindex if the target shard count isn't a factor of the source.

Reindex runs asynchronously via the tasks API with configurable `requests_per_second` throttle (tier profile may reduce this further). Uses a 30-minute scroll keepalive (`source.scroll: 30m`) to prevent scroll expiry on object-storage-backed shards under cluster pressure. On transient failures (unavailable shards, node disconnects, scroll expiry), retries up to 3 times with `conflicts:proceed` to skip already-indexed docs. Each retry waits for the target index to go green, then fires a fresh reindex with a new scroll context.

### 5.4 Force Merge and Snapshot

Checks the tasks API for already-running force merges before starting (prevents duplicate merges after HTTP timeouts). On timeout, polls for completion instead of starting a second merge.

Creates a one-off snapshot with `sharderator-{original_name}-{timestamp}` naming. Polls `_snapshot/_status` until SUCCESS.

### 5.5 Remounting and Verification

Mounts the new snapshot as `shared_cache` (frozen) with a `{original_frozen_name}-sharderated` naming convention — e.g., `partial-.ds-test-oversharded-10-metrics-2026.04.15-000001-sharderated`. This preserves the `partial-.ds-` prefix that Kibana, ILM, and Elastic Cloud tooling expect for data stream grouping, while the `-sharderated` suffix is greppable across the cluster. Double-suffixing is prevented via `.removesuffix("-sharderated")`. Stale mount cleanup checks both the new suffix-based name and the legacy `partial-sharderator-` prefix for transition safety. Strips ILM (`index.lifecycle.indexing_complete: true`) to prevent re-processing.

Verification uses `_wait_for_doc_count()` which polls with a 60-second timeout for frozen index stats to become available (frozen indices are partially mounted searchable snapshots; their stats can take several seconds to populate after mount). Compares the stored expected doc count against the new mount's actual count, checks shard count matches target, confirms green health, and validates mapping field equivalence.

### 5.6 Swapping

Data stream backing index swap happens in the SWAPPING state, **after** verification passes. Uses add-before-remove ordering — if the API call fails partway, the data stream has both old and new indices (queries deduplicate) rather than missing a time range. Retries up to 3 times with idempotency checks.

### 5.7 Cleanup

Deletes the old frozen mount, intermediate restored index, intermediate shrunk index. Optionally deletes the old snapshot, but only after checking that no other indices are mounted from it — including cold-tier `restored-*` mounts, manually mounted bare indices, and `-sharderated` suffixed mounts (reconstructed by stripping `sharderator-shrunk-`/`sharderator-restore-` working prefixes from snapshot index names).

## 6. Merge Mode Pipeline Detail

### 6.1 Index Grouping

`merge_analyzer.py` parses frozen index names using a regex that handles common Elastic naming conventions:

```
partial-.ds-metrics-system.cpu-default-2026.02.14-000804
  ^prefix   ^base_pattern                ^date    ^generation
```

Groups by `(base_pattern, time_bucket)` where the bucket is monthly, quarterly, or yearly. Groups with fewer than 2 indices are excluded. The `INDEX_PATTERN` regex includes an optional `(?:-sharderated)?` suffix so previously sharderated indices can be included in future merge proposals — the suffix is stripped during matching so it doesn't pollute base name grouping. `_parse_merged_name()` also strips `-sharderated` before parsing for merge job resume compatibility.

### 6.2 Lazy Validation

When the operator checks a merge group in the tree, a validation function fires (once per group, cached). It checks snapshot metadata accessibility for all source indices and samples mapping drift between the first and last index. It also calls `union_mappings` on the sampled mappings to detect field type conflicts (e.g., `keyword` vs `text`). Non-viable groups are greyed out and uncheckable. Groups with mapping drift or type conflicts get a yellow warning but remain selectable — the operator decides.

### 6.3 Dynamic Batched Restore

All source indices in a merge group need to be restored to the working tier. Instead of a fixed batch count, `compute_dynamic_batch` sizes batches by available disk budget — three 500MB indices batch together; one 200GB index gets its own batch. The fixed `restore_batch_size` cap is still respected as a maximum.

A cumulative disk space gate runs first, accounting for 2x the total source size (sources + merged target). Disk is rechecked before each subsequent batch (at a lower 15% threshold since we're already committed) and again before starting the reindex.

### 6.4 Multi-Source Reindex

Creates the target index with union mappings and merged analysis settings (normalizers, analyzers, tokenizers, filters from all source indices). Uses `body=` with correct nesting for the analysis block — the dotted `index.analysis` notation doesn't work for nested objects in the ES create index API.

`union_mappings` now returns `(merged_mapping, conflicts)`. Any field type conflicts (e.g., `status` is `keyword` in some indices and `text` in others) are logged at WARNING level before the index is created. The first-seen-wins resolution strategy is unchanged — the conflicts list provides transparency without blocking the merge.

Reindex runs asynchronously via the tasks API with tier-adaptive `requests_per_second` throttle and a 30-minute scroll keepalive (`source.scroll: 30m`). On transient failures, retries up to 3 times with `conflicts:proceed` to skip already-indexed docs.

### 6.5 Atomic Multi-DS Swap

After verification, all source backing indices across all data streams are removed and the merged index is added in a **single** `_data_stream/_modify` call. This is the critical atomicity property — a crash between two separate calls would leave some data streams pointing at the new index and others still pointing at old sources. The single-call approach is all-or-nothing. Retries re-check membership for idempotency.

## 7. Safety Architecture

### 7.1 Pre-Flight Checks

Run at the start of every fresh job (not on resume) and during dry runs:
- Cluster health: red = block, yellow = configurable, >10 relocating/initializing shards = block
- Circuit breakers: any data-node breaker >80% triggers adaptive wait (see 7.4). Non-data nodes (master-only, tiebreaker, ML) are excluded — their memory pressure is irrelevant to Sharderator's workload.
- Disk space: calculates post-operation free percentage on working tier nodes only

### 7.2 Mid-Operation Monitoring

Between every pipeline stage, `_check_cancel` also calls `check_cluster_health_mid_operation`. This checks (data nodes only — tiebreaker/master-only/ML nodes are skipped via `_is_data_node()`):
- Red cluster status (pause up to 15 minutes, then abort)
- Circuit breaker ratios at 75% threshold (5 points below the hard gate)
- Thread pool queue depths (write, search, snapshot) — warns if queue > 100
- Pending cluster tasks — warns if > 50
- JVM heap pressure — warns if any data node > 85%
- Relocating/initializing shard counts

On pressure detection, waits with linear backoff (30s, 60s, 90s... up to 300s cap). Logs and continues rather than failing — the operator chose to run.

### 7.3 Verification Before Swap

The SWAPPING state (data stream modification) was deliberately separated from REMOUNTING and placed after VERIFYING. This ensures that the irreversible data stream mutation only happens after we've confirmed the new mount is healthy. If verification fails, the data stream still points at the original index.

### 7.4 Circuit Breaker Adaptive Backpressure

Three layers of circuit breaker awareness prevent cascading failures during batch operations:

**Layer A — Wait-capable preflight gate (`check_circuit_breakers`):**
When a circuit breaker exceeds 80%, instead of failing immediately, the function enters a linear backoff loop (30s, 60s, 90s… capped at 300s). If pressure clears within the configurable timeout (`circuit_breaker_wait_minutes`, default 10), the operation proceeds. If not, it raises `ClusterNotHealthyError`. Setting the timeout to 0 restores old fail-fast behavior.

**Layer B — Mid-operation breaker awareness (`_check_cluster_pressure`):**
The mid-operation pressure check now includes circuit breaker ratios via `_get_hot_breakers(threshold=0.75)`. The 75% threshold is intentionally 5 points below the 80% hard gate — this means mid-operation pauses kick in before the next index's preflight would reject, giving the cluster headroom to settle.

**Layer C — Inter-index backpressure (`wait_for_cluster_ready`):**
Called between indices/groups in all four batch loops (CLI shrink, CLI merge, GUI ShrinkWorkerThread, GUI MergeWorkerThread). Combines health + breaker + pressure checks with adaptive waiting. If all checks pass immediately, returns with zero delay — no artificial overhead on healthy clusters. Configurable via `batch_backpressure_timeout_minutes` (default 15, 0 = disabled).

The shared helper `_get_hot_breakers(client, threshold)` scans data nodes for circuit breakers above the given threshold and returns `(breaker_name, node_name, ratio)` tuples. Non-data nodes (master-only, tiebreaker, ML, ingest) are skipped via `_is_data_node()` which checks for any data-tier role (`data`, `data_hot`, `data_warm`, `data_cold`, `data_frozen`, `data_content`). The `nodes.stats` calls include the `os` metric to get the `roles` field. Both the preflight gate and the pressure check use this helper, eliminating code duplication.

**Behavior before backpressure:**
```
Index 1: COMPLETED
Index 2: COMPLETED
Index 3: COMPLETED  (cluster now at 82% breaker)
Index 4: FAILED     (instant — 0s wait)
Index 5: FAILED     (instant)
...
```

**Behavior after backpressure (defaults):**
```
Index 1: COMPLETED
Index 2: COMPLETED
Index 3: COMPLETED  (cluster now at 82% breaker)
  [batch loop] Pressure detected: parent breaker at 82% — waiting...
  [batch loop] +30s: still at 78% — waiting...
  [batch loop] +90s: pressure resolved
Index 4: COMPLETED  (breaker had settled to 65%)
...
```

### 7.5 Job Persistence

Every state transition is persisted to the `sharderator-tracker` Elasticsearch index via `save_job`. On restart, `load_job` retrieves the last known state. FAILED jobs are treated as fresh starts on retry (the stale record is deleted). Merge jobs serialize enriched index metadata into the job record so resume doesn't require re-querying source indices that may have been partially cleaned up.

`JobRecord.to_dict()` uses `dataclasses.asdict()` — new fields added to the dataclass are automatically included in serialization. `from_dict()` filters to valid field names so unknown keys from older/newer tracker docs are dropped cleanly.

### 7.6 Size-Tier Adaptive Timeouts

After ANALYZING, both orchestrators classify the index/group into a size tier and pull a `TierProfile`:

| Tier | Size | Recovery Timeout | Reindex Throttle |
|------|------|-----------------|-----------------|
| SMALL | < 1 GB | 10 min | Unlimited |
| MEDIUM | 1–50 GB | 30 min | 10,000 docs/sec |
| LARGE | 50–500 GB | 2 hours | 5,000 docs/sec |
| HUGE | > 500 GB | 6 hours | 2,000 docs/sec |

The orchestrator uses `max(cfg.recovery_timeout_minutes, tier_profile.recovery_timeout_minutes)` so the config value serves as a minimum. The tier throttle overrides the config throttle if more conservative.

## 8. Defrag Visualization Widget

A per-shard cell grid inspired by the Windows 95/98/XP defrag utility. Each shard in every index gets its own 10x10 pixel square cell in a continuous grid — an 18-index merge batch with 7-11 shards each becomes ~160 individual cells filling a proper chunky grid.

The data model uses `ShardCell` (one per shard) grouped into `IndexGroup` (one per index). State transitions use a staggered sweep animation (30ms per cell) that lights up each shard sequentially, creating the classic "scan head moving across the disk" effect.

On completion, an index's cells collapse: the first N cells (where N = target shard count, usually 1) turn green, and the remaining cells become "FREED" — painted as dark near-black, representing reclaimed shard budget.

States in the `AWAITING_*` family pulse with sinusoidal opacity (160-255 alpha, 50ms tick). The active index gets a white highlight overlay and border on all its non-collapsed cells. A scan-line highlight marks the next cell to be swept.

A legend bar below the status text shows colored squares with labels for the key states.

Tooltips show per-cell detail: index name, shard number (e.g., "Shard: 3/7"), state, target, and progress.

The CLI equivalent uses ANSI-colored block characters that wrap to terminal width and update in-place via cursor-up + clear.

### Merge Name Mapping

During merge operations, the orchestrator emits state changes keyed by the merged output name (e.g., `partial-.ds-...-2026.04-merged`), but the defrag grid is keyed by source index names. A `_merge_name_map: dict[str, list[str]]` bridges this gap — `set_merge_groups()` registers the mapping from merged output names to source index names. When `update_state()` receives a merged name, it fans out the state change to all source index groups. This applies to state updates, progress updates, completion collapse, failure marking, and the active-index highlight in `paintEvent`. Both the GUI `DefragWidget` and the CLI `TerminalDefrag` implement this mapping.

## 9. CLI Mode

`sharderator-cli` provides headless operation with no PyQt6 dependency. Subcommands: `list`, `status`, `analyze`, `shrink`, `merge`, `report`. Full argument parsing for connection and operation settings, environment variable support (`ES_API_KEY`, `ES_PASSWORD`), and SIGINT/SIGTERM signal handling for graceful cancellation.

The CLI reuses the same `Orchestrator` and `MergeOrchestrator` classes as the GUI, passing a `CliEvents` implementation instead of `QtPipelineEvents`. No `QApplication` instance is needed.

Additional CLI features:
- `--order smallest-first|largest-first|as-is` on both shrink and merge
- `--priority "pattern1,pattern2"` on merge to move matching groups to the front
- `--output table|json` for dry-run format selection
- `--breaker-wait` and `--backpressure-wait` for circuit breaker backpressure tuning
- Pre-run sizing reports during `--dry-run`
- Export: `--csv` and `--json` on `list`, JSON output on `report`

## 10. Thread Safety

Cancellation flags in both orchestrators use `threading.Event` instead of bare booleans. This is explicitly thread-safe and future-proof for PEP 703 free-threaded Python (3.13+). The `cancel()` method is called from the GUI thread; `_check_cancel()` reads from the worker thread.

## 11. Atomic Data Stream Swaps

The `swapper.py` module uses add-before-remove ordering in `_data_stream/_modify` calls. If a partial failure occurs, the data stream temporarily has both old and new backing indices (queries deduplicate) rather than missing a time range.

For merge mode, `_atomic_swap_multi_ds` collapses all add/remove actions across all data streams into a single `modify_data_stream` request. Either all succeed or none do — no partial-failure window between data streams. The function retries up to 3 times with idempotency checks — it re-fetches current membership before each attempt so partially applied changes from a previous failed attempt are handled correctly.

## 12. Unit Test Suite

143 unit tests across 9 files, running in ~1s with no cluster dependency:

- `test_job.py` — State machine transitions (every valid path, every invalid path, terminal states, error storage, serialization round-trips using `dataclasses.asdict`)
- `test_index_info.py` — `needs_reindex`, `shard_reduction`, `store_size_mb` properties
- `test_merge_analyzer.py` — Index grouping at monthly/quarterly granularity, single-index exclusion, shard reduction calculation, mapping union with field drift and type conflicts. `TestMergeGroupFromJob` covers `from_job_record()` round-trips for both naming formats, fallback without enriched metadata, yearly buckets, and empty source indices. Analysis settings conflict detection and union. `TestSharderatedSuffix` covers `INDEX_PATTERN` matching for `-sharderated` suffixed indices, `_parse_merged_name` with sharderated suffix, and `propose_merges` grouping sharderated indices alongside normal ones.
- `test_preflight.py` — Cluster health gates (red/yellow/green, relocating, initializing). Circuit breaker backpressure: `_get_hot_breakers` scanning (empty, single hot, multi-node, zero limit, custom threshold), `check_circuit_breakers` wait-then-clear and wait-then-fail with mocked time, `check_circuit_breakers_instant` no-sleep guarantee, `_check_cluster_pressure` breaker integration at 75% threshold, `wait_for_cluster_ready` zero-delay on healthy clusters and wait-then-clear with mocked sleep. Node filtering: `_is_data_node` parametrized across all 6 data roles and 6 non-data roles, mixed roles, empty/missing roles. Integration tests verifying tiebreaker nodes are skipped by circuit breaker checks, pressure checks, and `_get_hot_breakers`.
- `test_defrag_widget.py` — Per-shard cell creation, group mapping, state updates with sweep queuing, completion collapse (target cells green, excess cells freed), failed state handling, progress tracking, paint rendering, reset, and edge cases. Merge name mapping: fan-out state updates, completed collapse across all source groups, failed marking, progress fan-out, unknown merged name handling, map clearing on set_indices and reset.
- `test_sizing.py` — Size tier classification, tier profile lookup, sort ordering (smallest-first/largest-first/as-is), priority queuing, dynamic batch sizing by disk budget
- `test_snapshotter.py` — Force merge deduplication with exact bracket matching
- `test_task_waiter.py` — Transient failure classification: parametrized across all 10 entries in `TRANSIENT_FAILURE_TYPES` (including `search_context_missing_exception` and `search_phase_execution_exception`), nested `caused_by` transient detection, permanent type rejection, empty cause handling. `wait_for_task` integration: all-transient failures raise `TransientReindexError`, mixed failures raise `PermanentReindexError`, clean completion returns normally.

## 13. End-to-End Test Suite

A separate `sharderator-test-suite/` package provides end-to-end validation against a real Elastic Cloud trial deployment. It is intentionally kept in its own directory and package namespace (`sharderator_test`) to avoid co-mingling with the main Sharderator codebase.

### Architecture

The test suite is a Click CLI (`sharderator-test`) with subcommands for each phase of the test lifecycle:

| Command | Purpose |
|---------|---------|
| `setup` | Create ILM policies, index templates, speed up ILM poll interval |
| `ingest-oversharded` | Generate Scenario A data (over-sharded data streams) |
| `ingest-daily` | Generate Scenario B data (daily index explosion) |
| `status` | Show frozen shard budget and ILM progress |
| `validate` | Run validation phases against the cluster |
| `cleanup` | Remove all test artifacts |
| `create-api-key` | Create a test API key with full cluster access |

### Test Scenarios

**Scenario A — Over-sharded indices:** Five data streams with 10/20/50 primary shards each, pushed to frozen via ILM with `max_age: 1m` rollover. After rollover, the old backing indices (with their high shard counts) move to frozen within ~3 minutes. This exercises Sharderator's shrink mode.

**Scenario B — Daily index explosion:** N days x 40 metric types, each a single-shard index pushed to frozen via ILM. This exercises Sharderator's merge mode (monthly granularity consolidation).

### Validation Phases

| Phase | What it tests |
|-------|--------------|
| `read-only` | `sharderator-cli list`, `list --json`, `status` |
| `dry-run` | Shrink and merge dry runs in both table and JSON format |
| `shrink` | Single index shrink + batch shrink with doc count verification |
| `merge` | Single group merge + full batch merge |
| `crash-recovery` | Start merge, kill after 15s, check tracker, resume |
| `export` | Frozen index CSV, job history JSON, merge plan JSON |

The `validate` command shells out to `sharderator-cli` as a subprocess, capturing stdout/stderr and parsing JSON output. This tests the full CLI path end-to-end, including argument parsing, connection handling, and output formatting.

### Platform Considerations

- Windows `cp1252` console encoding: subprocess calls inject `PYTHONIOENCODING=utf-8` to handle the block characters in the terminal defrag visualization
- Elastic Cloud trial clusters have a 1,000 frozen shard limit (not the 3,000 default)
- Tiebreaker nodes routinely trip the 80% circuit breaker threshold — validation phases pass `--ignore-circuit-breakers` to `sharderator-cli`
- `cat.indices()` in elasticsearch-py 8.x/9.x doesn't accept `ignore_unavailable` — uses `expand_wildcards="all"` with try/except instead
- `max_shards_per_node` in cluster settings can be a plain string or a dict with a `frozen` key depending on ES version/config
- Data streams require `_op_type: "create"` in bulk actions
- Dotted field names in `_source` (e.g., `host.name`) don't match nested object mappings — ingest uses actual nested dicts

## 14. Elasticsearch API Surface

| Operation | API | Notes |
|-----------|-----|-------|
| Cluster health | `GET /_cluster/health` | Pre-flight + mid-operation |
| Circuit breakers | `GET /_nodes/stats/breaker` | Pre-flight gate + mid-operation pressure |
| Thread pool stats | `GET /_nodes/stats/thread_pool` | Mid-operation pressure check |
| JVM heap stats | `GET /_nodes/stats/jvm` | Mid-operation pressure check |
| Pending tasks | `GET /_cluster/pending_tasks` | Mid-operation pressure check |
| Node disk info | `GET /_cat/nodes?h=name,disk.avail,disk.total,node.role&bytes=b` | Disk gate |
| List frozen indices | `GET /_cat/indices?h=index,pri,docs.count,store.size,dataset.size&bytes=b` | Uses `dataset.size` for accurate frozen sizes |
| Index settings | `GET /<index>/_settings` | Snapshot source discovery |
| Index mapping | `GET /<index>/_mapping` | Mapping preservation + drift detection |
| Data streams | `GET /_data_stream/*` | Membership detection (cached) |
| Restore snapshot | `POST /_snapshot/<repo>/<snap>/_restore` | With tier preference override |
| Shard allocation | `GET /_cat/shards/<index>?h=node,prirep` | Colocation verification |
| Shrink index | `POST /<index>/_shrink/<target>` | Hard-link based |
| Reindex | `POST /_reindex` | Async with tasks API, throttled, retry on transient |
| Tasks | `GET /_tasks/<id>` | Reindex + force merge monitoring |
| Force merge | `POST /<index>/_forcemerge?max_num_segments=1` | With duplicate detection |
| Create snapshot | `PUT /_snapshot/<repo>/<snap>` | One-off, `sharderator-` prefix |
| Snapshot status | `GET /_snapshot/<repo>/<snap>/_status` | Polling |
| Mount searchable snapshot | `POST /_snapshot/<repo>/<snap>/_mount?storage=shared_cache` | Frozen tier |
| Modify data stream | `POST /_data_stream/_modify` | Atomic backing index swap |
| Index stats | `GET /<index>/_stats` | Doc count verification |
| Delete index | `DELETE /<index>` | Cleanup |
| Delete snapshot | `DELETE /_snapshot/<repo>/<snap>` | With reference checking |
| Cluster settings | `GET /_cluster/settings?include_defaults=true` | Frozen shard limit |
| Tracker index | `sharderator-tracker` | Job persistence (index/get/delete/search) |

## 15. Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| elasticsearch | >= 8.12 | Elasticsearch Python client |
| pyyaml | >= 6.0 | Config file persistence |
| keyring | >= 25.0 | Secure credential storage |
| structlog | >= 24.0 | Structured logging |
| PyQt6 | >= 6.6 | GUI framework (optional, `[gui]` extra) |

Test suite additional dependencies:
| Package | Version | Purpose |
|---------|---------|---------|
| click | >= 8.1 | CLI framework for test harness |
| rich | >= 13.0 | Console formatting |

Python >= 3.10 required.

## 16. Scale Improvements

### Size Tiers

Indices are classified into four tiers based on store size. Each tier has a `TierProfile` with recovery timeout and reindex throttle values. Both orchestrators call `classify_index()` / `classify_merge_group()` after ANALYZING and use `max(cfg.timeout, tier_profile.timeout)` so the config value serves as a minimum. The tier throttle overrides the config throttle if more conservative.

### Dynamic Batching

`compute_dynamic_batch()` in `sizing.py` splits restore plans by disk budget instead of fixed count. Three 500MB indices batch together; one 200GB index gets its own batch. The fixed `restore_batch_size` cap is still respected as a maximum.

### Pre-Run Sizing Reports

`generate_sizing_report()` produces a formatted report showing total source size, largest index, working tier capacity, peak disk usage, headroom, and projected shard budget. Shown during CLI dry runs and GUI dry runs.

### Continuous Disk Monitoring

`_check_merge_disk_space_incremental()` rechecks disk before each restore batch (after the first) and before starting the reindex. Uses a lower 15% threshold since the initial 30% gate already passed.

### Deeper Health Checks

`check_cluster_health_mid_operation()` checks thread pool queue depths, pending cluster tasks, JVM heap pressure, circuit breaker ratios, and relocating/initializing shards on every stage transition. Uses linear backoff when pressure is detected.

### Size-Based Ordering

`sort_indices()` and `sort_merge_groups()` in `sizing.py` support `smallest-first`, `largest-first`, and `as-is` ordering. Default is smallest-first (fast shard reclamation, canary behavior).

### Priority Queuing

`prioritize_groups()` in `sizing.py` moves groups matching priority patterns to the front of the queue. Available via `--priority` CLI flag.

## 17. Export Feature

Five export types for change ticket documentation and operational record-keeping.

**Frozen tier analysis** — The Frozen Analyze tab has an Export button that saves the full health report as HTML or JSON. `FrozenAnalysis.format_html()` generates a self-contained HTML document with inline CSS, color-coded budget bar, flexbox summary cards, over-sharded and mergeable pattern tables, and a recommendation box. Print-friendly via `@media print` styles — Ctrl+P in any browser produces a clean PDF. JSON output uses `FrozenAnalysis.to_dict()` with cluster name and timestamp. CLI: `sharderator-cli analyze --json`.

**Post-operation change report** — After every batch (shrink or merge) completes, the completion summary dialog includes a "Save Report" button. The report captures before/after shard counts (pre-operation count stored in `_batch_shards_before` at batch start), shards reclaimed, completed items list, failed items with error details, and cluster metadata. Available as HTML (same professional styling as the analysis export) or JSON. Designed for change control ticket attachments.

**Frozen index list** — `SizingReport.to_dict()` serializes the report using `dataclasses.asdict()`. The Shrink Mode tab has an Export button that saves the frozen index table as CSV or JSON via a file save dialog. CLI: `sharderator-cli list --csv` or `--json`.

**Dry-run sizing report** — The Merge Mode tab has an "Export Dry Run" button that generates a sizing report, runs preflight checks, and saves the combined result as JSON. The JSON includes per-group detail (source indices, shard reduction, mapping conflicts) and preflight results. CLI: `sharderator-cli shrink --dry-run --output json` or `sharderator-cli merge --dry-run --output json`.

**Job history** — The Job Tracker tab has an Export button that saves all tracked jobs as JSON with cluster name and timestamp. Right-click any row → "Export Job" saves a single job record. CLI: `sharderator-cli report` (all jobs), `sharderator-cli report --index <name>` (single job), `sharderator-cli report --state COMPLETED|FAILED|all` (filtered).

## 18. Job Tracker Viewer

The third tab in the main window queries the `sharderator-tracker` Elasticsearch index and displays all tracked jobs in a table: index name, job type (SHRINK/MERGE), current state, error message, timestamps, and expected doc count. Rows are color-coded by state — red for FAILED, yellow for AWAITING_* states.

A right-click context menu provides Resume (re-queues the job from its last committed state), Delete (removes the tracker document), Copy Error (clipboard), and View History (popup dialog showing the full state transition timeline with timestamps).

**Resume for SHRINK jobs:** Creates a minimal `IndexInfo` from the job record and passes it to `Orchestrator.run()`. The orchestrator calls `load_job()` internally and resumes from the last committed state.

**Resume for MERGE jobs:** Calls `MergeGroup.from_job_record(job)` to reconstruct the `MergeGroup` from the persisted `enriched_metadata` field. If `enriched_metadata` is populated, full `IndexInfo` objects are rebuilt. If it's empty (very old job records), falls back to minimal `IndexInfo` from `source_frozen_indices` names. The reconstructed group is passed to `MergeOrchestrator.run()`, which calls `load_job()` and resumes from the last committed state.

`_parse_merged_name()` in `merge_analyzer.py` extracts `base_pattern` and `time_bucket` from the merged index name. Handles both the final frozen mount format (`partial-.ds-{base}-{bucket}-merged`) and the intermediate working name format (`sharderator-merged-{base}-{bucket}`).

## 19. Frozen Tier Analysis

### Engine: `frozen_analyzer.py`

`analyze_frozen_tier(indices, frozen_limit, min_shards_for_shrink)` takes the list of frozen `IndexInfo` objects and produces a `FrozenAnalysis` dataclass containing:

- **Budget metrics:** total shards, limit, usage percentage, status (OK/WARNING/CRITICAL/OVER LIMIT)
- **Over-sharded indices:** sorted by shard count descending, with per-index savings calculation
- **Mergeable patterns:** each `PatternSummary` contains the base pattern, index count, total shards, and projected shards after monthly/quarterly/yearly merge. Uses `propose_merges()` internally at each granularity to compute accurate group counts and savings.
- **Categorization counts:** already optimal (1 shard), already sharderated (`-sharderated` suffix), already merged (`-merged` suffix), unrecognized naming
- **Aggregate savings:** total shrink savings, total merge savings at each granularity

`FrozenAnalysis.format_text()` produces a human-readable report with a Unicode budget bar, tables, and recommendations. `FrozenAnalysis.to_dict()` produces JSON-serializable output for automation. `FrozenAnalysis.format_html()` generates a self-contained HTML document with inline CSS suitable for printing to PDF.

### GUI: `analyze_widget.py`

The "Frozen Analyze" tab is the first tab in the main window — the starting point after connecting. It contains:

- **BudgetBar** — a `QProgressBar` subclass with color-coded chunk styling (green/yellow/orange/red) and dark background. Uses 1000-step granularity for smooth rendering at low percentages.
- **Summary cards** — four `SummaryCard(QFrame)` widgets with `QFrame.Shape.Box` borders, each showing a title and detail text.
- **Over-sharded table** — sortable `QTableWidget` with numeric sorting via `setData(DisplayRole, int)`. Columns: Index Name, Shards, Target, Savings, Size (MB).
- **Mergeable patterns table** — sortable `QTableWidget`. Columns: Base Pattern, Indices, Shards, → Monthly, Savings, Size (MB).
- **Recommendation bar** — bottom-line summary with total reclaimable shards, breakdown, and projected budget.

Auto-populates on connect and on every refresh (F5). Clears on disconnect. Export button saves the report as HTML or JSON via file dialog.

### Batch Completion Dialog

When a shrink or merge batch finishes, a `QMessageBox` pops up showing: items completed/failed, shards reclaimed, updated budget. A "Save Report" button generates a post-operation change report (HTML or JSON) documenting before/after shard counts, completed items, and failed items with error details.

### CLI: `sharderator-cli analyze`

Text output includes a Unicode budget bar (`█░`), over-sharded table (top 15), mergeable patterns table (top 20), summary counts, and recommendations with projected budget. `--json` outputs the full `FrozenAnalysis.to_dict()`. `--min-shards` controls the over-sharded threshold (default 2).

### Shrink Guard

The shrink orchestrator includes a guard after `analyze()` that detects `shard_count <= target_shard_count` and skips the index immediately (marks COMPLETED, no restore/shrink/snapshot cycle). This prevents merged indices (1 shard) from entering the shrink pipeline. The CLI also pre-filters with `shard_count > target_shard_count` before the orchestrator loop.

## 20. Select All and Filter

Both Shrink Mode and Merge Mode tabs have a filter bar above their table/tree:

```
[ Filter indices...          ] [Select All] [Deselect All]
```

**Shrink Mode** uses `IndexFilterProxy(QSortFilterProxyModel)` between the view and the source model. The proxy's `filterAcceptsRow()` matches against column 1 (Index Name) using `fnmatch` for glob patterns (`*apache*`) or substring matching when no wildcards are present. `visible_source_rows()` returns source-model row indices for all currently visible rows, which `select_visible()` / `deselect_visible()` on the model use for bulk check/uncheck.

**Merge Mode** uses item visibility on the `QTreeWidget` — `apply_filter()` iterates top-level items (pattern groups) and calls `setHidden()` based on whether the pattern name matches. `select_visible()` / `deselect_visible()` iterate visible items and batch check state changes with `blockSignals(True)`, then run lazy validation afterward to avoid hammering the cluster with N individual validation calls.

**Selection persistence:** Checked items survive across refreshes and re-analysis. `get_checked_names()` / `restore_checked_names()` on the shrink model stash and restore by index name. `get_checked_keys()` / `restore_checked_keys()` on the merge tree stash and restore by `base_pattern|time_bucket` key.

**Ctrl+A** triggers Select All for the active tab (Shrink or Merge, not Job Tracker).

**Targeted bulk selection workflow:** Type `*apache*` → Select All → clear filter → type `*docker*` → Select All → both sets are now checked. Deselect All only affects visible (filtered) rows.

## 21. Scroll Context and Reindex Resilience

### Transient Failure Classification

`task_waiter.py` maintains a `TRANSIENT_FAILURE_TYPES` set used by `_is_transient_failure()` to classify reindex doc failures. Two scroll-related types were added:

- `search_context_missing_exception` — the scroll cursor expired between batch fetches. The data is still there; retrying creates a fresh scroll.
- `search_phase_execution_exception` — a wrapper that sometimes contains scroll failures as a nested cause.

When all doc failures in a completed reindex task are transient, `wait_for_task()` raises `TransientReindexError`. When any failure is non-transient, it raises `PermanentReindexError`. The retry loops in both merge and shrink paths catch `TransientReindexError` and retry with `conflicts:proceed`.

### Scroll Keepalive

Both `merge_reindex()` in `merger.py` and `_reindex_async()` in `shrinker.py` set `source.scroll: 30m` in the reindex body. The default 5-minute keepalive is too short for object-storage-backed frozen-tier shards under cluster pressure. The 30-minute keepalive is per-batch (each scroll fetch resets the timer), not total operation time.

### Shrink Reindex Retry

`wait_for_reindex()` in `shrinker.py` now includes a retry loop matching the merge path's pattern. On `TransientReindexError`: backs off with linear delay (30s × attempt), waits for the target index to go green, fires a new reindex with `conflicts:proceed` and a fresh 30m scroll, and updates `job.merge_task_id` with the new task ID. Up to 3 retries. `PermanentReindexError` propagates immediately.

## 22. Known Limitations

- Cross-index merge (combining indices from different base patterns) is not supported. Each merge group must share a base pattern.
- The tool assumes standard Elastic naming conventions (`partial-`, `.ds-`, generation numbers). Indices with non-standard names may not be detected by the merge analyzer.
- Minimum ES version 8.7 is required. Some features (like the Health Report API) work better on 8.12+.
- No multi-cluster support — one connection at a time.
- The CLI requires the same Python package as the GUI but not PyQt6. The `[gui]` extra adds PyQt6 for the GUI entry point.
- Cancellation during a backpressure wait (`time.sleep`) is not immediate — the current sleep interval must complete before the cancellation is detected at the next `_check_cancel`. Maximum unresponsive window is 300s (the sleep cap).
