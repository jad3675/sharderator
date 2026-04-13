# Sharderator — Technical Design Document

Date: April 2026
Version: 0.2.0

---

## 1. Background

Elastic Cloud's frozen tier stores data as partially mounted searchable snapshots backed by object storage (S3, GCS, Azure Blob). Each frozen data node has a hard shard limit of 3,000 (`cluster.max_shards_per_node.frozen`). In managed environments with hundreds of ILM-driven indices, this budget fills up fast — either because indices arrive with too many primary shards, or because daily index patterns create one shard per day per metric type.

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

Restores from the backing snapshot with `_tier_preference` override to route to the working tier, zero replicas, and a `sharderator-restore-` prefix to avoid name collisions.

### 5.3 Shrinking

Prefers the Shrink API (hard-link based, dramatically faster than reindex). Selects the warm/hot node with the most available disk, relocates all primaries there, waits for colocation confirmation by polling `_cat/shards` (not just green health — green doesn't guarantee all primaries are on the target node). Falls back to async reindex if the target shard count isn't a factor of the source.

Reindex runs asynchronously via the tasks API with configurable `requests_per_second` throttle (tier profile may reduce this further). On transient failures (unavailable shards, node disconnects), retries up to 3 times with `conflicts:proceed` to skip already-indexed docs.

### 5.4 Force Merge and Snapshot

Checks the tasks API for already-running force merges before starting (prevents duplicate merges after HTTP timeouts). On timeout, polls for completion instead of starting a second merge.

Creates a one-off snapshot with `sharderator-{original_name}-{timestamp}` naming. Polls `_snapshot/_status` until SUCCESS.

### 5.5 Remounting and Verification

Mounts the new snapshot as `shared_cache` (frozen) with a temporary name to avoid collision with the still-existing old mount. Strips ILM (`index.lifecycle.indexing_complete: true`) to prevent re-processing.

Verification compares the stored expected doc count against the new mount's actual count, checks shard count matches target, confirms green health, and validates mapping field equivalence.

### 5.6 Swapping

Data stream backing index swap happens in the SWAPPING state, **after** verification passes. Uses add-before-remove ordering — if the API call fails partway, the data stream has both old and new indices (queries deduplicate) rather than missing a time range. Retries up to 3 times with idempotency checks.

### 5.7 Cleanup

Deletes the old frozen mount, intermediate restored index, intermediate shrunk index. Optionally deletes the old snapshot, but only after checking that no other indices are mounted from it — including cold-tier `restored-*` mounts and manually mounted bare indices.

## 6. Merge Mode Pipeline Detail

### 6.1 Index Grouping

`merge_analyzer.py` parses frozen index names using a regex that handles common Elastic naming conventions:

```
partial-.ds-metrics-system.cpu-default-2026.02.14-000804
  ^prefix   ^base_pattern                ^date    ^generation
```

Groups by `(base_pattern, time_bucket)` where the bucket is monthly, quarterly, or yearly. Groups with fewer than 2 indices are excluded.

### 6.2 Lazy Validation

When the operator checks a merge group in the tree, a validation function fires (once per group, cached). It checks snapshot metadata accessibility for all source indices and samples mapping drift between the first and last index. It also calls `union_mappings` on the sampled mappings to detect field type conflicts (e.g., `keyword` vs `text`). Non-viable groups are greyed out and uncheckable. Groups with mapping drift or type conflicts get a yellow warning but remain selectable — the operator decides.

### 6.3 Dynamic Batched Restore

All source indices in a merge group need to be restored to the working tier. Instead of a fixed batch count, `compute_dynamic_batch` sizes batches by available disk budget — three 500MB indices batch together; one 200GB index gets its own batch. The fixed `restore_batch_size` cap is still respected as a maximum.

A cumulative disk space gate runs first, accounting for 2x the total source size (sources + merged target). Disk is rechecked before each subsequent batch (at a lower 15% threshold since we're already committed) and again before starting the reindex.

### 6.4 Multi-Source Reindex

Creates the target index with union mappings and merged analysis settings (normalizers, analyzers, tokenizers, filters from all source indices). Uses `body=` with correct nesting for the analysis block — the dotted `index.analysis` notation doesn't work for nested objects in the ES create index API.

`union_mappings` now returns `(merged_mapping, conflicts)`. Any field type conflicts (e.g., `status` is `keyword` in some indices and `text` in others) are logged at WARNING level before the index is created. The first-seen-wins resolution strategy is unchanged — the conflicts list provides transparency without blocking the merge.

Reindex runs asynchronously via the tasks API with tier-adaptive `requests_per_second` throttle. On transient failures, retries up to 3 times with `conflicts:proceed` to skip already-indexed docs.

### 6.5 Atomic Multi-DS Swap

After verification, all source backing indices across all data streams are removed and the merged index is added in a **single** `_data_stream/_modify` call. This is the critical atomicity property — a crash between two separate calls would leave some data streams pointing at the new index and others still pointing at old sources. The single-call approach is all-or-nothing. Retries re-check membership for idempotency.

## 7. Safety Architecture

### 7.1 Pre-Flight Checks

Run at the start of every fresh job (not on resume) and during dry runs:
- Cluster health: red = block, yellow = configurable, >10 relocating/initializing shards = block
- Circuit breakers: any breaker >80% = block (overridable via settings)
- Disk space: calculates post-operation free percentage on working tier nodes only

### 7.2 Mid-Operation Monitoring

Between every pipeline stage, `_check_cancel` also calls `check_cluster_health_mid_operation`. This now checks:
- Red cluster status (pause up to 15 minutes, then abort)
- Thread pool queue depths (write, search, snapshot) — warns if queue > 100
- Pending cluster tasks — warns if > 50
- JVM heap pressure — warns if any node > 85%
- Relocating/initializing shard counts

On pressure detection, waits with linear backoff (30s, 60s, 90s... up to 300s cap). Logs and continues rather than failing — the operator chose to run.

### 7.3 Verification Before Swap

The SWAPPING state (data stream modification) was deliberately separated from REMOUNTING and placed after VERIFYING. This ensures that the irreversible data stream mutation only happens after we've confirmed the new mount is healthy. If verification fails, the data stream still points at the original index.

### 7.4 Job Persistence

Every state transition is persisted to the `sharderator-tracker` Elasticsearch index via `save_job`. On restart, `load_job` retrieves the last known state. FAILED jobs are treated as fresh starts on retry (the stale record is deleted). Merge jobs serialize enriched index metadata into the job record so resume doesn't require re-querying source indices that may have been partially cleaned up.

`JobRecord.to_dict()` uses `dataclasses.asdict()` — new fields added to the dataclass are automatically included in serialization. `from_dict()` filters to valid field names so unknown keys from older/newer tracker docs are dropped cleanly.

### 7.5 Size-Tier Adaptive Timeouts

After ANALYZING, both orchestrators classify the index/group into a size tier and pull a `TierProfile`:

| Tier | Size | Recovery Timeout | Reindex Throttle |
|------|------|-----------------|-----------------|
| SMALL | < 1 GB | 10 min | Unlimited |
| MEDIUM | 1–50 GB | 30 min | 10,000 docs/sec |
| LARGE | 50–500 GB | 2 hours | 5,000 docs/sec |
| HUGE | > 500 GB | 6 hours | 2,000 docs/sec |

The orchestrator uses `max(cfg.recovery_timeout_minutes, tier_profile.recovery_timeout_minutes)` so the config value serves as a minimum. The tier throttle overrides the config throttle if more conservative.

## 8. Defrag Visualization Widget

A per-shard cell grid inspired by the Windows 95/98/XP defrag utility. Each shard in every index gets its own 10×10 pixel square cell in a continuous grid — an 18-index merge batch with 7-11 shards each becomes ~160 individual cells filling a proper chunky grid.

The data model uses `ShardCell` (one per shard) grouped into `IndexGroup` (one per index). State transitions use a staggered sweep animation (30ms per cell) that lights up each shard sequentially, creating the classic "scan head moving across the disk" effect.

On completion, an index's cells collapse: the first N cells (where N = target shard count, usually 1) turn green, and the remaining cells become "FREED" — painted as dark near-black, representing reclaimed shard budget.

States in the `AWAITING_*` family pulse with sinusoidal opacity (160-255 alpha, 50ms tick). The active index gets a white highlight overlay and border on all its non-collapsed cells. A scan-line highlight marks the next cell to be swept.

A legend bar below the status text shows colored squares with labels for the key states.

Tooltips show per-cell detail: index name, shard number (e.g., "Shard: 3/7"), state, target, and progress.

The CLI equivalent uses ANSI-colored `▓` block characters that wrap to terminal width and update in-place via cursor-up + clear.

## 9. CLI Mode

`sharderator-cli` provides headless operation with no PyQt6 dependency. Subcommands: `list`, `status`, `shrink`, `merge`. Full argument parsing for connection and operation settings, environment variable support (`ES_API_KEY`, `ES_PASSWORD`), and SIGINT/SIGTERM signal handling for graceful cancellation.

The CLI reuses the same `Orchestrator` and `MergeOrchestrator` classes as the GUI, passing a `CliEvents` implementation instead of `QtPipelineEvents`. No `QApplication` instance is needed.

Additional CLI features:
- `--order smallest-first|largest-first|as-is` on both shrink and merge
- `--priority "pattern1,pattern2"` on merge to move matching groups to the front
- Pre-run sizing reports during `--dry-run`

## 10. Thread Safety

Cancellation flags in both orchestrators use `threading.Event` instead of bare booleans. This is explicitly thread-safe and future-proof for PEP 703 free-threaded Python (3.13+). The `cancel()` method is called from the GUI thread; `_check_cancel()` reads from the worker thread.

## 11. Atomic Data Stream Swaps

The `swapper.py` module uses add-before-remove ordering in `_data_stream/_modify` calls. If a partial failure occurs, the data stream temporarily has both old and new backing indices (queries deduplicate) rather than missing a time range.

For merge mode, `_atomic_swap_multi_ds` collapses all add/remove actions across all data streams into a single `modify_data_stream` request. Either all succeed or none do — no partial-failure window between data streams. The function retries up to 3 times with idempotency checks — it re-fetches current membership before each attempt so partially applied changes from a previous failed attempt are handled correctly.

## 12. Test Suite

67 unit tests across 6 files, running in ~0.6s with no cluster dependency:

- `test_job.py` — State machine transitions (every valid path, every invalid path, terminal states, error storage, serialization round-trips using `dataclasses.asdict`)
- `test_index_info.py` — `needs_reindex`, `shard_reduction`, `store_size_mb` properties
- `test_merge_analyzer.py` — Index grouping at monthly/quarterly granularity, single-index exclusion, shard reduction calculation, mapping union with field drift and type conflicts. `TestMergeGroupFromJob` covers `from_job_record()` round-trips for both naming formats, fallback without enriched metadata, yearly buckets, and empty source indices.
- `test_preflight.py` — Cluster health gates (red/yellow/green, relocating, initializing), circuit breaker threshold detection with mocked ES client
- `test_defrag_widget.py` — Per-shard cell creation, group mapping, state updates with sweep queuing, completion collapse (target cells green, excess cells freed), failed state handling, progress tracking, paint rendering, reset, and edge cases
- `test_sizing.py` — Size tier classification, tier profile lookup, sort ordering (smallest-first/largest-first/as-is), priority queuing, dynamic batch sizing by disk budget

## 13. Elasticsearch API Surface

| Operation | API | Notes |
|-----------|-----|-------|
| Cluster health | `GET /_cluster/health` | Pre-flight + mid-operation |
| Circuit breakers | `GET /_nodes/stats/breaker` | Pre-flight |
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

## 14. Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| elasticsearch | >= 8.12 | Elasticsearch Python client |
| es_client | >= 8.14 | Connection configuration helper |
| pyyaml | >= 6.0 | Config file persistence |
| keyring | >= 25.0 | Secure credential storage |
| structlog | >= 24.0 | Structured logging |
| PyQt6 | >= 6.6 | GUI framework (optional, `[gui]` extra) |

Python >= 3.10 required.

## 15. Job Tracker Viewer

The third tab in the main window queries the `sharderator-tracker` Elasticsearch index and displays all tracked jobs in a table: index name, job type (SHRINK/MERGE), current state, error message, timestamps, and expected doc count. Rows are color-coded by state — red for FAILED, yellow for AWAITING_* states.

A right-click context menu provides Resume (re-queues the job from its last committed state), Delete (removes the tracker document), Copy Error (clipboard), and View History (popup dialog showing the full state transition timeline with timestamps).

**Resume for SHRINK jobs:** Creates a minimal `IndexInfo` from the job record and passes it to `Orchestrator.run()`. The orchestrator calls `load_job()` internally and resumes from the last committed state.

**Resume for MERGE jobs:** Calls `MergeGroup.from_job_record(job)` to reconstruct the `MergeGroup` from the persisted `enriched_metadata` field. If `enriched_metadata` is populated (all jobs since v0.2.0), full `IndexInfo` objects are rebuilt. If it's empty (very old job records), falls back to minimal `IndexInfo` from `source_frozen_indices` names. The reconstructed group is passed to `MergeOrchestrator.run()`, which calls `load_job()` and resumes from the last committed state.

`_parse_merged_name()` in `merge_analyzer.py` extracts `base_pattern` and `time_bucket` from the merged index name. Handles both the final frozen mount format (`partial-.ds-{base}-{bucket}-merged`) and the intermediate working name format (`sharderator-merged-{base}-{bucket}`).

The Refresh button in the toolbar (also bound to F5) reloads cluster metadata, the frozen index list, and the tracker tab in a single action. It also fires automatically after every batch completion via `_on_batch_done`, so the toolbar shard count and index table update live without requiring a manual disconnect/reconnect cycle.

The tracker query uses `list_all_jobs()` which searches the tracker index sorted by `updated_at` descending, limited to 500 documents. JSON blob fields (enriched_metadata, mappings, settings) are deserialized on load. If the tracker index doesn't exist yet, the viewer shows an empty table with a helpful message.

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

`check_cluster_health_mid_operation()` now checks thread pool queue depths, pending cluster tasks, JVM heap pressure, and relocating/initializing shards on every stage transition. Uses linear backoff when pressure is detected.

### Size-Based Ordering

`sort_indices()` and `sort_merge_groups()` in `sizing.py` support `smallest-first`, `largest-first`, and `as-is` ordering. Default is smallest-first (fast shard reclamation, canary behavior).

### Priority Queuing

`prioritize_groups()` in `sizing.py` moves groups matching priority patterns to the front of the queue. Available via `--priority` CLI flag.

## 16. Export Feature

Three export types for change ticket documentation and operational record-keeping.

**Frozen index list** — `SizingReport.to_dict()` serializes the report using `dataclasses.asdict()`. The Shrink Mode tab has an Export button that saves the frozen index table as CSV or JSON via a file save dialog. CLI: `sharderator-cli list --csv` or `--json`.

**Dry-run sizing report** — The Merge Mode tab has an "Export Dry Run" button that generates a sizing report, runs preflight checks, and saves the combined result as JSON. The JSON includes per-group detail (source indices, shard reduction, mapping conflicts) and preflight results. CLI: `sharderator-cli shrink --dry-run --output json` or `sharderator-cli merge --dry-run --output json`.

**Job history** — The Job Tracker tab has an Export button that saves all tracked jobs as JSON with cluster name and timestamp. Right-click any row → "Export Job" saves a single job record. CLI: `sharderator-cli report` (all jobs), `sharderator-cli report --index <name>` (single job), `sharderator-cli report --state COMPLETED|FAILED|all` (filtered).

The `report` subcommand queries the `sharderator-tracker` index and outputs JSON with `default=str` to handle any float timestamps or enum values.

## 17. Known Limitations

- Cross-index merge (combining indices from different base patterns) is not supported. Each merge group must share a base pattern.
- The tool assumes standard Elastic naming conventions (`partial-`, `.ds-`, generation numbers). Indices with non-standard names may not be detected by the merge analyzer.
- Minimum ES version 8.7 is required. Some features (like the Health Report API) work better on 8.12+.
- No multi-cluster support — one connection at a time.
- The CLI requires the same Python package as the GUI but not PyQt6. The `[gui]` extra adds PyQt6 for the GUI entry point.
- Merge job resume from the GUI Tracker tab requires re-running the merge group from the Merge Mode tab (the tracker resume button handles shrink jobs only).
