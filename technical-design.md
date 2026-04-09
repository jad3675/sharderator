# Sharderator — Technical Design Document

Author: John @ AHEAD
Date: April 2026
Version: 0.2.0

---

## 1. Background

Elastic Cloud's frozen tier stores data as partially mounted searchable snapshots backed by object storage (S3, GCS, Azure Blob). Each frozen data node has a hard shard limit of 3,000 (`cluster.max_shards_per_node.frozen`). In managed environments with hundreds of ILM-driven indices, this budget fills up fast — either because indices arrive with too many primary shards, or because daily index patterns create one shard per day per metric type.

Frozen indices are immutable. You cannot shrink, reindex, or forcemerge them in place. The only way to restructure them is to restore a full copy from the backing snapshot to a writable tier, mutate it, re-snapshot, and re-mount.

Sharderator automates this workflow with a PyQt6 GUI, two operational modes (shrink and merge), and safety hardening for production clusters.

## 2. Prior Art: Elastic PII Tool

The design draws heavily from Elastic's [PII redaction tool](https://github.com/elastic/pii-tool) (`elastic/pii-tool`), which solves a related problem: mutating document content in frozen-tier indices. The PII tool's architecture established several patterns we adopted and adapted.

### Patterns Borrowed from the PII Tool

The PII tool restores frozen indices from their backing snapshots to a writable tier using `restore_settings` with `_tier_preference` overrides to route to warm/hot nodes. We use the same approach — our `restorer.py` mirrors this pattern, restoring with `index.routing.allocation.include._tier_preference: data_warm,data_hot,data_content` and zero replicas.

The PII tool tracks job progress in a dedicated Elasticsearch index (`redactions-tracker`) so interrupted jobs can resume. Our `tracker.py` implements the same concept with `sharderator-tracker`, persisting full `JobRecord` state after each pipeline transition.

The PII tool handles the `partial-*` prefix convention for frozen indices and `restored-*` for cold tier mounts. Our code handles the same naming conventions throughout the pipeline, including during snapshot deletion safety checks where we verify both frozen and cold mount prefixes.

The PII tool force-merges after mutation (`max_num_segments: 1` or `only_expunge_deletes`) before re-snapshotting. We always force-merge to 1 segment per shard since we're creating fresh snapshots — fewer segments means fewer reads from object storage during search.

The PII tool uses `es_client` for connection management with environment variable support. We use `elasticsearch-py` directly with the same auth patterns (Cloud ID + API key, basic auth) but wrap it in a GUI connection dialog with keyring-backed credential storage.

### Where We Diverge

The PII tool modifies documents in-place via `update_by_query` then re-snapshots. Sharderator never touches document content — we restructure shard layout via the Shrink API or multi-source reindex.

The PII tool is CLI-driven with YAML job configuration files. Sharderator replaces this with a GUI that auto-discovers frozen indices, proposes merge candidates, and provides interactive selection with real-time progress.

The PII tool processes one index per job definition. Sharderator's merge mode processes N indices as a group, requiring batched restores, union mapping computation, and atomic data stream swaps.

The PII tool's dry-run mode (`--dry-run`) skips mutations. Our dry run goes further — it exercises the full pre-flight safety suite (cluster health, circuit breakers, disk space calculations) against the live cluster and reports whether execution would be blocked.

## 3. Architecture

```
+---------------------------------------------------+
|                   PyQt6 GUI                        |
|  +-------------+ +-----------+ +----------------+ |
|  | Connection  | | Shrink    | | Merge Mode     | |
|  | Dialog      | | Mode Tab  | | Tab + Tree     | |
|  +------+------+ +-----+-----+ +-------+--------+ |
|         |              |               |           |
|  +------v--------------v---------------v--------+  |
|  |            Main Window                       |  |
|  |  (worker threads, signal routing, log)       |  |
|  +-------------------+--------------------------+  |
|                      |                             |
|  +-------------------v--------------------------+  |
|  |  Orchestrator (shrink) / MergeOrchestrator   |  |
|  |  State machine per job, preflight checks,    |  |
|  |  mid-operation health monitoring             |  |
|  +-------------------+--------------------------+  |
|                      |                             |
|  +-------------------v--------------------------+  |
|  |         Engine Modules                       |  |
|  |  preflight | analyzer | restorer | shrinker  |  |
|  |  merger | snapshotter | mounter | swapper    |  |
|  |  verifier | cleaner                          |  |
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

The main thread runs the Qt event loop and handles all GUI updates. Pipeline execution runs in a `QThread` (`ShrinkWorkerThread` or `MergeWorkerThread`). Communication between the worker and GUI uses Qt signals for progress, state changes, log messages, and completion/failure notifications.

No concurrent index operations within a single run. Shrink mode processes indices sequentially. Merge mode processes groups sequentially, but within each group, restores happen in configurable batches (default 3).

## 4. State Machine

Both pipelines share a unified `JobState` enum with valid transitions enforced at runtime. The shrink pipeline uses the SHRINKING/AWAITING_SHRINK path; the merge pipeline uses the MERGING/AWAITING_MERGE path. Both converge at FORCE_MERGING.

```
PENDING
  |
  v
ANALYZING ---- gather metadata, validate snapshot source, cache doc count
  |
  v
RESTORING ---- restore from snapshot to warm/hot tier (batched in merge mode)
  |
  v
AWAITING_RECOVERY ---- poll until all restored indices are green
  |
  +---> SHRINKING (shrink mode) ---- shrink API or reindex fallback
  |       |
  |       v
  |     AWAITING_SHRINK ---- wait for shrunk index to be green
  |       |
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
SWAPPING ---- swap data stream backing indices (only after verification)
  |
  v
CLEANING_UP ---- delete old mount, intermediates, optionally old snapshot
  |
  v
COMPLETED
```

Any state can transition to FAILED. Failed jobs are persisted to the tracking index. On retry, a FAILED job is treated as a fresh start.

## 5. Shrink Mode Pipeline Detail

### 5.1 Analyzing

Reads `index.store.snapshot.*` settings to identify the source snapshot and repository. Retrieves mappings, detects custom routing (`_routing.required`), checks data stream membership (cached via `get_data_stream_membership` to avoid N redundant API calls). Calculates target shard count (1 unless doc count exceeds the Lucene 2.1B limit). Stores the authoritative doc count on the job record for later verification — the verifier uses this stored value rather than re-querying the frozen index, which may be slow or already swapped out of a data stream.

### 5.2 Restoring

Hard disk space gate runs first: calculates post-restore free percentage on warm/hot/content nodes (not all nodes — frozen nodes with terabytes free can't be used for restores). If free space would drop below the safety margin (default 30%), the operation is refused with `InsufficientDiskError`.

Restores from the backing snapshot with `_tier_preference` override to route to the working tier, zero replicas, and a `sharderator-restore-` prefix to avoid name collisions.

### 5.3 Shrinking

Prefers the Shrink API (hard-link based, dramatically faster than reindex). Selects the warm/hot node with the most available disk, relocates all primaries there, sets read-only, then waits for colocation confirmation by polling `_cat/shards` — not just green health, which doesn't guarantee all primaries are on the target node. Falls back to reindex if the target shard count isn't a factor of the source (rare when targeting 1, since 1 divides everything).

Reindex respects the configurable `requests_per_second` throttle to avoid starving production ingest.

### 5.4 Force Merge and Snapshot

Force-merges to 1 segment per shard. Checks the tasks API for already-running force merges before starting (prevents duplicate merges after HTTP timeouts). On timeout, polls for completion instead of starting a second merge.

Creates a one-off snapshot with `sharderator-{original_name}-{timestamp}` naming. Polls `_snapshot/_status` until SUCCESS.

### 5.5 Remounting and Verification

Mounts the new snapshot as `shared_cache` (frozen) with a temporary name to avoid collision with the still-existing old mount. Strips ILM (`index.lifecycle.indexing_complete: true`) to prevent re-processing.

Verification compares the stored expected doc count against the new mount's actual count, checks shard count matches target, confirms green health, and validates mapping field equivalence.

Data stream backing index swap happens in the SWAPPING state, after verification passes. This ordering is critical — if verification fails, the data stream still points at the old (working) index.

### 5.6 Cleanup

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

When the operator checks a merge group in the tree, a validation function fires (once per group). It checks snapshot metadata accessibility for all source indices and samples mapping drift between the first and last index. Non-viable groups are greyed out and uncheckable. Groups with mapping drift get a yellow warning but remain selectable — `union_mappings` handles field drift at execution time.

### 6.3 Batched Restore

All source indices in a merge group need to be restored to the working tier. Instead of firing all N restores simultaneously, they're batched in groups of `restore_batch_size` (default 3). Each batch waits for green health before the next batch starts. This prevents I/O saturation and circuit breaker trips.

A cumulative disk space gate runs first, accounting for 2x the total source size (sources + merged target).

Restore names include the time bucket slug to avoid collisions across merge groups. Leftover indices from previous failed runs are cleaned up before restoring.

### 6.4 Multi-Source Reindex

Creates the target index with union mappings and merged analysis settings (normalizers, analyzers, tokenizers, filters from all source indices). Uses `body=` with correct nesting for the analysis block — the dotted `index.analysis` notation doesn't work for nested objects in the ES create index API.

Reindex runs asynchronously via the tasks API with configurable `requests_per_second` throttle. Polls for completion, extracts readable error details from the task response (type, reason, caused_by), and checks for partial document failures.

### 6.5 Data Stream Atomic Swap

After verification, all source backing indices are removed from the data stream and the merged index is added in a single `_data_stream/_modify` call. Before building the swap actions, each source index is verified to still be a data stream member — parallel ILM actions or previous partial runs may have already removed some.

## 7. Safety Architecture

### 7.1 Pre-Flight Checks

Run at the start of every fresh job (not on resume) and during dry runs:

- Cluster health: red = block, yellow = configurable, >10 relocating/initializing shards = block
- Circuit breakers: any breaker >80% = block (overridable via settings)
- Disk space: calculates post-operation free percentage on working tier nodes only

### 7.2 Mid-Operation Monitoring

Between every pipeline stage, `_check_cancel` also calls `check_cluster_health_mid_operation`. If the cluster goes red, the tool pauses for up to 15 minutes, polling every 30 seconds. If it recovers, execution continues. If it stays red, the job fails and can be resumed later.

### 7.3 Verification Before Mutation

The SWAPPING state (data stream modification) was deliberately separated from REMOUNTING and placed after VERIFYING. This ensures that the irreversible data stream mutation only happens after we've confirmed the new mount is healthy. If verification fails, the data stream still points at the original index.

### 7.4 Job Persistence

Every state transition is persisted to the `sharderator-tracker` Elasticsearch index via `save_job`. On restart, `load_job` retrieves the last known state. FAILED jobs are treated as fresh starts on retry (the stale record is deleted). Merge jobs serialize enriched index metadata into the job record so resume doesn't require re-querying source indices that may have been partially cleaned up.

## 8. Elasticsearch API Surface

| Operation | API | Notes |
|-----------|-----|-------|
| Cluster health | `GET /_cluster/health` | Pre-flight + mid-operation |
| Circuit breakers | `GET /_nodes/stats/breaker` | Pre-flight |
| Node disk info | `GET /_cat/nodes?h=name,disk.avail,disk.total,node.role&bytes=b` | Disk gate |
| List frozen indices | `GET /_cat/indices?h=index,pri,docs.count,store.size,dataset.size&bytes=b` | Uses `dataset.size` for accurate frozen sizes |
| Index settings | `GET /<index>/_settings` | Snapshot source discovery |
| Index mapping | `GET /<index>/_mapping` | Mapping preservation + drift detection |
| Data streams | `GET /_data_stream/*` | Membership detection (cached) |
| Restore snapshot | `POST /_snapshot/<repo>/<snap>/_restore` | With tier preference override |
| Shard allocation | `GET /_cat/shards/<index>?h=node,prirep` | Colocation verification |
| Shrink index | `POST /<index>/_shrink/<target>` | Hard-link based |
| Reindex | `POST /_reindex` | Async with tasks API, throttled |
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

## 9. Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| PyQt6 | >= 6.6 | GUI framework |
| elasticsearch | >= 8.12 | Elasticsearch Python client |
| es_client | >= 8.14 | Connection configuration helper |
| pyyaml | >= 6.0 | Config file persistence |
| keyring | >= 25.0 | Secure credential storage |
| structlog | >= 24.0 | Structured logging |

Python >= 3.10 required.

## 10. Known Limitations

- No CLI mode yet — GUI only. CLI with YAML job files (similar to the PII tool pattern) is planned for a future version.
- Cross-index merge (combining indices from different base patterns) is not supported. Each merge group must share a base pattern.
- The tool assumes standard Elastic naming conventions (`partial-`, `.ds-`, generation numbers). Indices with non-standard names may not be detected by the merge analyzer.
- Minimum ES version 8.7 is required. Some features (like the Health Report API) work better on 8.12+.
- No multi-cluster support — one connection at a time.
