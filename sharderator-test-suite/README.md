# Sharderator Test Suite

End-to-end test harness for validating Sharderator against a real Elastic Cloud trial deployment.

Creates two realistic frozen-tier shard pressure scenarios:

- **Scenario A** — Over-sharded indices (10/20/50 primary shards) pushed to frozen via ILM
- **Scenario B** — Daily index explosion (90 days × 40 metric types = 3,600 single-shard indices)

Together these exceed the 3,000 shard ceiling and exercise both Sharderator shrink mode and merge mode.

## Prerequisites

- Python 3.10+
- An Elastic Cloud trial deployment with frozen tier enabled
- `pip install elasticsearch`

## Setup

```bash
cd sharderator-test-suite
pip install -e .
```

Copy the environment template and fill in your credentials:

```bash
cp .env.example .env
# Edit .env with your CLOUD_ID and API_KEY
```

## Quick Start

```bash
# 1. Configure the cluster (ILM policies, templates, speed up poll interval)
sharderator-test setup

# 2. Generate Scenario A data (over-sharded indices)
sharderator-test ingest-oversharded

# 3. Generate Scenario B data (daily index explosion)
sharderator-test ingest-daily

# 4. Monitor ILM progress until indices are frozen
sharderator-test status

# 5. Run the full validation checklist
sharderator-test validate --phase all

# 6. Clean up everything when done
sharderator-test cleanup
```

## Individual Commands

```bash
# Cluster setup only
sharderator-test setup --cloud-id $CLOUD_ID --api-key $KEY

# Ingest with custom doc count
sharderator-test ingest-oversharded --docs-per-stream 200000

# Ingest daily with fewer days (faster)
sharderator-test ingest-daily --days 30

# Check frozen shard budget
sharderator-test status

# Run specific validation phase
sharderator-test validate --phase dry-run
sharderator-test validate --phase shrink
sharderator-test validate --phase merge
sharderator-test validate --phase crash-recovery

# Export results
sharderator-test validate --phase export --output results.json

# Cleanup
sharderator-test cleanup --dry-run   # show what would be deleted
sharderator-test cleanup             # actually delete
```

## Validation Phases

| Phase | What it tests |
|-------|--------------|
| `read-only` | `list` and `status` commands |
| `dry-run` | Shrink and merge dry runs with JSON export |
| `shrink` | Single index shrink, then batch |
| `merge` | Single group merge, then batch |
| `crash-recovery` | Interrupt mid-operation, resume from tracker |
| `export` | Job history JSON export |
| `all` | All phases in order |

## Expected Shard Budget

| Source | Indices | Shards/Index | Total |
|--------|---------|-------------|-------|
| Oversharded-10 (~6 rollovers) | ~6 | 10 | ~60 |
| Oversharded-20 (~6 rollovers) | ~6 | 20 | ~120 |
| Oversharded-50 (~3 rollovers) | ~3 | 50 | ~150 |
| Daily explosion (90d × 40 types) | 3,600 | 1 | 3,600 |
| **Total** | | | **~3,930** |

This exceeds the 3,000 ceiling. Generate in batches if needed: create 60 days, run Sharderator to consolidate, then create remaining 30 days.

## Timeline

| Day | Activity |
|-----|----------|
| 1 | Deploy, run `setup` + `ingest-*` |
| 1–2 | Wait for ILM to freeze everything (`status --watch`) |
| 2 | `validate --phase read-only` and `dry-run` |
| 2–3 | `validate --phase shrink` and `merge` |
| 3–5 | Batch operations at scale |
| 5–6 | `validate --phase crash-recovery` and `export` |
