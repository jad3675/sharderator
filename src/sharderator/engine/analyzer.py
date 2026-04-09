"""ANALYZING state — gather index metadata and determine consolidation strategy."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.client.queries import enrich_index_info
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def analyze(client: Elasticsearch, info: IndexInfo, job: JobRecord) -> IndexInfo:
    """Enrich index info and validate that consolidation is possible."""
    job.transition(JobState.ANALYZING)

    info = enrich_index_info(client, info)

    # Validate snapshot source exists
    if not info.snapshot_name or not info.repository_name:
        raise RuntimeError(
            f"Cannot determine source snapshot for {info.name}. "
            "Index may have been manually mounted."
        )

    # Verify snapshot is accessible
    snap = client.snapshot.get(
        repository=info.repository_name, snapshot=info.snapshot_name
    )
    snapshots = snap.get("snapshots", [])
    if not snapshots or snapshots[0].get("state") != "SUCCESS":
        raise RuntimeError(
            f"Source snapshot {info.snapshot_name} in {info.repository_name} "
            "is not in SUCCESS state."
        )

    job.original_snapshot_name = info.snapshot_name
    job.repository_name = info.repository_name

    # Fix 2.3: Capture authoritative doc count now, before any mutations.
    # The verifier will compare against this stored value rather than
    # re-querying the old frozen index (which may be slow or swapped out).
    job.expected_doc_count = info.doc_count

    if info.has_custom_routing:
        log.warning(
            "custom_routing_detected",
            index=info.name,
            msg="Index uses custom routing. Shrink preserves routing; "
            "reindex fallback must explicitly handle _routing field.",
        )

    log.info(
        "analysis_complete",
        index=info.name,
        shards=info.shard_count,
        target=info.target_shard_count,
        needs_reindex=info.needs_reindex,
        data_stream=info.data_stream_name or None,
    )
    return info
