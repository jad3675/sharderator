"""VERIFYING state — confirm the new frozen mount matches the original."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


class VerificationError(Exception):
    pass


def verify(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
) -> None:
    """Verify the new frozen mount is healthy and matches the original.

    Fix 2.3: Uses job.expected_doc_count (captured during ANALYZING) instead of
    re-querying the old frozen index. The old index may have been swapped out of
    a data stream by this point, and frozen stats are slow/unreliable.
    """
    job.transition(JobState.VERIFYING)
    new_index = job.new_frozen_index
    errors: list[str] = []

    # 1. Doc count — compare against stored value
    old_docs = job.expected_doc_count
    new_stats = client.indices.stats(index=new_index)
    new_docs = new_stats["_all"]["primaries"]["docs"]["count"]
    if old_docs != new_docs:
        errors.append(f"Doc count mismatch: expected={old_docs}, new={new_docs}")

    # 2. Shard count
    new_settings = client.indices.get_settings(index=new_index)
    new_shards = int(
        new_settings[new_index]["settings"]["index"]["number_of_shards"]
    )
    if new_shards != info.target_shard_count:
        errors.append(
            f"Shard count mismatch: expected={info.target_shard_count}, got={new_shards}"
        )

    # 3. Health
    health = client.cluster.health(index=new_index)
    if health["status"] != "green":
        errors.append(f"New index health is {health['status']}, expected green")

    # 4. Mapping equivalence (structural check)
    new_mapping = client.indices.get_mapping(index=new_index)
    new_props = new_mapping[new_index]["mappings"].get("properties", {})
    old_props = info.mappings.get("properties", {})
    if set(new_props.keys()) != set(old_props.keys()):
        missing = set(old_props.keys()) - set(new_props.keys())
        extra = set(new_props.keys()) - set(old_props.keys())
        errors.append(f"Mapping field mismatch: missing={missing}, extra={extra}")

    if errors:
        msg = "; ".join(errors)
        log.error("verification_failed", index=new_index, errors=errors)
        raise VerificationError(msg)

    log.info("verification_passed", index=new_index, docs=new_docs, shards=new_shards)
