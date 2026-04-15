"""VERIFYING state — confirm the new frozen mount matches the original."""

from __future__ import annotations

import time

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


class VerificationError(Exception):
    pass


def _wait_for_doc_count(
    client: Elasticsearch,
    index: str,
    timeout_seconds: int = 60,
) -> int | None:
    """Poll until a frozen index reports doc stats.

    Frozen indices are partially mounted searchable snapshots; their stats
    can take a few seconds to populate after mount. Returns doc count,
    or None if timed out.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            # Wait for at least yellow health first
            client.cluster.health(index=index, wait_for_status="yellow", timeout="10s")
            stats = client.indices.stats(index=index)
            primaries = stats.get("_all", {}).get("primaries", {})
            docs = primaries.get("docs")
            if docs is not None and "count" in docs:
                return docs["count"]
        except Exception:
            pass
        time.sleep(2)
    return None


def verify(
    client: Elasticsearch,
    info: IndexInfo,
    job: JobRecord,
) -> None:
    """Verify the new frozen mount is healthy and matches the original."""
    job.transition(JobState.VERIFYING)
    new_index = job.new_frozen_index
    errors: list[str] = []

    # 1. Doc count — wait for frozen stats to become available, then compare
    new_docs = _wait_for_doc_count(client, new_index, timeout_seconds=60)
    if new_docs is None:
        errors.append(
            f"Timed out waiting for stats on {new_index}. "
            f"Frozen index may still be initializing."
        )
    else:
        old_docs = job.expected_doc_count
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

    log.info(
        "verification_passed",
        index=new_index,
        docs=new_docs,
        shards=new_shards,
    )
