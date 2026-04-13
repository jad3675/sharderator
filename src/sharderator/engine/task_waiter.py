"""Shared async task polling logic for reindex and other long-running ES tasks."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

from elasticsearch import Elasticsearch

from sharderator.util.logging import get_logger

log = get_logger(__name__)

# Failure types that indicate transient cluster issues (shard offline, node
# overloaded, etc.) rather than permanent data/mapping problems. A reindex
# retry with conflicts:proceed can recover from these.
TRANSIENT_FAILURE_TYPES = {
    "unavailable_shards_exception",
    "es_rejected_execution_exception",
    "cluster_block_exception",
    "node_disconnected_exception",
    "node_not_connected_exception",
    "receive_timeout_transport_exception",
    "send_request_transport_exception",
    "connect_transport_exception",
}


class TransientReindexError(RuntimeError):
    """Raised when all reindex doc failures are transient and retryable."""

    def __init__(self, message: str, failure_count: int, created: int, total: int):
        super().__init__(message)
        self.failure_count = failure_count
        self.created = created
        self.total = total


class PermanentReindexError(RuntimeError):
    """Raised when reindex doc failures include non-transient errors."""
    pass


def wait_for_task(
    client: Elasticsearch,
    task_id: str,
    progress_callback=None,
    timeout_minutes: int = 120,
) -> None:
    """Poll an async ES task until completion. Used by shrink reindex and merge reindex.

    Raises:
        TransientReindexError: All doc failures are transient (caller should retry).
        PermanentReindexError: At least one non-transient doc failure.
        RuntimeError: Task-level error or timeout.
    """
    deadline = time.time() + timeout_minutes * 60

    while time.time() < deadline:
        try:
            task = client.tasks.get(task_id=task_id)
            if task.get("completed"):
                # Extract readable error detail
                error = task.get("error")
                if error:
                    if isinstance(error, dict):
                        reason = error.get("reason", "")
                        err_type = error.get("type", "unknown")
                        caused_by = error.get("caused_by", {})
                        caused_reason = (
                            caused_by.get("reason", "")
                            if isinstance(caused_by, dict)
                            else ""
                        )
                        detail = f"{err_type}: {reason}"
                        if caused_reason:
                            detail += f" (caused by: {caused_reason})"
                    else:
                        detail = str(error)
                    raise RuntimeError(f"Reindex task failed: {detail}")

                # Check for partial document failures
                response = task.get("response", {})
                failures = response.get("failures", [])
                if failures:
                    total_created = response.get("created", 0)
                    total_docs = response.get("total", 0)
                    sample = failures[0]
                    detail = _extract_failure_detail(sample)

                    # Log full failure for debugging
                    log.error(
                        "reindex_doc_failures",
                        task_id=task_id,
                        failure_count=len(failures),
                        total_docs=total_docs,
                        created=total_created,
                        first_failure=json.dumps(sample, default=str)[:2000],
                    )

                    # Classify: are ALL failures transient?
                    all_transient = all(
                        _is_transient_failure(f) for f in failures
                    )

                    msg = (
                        f"Reindex completed with {len(failures)} doc failure(s) "
                        f"out of {total_docs} total ({total_created} created). "
                        f"First: {detail}"
                    )

                    if all_transient:
                        raise TransientReindexError(
                            msg, len(failures), total_created, total_docs,
                        )
                    else:
                        raise PermanentReindexError(msg)
                return

            if progress_callback:
                status = task.get("task", {}).get("status", {})
                total = status.get("total", 0)
                created = status.get("created", 0)
                if total > 0:
                    progress_callback(created / total * 100)
        except (TransientReindexError, PermanentReindexError, RuntimeError):
            raise
        except Exception:
            pass

        time.sleep(10)

    raise RuntimeError(f"Task timeout after {timeout_minutes}m for task {task_id}")


def _is_transient_failure(failure: dict) -> bool:
    """Check if a reindex doc failure is a transient cluster issue."""
    cause = failure.get("cause", {})
    if isinstance(cause, dict):
        err_type = cause.get("type", "")
        if err_type in TRANSIENT_FAILURE_TYPES:
            return True
        # Check nested caused_by
        caused_by = cause.get("caused_by", {})
        if isinstance(caused_by, dict):
            inner_type = caused_by.get("type", "")
            if inner_type in TRANSIENT_FAILURE_TYPES:
                return True
    return False


def _extract_failure_detail(failure: dict) -> str:
    """Best-effort extraction of a human-readable error from a reindex failure object.

    ES failure structures are inconsistent. Known shapes:
      {"cause": {"type": "...", "reason": "..."}}
      {"cause": {"type": "...", "reason": "...", "caused_by": {"type": "...", "reason": "..."}}}
      {"reason": "..."}
      {"index": "...", "type": "...", "id": "...", "cause": {...}}
    """
    parts = []

    # Index/doc ID context
    if failure.get("index"):
        parts.append(f"index={failure['index']}")
    if failure.get("id"):
        parts.append(f"doc={failure['id']}")

    # Primary error from cause dict
    cause = failure.get("cause", {})
    if isinstance(cause, dict):
        err_type = cause.get("type", "")
        reason = cause.get("reason", "")
        if err_type or reason:
            parts.append(f"{err_type}: {reason}".strip(": "))

        # Nested caused_by
        caused_by = cause.get("caused_by", {})
        if isinstance(caused_by, dict):
            inner_type = caused_by.get("type", "")
            inner_reason = caused_by.get("reason", "")
            if inner_type or inner_reason:
                parts.append(f"caused by {inner_type}: {inner_reason}".strip(": "))
    elif isinstance(cause, str):
        parts.append(cause)

    # Fallback: top-level reason
    if not parts and failure.get("reason"):
        parts.append(str(failure["reason"]))

    return "; ".join(parts) if parts else f"(unknown structure: {json.dumps(failure, default=str)[:500]})"
