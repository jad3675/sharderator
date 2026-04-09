"""Persist and resume job state via an Elasticsearch tracking index."""

from __future__ import annotations

from elasticsearch import Elasticsearch, NotFoundError

from sharderator.models.job import JobRecord
from sharderator.util.logging import get_logger

log = get_logger(__name__)

TRACKING_INDEX = "sharderator-tracker"


def save_job(client: Elasticsearch, job: JobRecord) -> None:
    """Upsert the job record into the tracking index."""
    try:
        client.index(
            index=TRACKING_INDEX,
            id=job.index_name,
            document=job.to_dict(),
            refresh="wait_for",
        )
    except Exception as e:
        log.warning("tracker_save_failed", index=job.index_name, error=str(e))


def load_job(client: Elasticsearch, index_name: str) -> JobRecord | None:
    """Load a job record, or None if not found."""
    try:
        doc = client.get(index=TRACKING_INDEX, id=index_name)
        return JobRecord.from_dict(doc["_source"])
    except NotFoundError:
        return None
    except Exception as e:
        log.warning("tracker_load_failed", index=index_name, error=str(e))
        return None


def delete_job(client: Elasticsearch, index_name: str) -> None:
    """Remove a completed job record."""
    try:
        client.delete(index=TRACKING_INDEX, id=index_name, refresh="wait_for")
    except NotFoundError:
        pass
    except Exception as e:
        log.warning("tracker_delete_failed", index=index_name, error=str(e))
