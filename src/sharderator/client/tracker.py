"""Persist and resume job state via an Elasticsearch tracking index."""

from __future__ import annotations

import json

from elasticsearch import Elasticsearch, NotFoundError

from sharderator.models.job import JobRecord
from sharderator.util.logging import get_logger

log = get_logger(__name__)

TRACKING_INDEX = "sharderator-tracker"

# Fields that contain deeply nested dicts (like full index mappings) which
# would blow past ES's default 1000-field limit if indexed as objects.
# We serialize these to JSON strings before saving and deserialize on load.
_JSON_BLOB_FIELDS = ("enriched_metadata", "mappings", "settings")


def save_job(client: Elasticsearch, job: JobRecord) -> None:
    """Upsert the job record into the tracking index."""
    try:
        doc = job.to_dict()
        for field in _JSON_BLOB_FIELDS:
            if field in doc and doc[field]:
                doc[field] = json.dumps(doc[field])
        client.index(
            index=TRACKING_INDEX,
            id=job.index_name,
            document=doc,
            refresh="wait_for",
        )
    except Exception as e:
        log.warning("tracker_save_failed", index=job.index_name, error=str(e))


def load_job(client: Elasticsearch, index_name: str) -> JobRecord | None:
    """Load a job record, or None if not found."""
    try:
        doc = client.get(index=TRACKING_INDEX, id=index_name)
        source = doc["_source"]
        for field in _JSON_BLOB_FIELDS:
            if field in source and isinstance(source[field], str):
                try:
                    source[field] = json.loads(source[field])
                except (json.JSONDecodeError, TypeError):
                    log.warning(
                        "tracker_json_parse_failed",
                        field=field,
                        index=index_name,
                    )
                    source[field] = [] if field == "enriched_metadata" else {}
        return JobRecord.from_dict(source)
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


def list_all_jobs(client: Elasticsearch) -> list[JobRecord]:
    """Query the tracker index for all job documents, newest first."""
    try:
        results = client.search(
            index=TRACKING_INDEX,
            size=500,
            sort=[{"updated_at": {"order": "desc"}}],
        )
        jobs = []
        for hit in results["hits"]["hits"]:
            source = hit["_source"]
            for field in _JSON_BLOB_FIELDS:
                if field in source and isinstance(source[field], str):
                    try:
                        source[field] = json.loads(source[field])
                    except (json.JSONDecodeError, TypeError):
                        log.warning(
                            "tracker_json_parse_failed",
                            field=field,
                            index=source.get("index_name", "unknown"),
                        )
                        source[field] = [] if field == "enriched_metadata" else {}
            jobs.append(JobRecord.from_dict(source))
        return jobs
    except NotFoundError:
        return []
    except Exception as e:
        log.warning("tracker_list_failed", error=str(e))
        return []
