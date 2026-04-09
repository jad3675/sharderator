"""SWAPPING state — swap data stream backing indices AFTER verification passes.

Safety fix: data stream mutations now happen after we've confirmed the new
mount is healthy, not during REMOUNTING where a verification failure would
leave the data stream pointing at a bad index with no rollback.
"""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def swap_data_stream(
    client: Elasticsearch, info: IndexInfo, job: JobRecord
) -> None:
    """Swap single backing index. Called after verification for shrink mode."""
    job.transition(JobState.SWAPPING)

    if not info.is_data_stream_member or not info.data_stream_name:
        log.info("no_data_stream_swap_needed", index=info.name)
        return

    new_index = job.new_frozen_index

    # Verify old index is still in the data stream
    ds_info = client.indices.get_data_stream(name=info.data_stream_name)
    current_backing: set[str] = set()
    for ds in ds_info.get("data_streams", []):
        if ds["name"] == info.data_stream_name:
            current_backing = {
                b["index_name"] for b in ds.get("indices", [])
            }
            break

    if info.name not in current_backing:
        log.warning("old_index_not_in_data_stream", index=info.name)
        client.indices.modify_data_stream(
            body={"actions": [
                {"add_backing_index": {"data_stream": info.data_stream_name, "index": new_index}}
            ]}
        )
        return

    client.indices.modify_data_stream(
        body={"actions": [
            {"remove_backing_index": {"data_stream": info.data_stream_name, "index": info.name}},
            {"add_backing_index": {"data_stream": info.data_stream_name, "index": new_index}},
        ]}
    )
    log.info(
        "data_stream_swapped",
        data_stream=info.data_stream_name,
        old=info.name,
        new=new_index,
    )


def swap_data_stream_merged(
    client: Elasticsearch,
    job: JobRecord,
    source_indices: list[IndexInfo],
) -> None:
    """Swap all source backing indices for merged index. Called after verification."""
    job.transition(JobState.SWAPPING)

    final_name = job.new_frozen_index
    ds_groups: dict[str, list[IndexInfo]] = {}
    for idx in source_indices:
        if idx.is_data_stream_member and idx.data_stream_name:
            ds_groups.setdefault(idx.data_stream_name, []).append(idx)

    if not ds_groups:
        log.info("no_data_stream_swap_needed_merge")
        return

    for ds_name, members in ds_groups.items():
        # Verify membership before swapping
        try:
            ds_info = client.indices.get_data_stream(name=ds_name)
            current_backing: set[str] = set()
            for ds in ds_info.get("data_streams", []):
                if ds["name"] == ds_name:
                    current_backing = {
                        b["index_name"] for b in ds.get("indices", [])
                    }
                    break
            valid = [m for m in members if m.name in current_backing]
            if len(valid) < len(members):
                removed = [m.name for m in members if m.name not in current_backing]
                log.warning(
                    "backing_indices_already_removed",
                    data_stream=ds_name,
                    removed=removed,
                )
        except Exception:
            valid = members

        actions = [
            {"remove_backing_index": {"data_stream": ds_name, "index": m.name}}
            for m in valid
        ]
        actions.append(
            {"add_backing_index": {"data_stream": ds_name, "index": final_name}}
        )
        client.indices.modify_data_stream(body={"actions": actions})
        log.info(
            "data_stream_merged_swapped",
            data_stream=ds_name,
            sources=len(valid),
        )
