"""SWAPPING state — data stream backing index swap AFTER verification.

Key safety properties:
- Add-before-remove ordering (partial failure = both old+new, not missing)
- Single-DS shrink swaps use _atomic_swap with retry
- Multi-DS merge swaps use _atomic_swap_multi_ds: ALL actions across ALL
  data streams in ONE modify_data_stream call
"""
from __future__ import annotations
import time
from elasticsearch import Elasticsearch
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState
from sharderator.util.logging import get_logger
log = get_logger(__name__)


def swap_data_stream(client: Elasticsearch, info: IndexInfo, job: JobRecord) -> None:
    """Swap single backing index after verification (shrink mode)."""
    job.transition(JobState.SWAPPING)
    if not info.is_data_stream_member or not info.data_stream_name:
        log.info("no_data_stream_swap_needed", index=info.name)
        return
    _atomic_swap(client, info.data_stream_name, [job.new_frozen_index], [info.name])
    log.info("data_stream_swapped", ds=info.data_stream_name, old=info.name, new=job.new_frozen_index)


def swap_data_stream_merged(client: Elasticsearch, job: JobRecord, source_indices: list[IndexInfo]) -> None:
    """ONE call for ALL data streams — no per-DS iteration."""
    job.transition(JobState.SWAPPING)
    final_name = job.new_frozen_index
    ds_groups: dict[str, list[IndexInfo]] = {}
    for idx in source_indices:
        if idx.is_data_stream_member and idx.data_stream_name:
            ds_groups.setdefault(idx.data_stream_name, []).append(idx)
    if not ds_groups:
        log.info("no_data_stream_swap_needed_merge")
        return
    _atomic_swap_multi_ds(client, ds_groups, final_name)
    for ds_name in ds_groups:
        log.info("data_stream_merged_swapped", ds=ds_name, n=len(ds_groups[ds_name]))


def _atomic_swap(client, ds_name, add_indices, remove_indices, max_retries=3):
    """Idempotent single-DS swap: add first, remove second, with retry.

    Backoff: 1s, 2s, 4s (2**attempt where attempt starts at 0).
    Three retries is enough for transient network blips.
    """
    for attempt in range(max_retries):
        ds_info = client.indices.get_data_stream(name=ds_name)
        current = set()
        for ds in ds_info.get("data_streams", []):
            if ds["name"] == ds_name:
                current = {b["index_name"] for b in ds.get("indices", [])}
                break
        actions = []
        for idx in add_indices:
            if idx not in current:
                actions.append({"add_backing_index": {"data_stream": ds_name, "index": idx}})
        for idx in remove_indices:
            if idx in current:
                actions.append({"remove_backing_index": {"data_stream": ds_name, "index": idx}})
        if not actions:
            return
        try:
            client.indices.modify_data_stream(body={"actions": actions})
            return
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log.warning("swap_retry", attempt=attempt + 1, error=str(e))
            time.sleep(2 ** attempt)


def _atomic_swap_multi_ds(client, ds_groups, final_name, max_retries=3):
    """Atomic swap across multiple data streams in a SINGLE API call.

    All add/remove actions for all data streams bundled into one request.
    Either all succeed or none do. Retries re-check membership.
    Backoff: 1s, 2s, 4s (2**attempt where attempt starts at 0).
    """
    for attempt in range(max_retries):
        current_by_ds = {}
        for ds_name in ds_groups:
            try:
                ds_info = client.indices.get_data_stream(name=ds_name)
                for ds in ds_info.get("data_streams", []):
                    if ds["name"] == ds_name:
                        current_by_ds[ds_name] = {b["index_name"] for b in ds.get("indices", [])}
                        break
                if ds_name not in current_by_ds:
                    current_by_ds[ds_name] = set()
            except Exception:
                current_by_ds[ds_name] = set()

        actions = []
        # Phase 1: Add merged index to every DS that needs it
        for ds_name in ds_groups:
            if final_name not in current_by_ds.get(ds_name, set()):
                actions.append({"add_backing_index": {"data_stream": ds_name, "index": final_name}})
        # Phase 2: Remove old source indices from every DS
        for ds_name, members in ds_groups.items():
            current = current_by_ds.get(ds_name, set())
            for m in members:
                if m.name in current:
                    actions.append({"remove_backing_index": {"data_stream": ds_name, "index": m.name}})

        if not actions:
            return
        try:
            client.indices.modify_data_stream(body={"actions": actions})
            return
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            log.warning("multi_ds_swap_retry", attempt=attempt + 1, error=str(e))
            time.sleep(2 ** attempt)
