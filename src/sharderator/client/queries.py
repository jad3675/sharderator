"""Reusable Elasticsearch query helpers."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from sharderator.models.index_info import IndexInfo
from sharderator.util.logging import get_logger

log = get_logger(__name__)


def list_frozen_indices(client: Elasticsearch) -> list[IndexInfo]:
    """Return metadata for all frozen-tier (partially mounted) indices.

    Fix 2.1: Uses dataset.size instead of store.size. Frozen indices report
    near-zero local store since data lives in object storage. dataset.size
    reports the full logical size from the backing snapshot (available since ES 8.4).
    """
    indices = client.cat.indices(
        format="json",
        h="index,pri,docs.count,store.size,dataset.size",
        bytes="b",
    )
    frozen: list[IndexInfo] = []

    for idx in indices:
        name = idx.get("index", "")
        if not (name.startswith("partial-") or name.startswith(".partial-")):
            continue

        shard_count = int(idx.get("pri", 0))
        doc_count = int(idx.get("docs.count") or 0)
        # Prefer dataset.size for frozen indices, fall back to store.size
        store_bytes = int(idx.get("dataset.size") or idx.get("store.size") or 0)

        info = IndexInfo(
            name=name,
            shard_count=shard_count,
            doc_count=doc_count,
            store_size_bytes=store_bytes,
        )
        frozen.append(info)

    # Sort by shard count descending — worst offenders first
    frozen.sort(key=lambda i: i.shard_count, reverse=True)
    return frozen


def get_data_stream_membership(client: Elasticsearch) -> dict[str, str]:
    """Return a map of index_name -> data_stream_name for all backing indices."""
    membership: dict[str, str] = {}
    try:
        ds_resp = client.indices.get_data_stream(name="*")
        for ds in ds_resp.get("data_streams", []):
            for backing in ds.get("indices", []):
                membership[backing["index_name"]] = ds["name"]
    except Exception:
        pass
    return membership


def enrich_index_info(
    client: Elasticsearch,
    info: IndexInfo,
    ds_membership: dict[str, str] | None = None,
) -> IndexInfo:
    """Fill in snapshot source, mappings, ILM, routing details for an index."""
    settings_resp = client.indices.get_settings(index=info.name)
    idx_settings = settings_resp[info.name]["settings"]["index"]

    snapshot_meta = idx_settings.get("store", {}).get("snapshot", {})
    info.snapshot_name = snapshot_meta.get("snapshot_name", "")
    info.repository_name = snapshot_meta.get("repository_name", "")
    info.original_index_name = snapshot_meta.get("index_name", info.name)

    info.ilm_policy = idx_settings.get("lifecycle", {}).get("name", "")

    # Mappings
    mapping_resp = client.indices.get_mapping(index=info.name)
    info.mappings = mapping_resp[info.name]["mappings"]

    # Check for custom routing
    routing_meta = info.mappings.get("_routing", {})
    info.has_custom_routing = routing_meta.get("required", False)

    # Data stream membership — use cached map if available
    if ds_membership is not None:
        if info.name in ds_membership:
            info.is_data_stream_member = True
            info.data_stream_name = ds_membership[info.name]
    else:
        try:
            ds_resp = client.indices.get_data_stream(name="*")
            for ds in ds_resp.get("data_streams", []):
                backing = [b["index_name"] for b in ds.get("indices", [])]
                if info.name in backing:
                    info.is_data_stream_member = True
                    info.data_stream_name = ds["name"]
                    break
        except Exception:
            pass

    # Target shard count
    if info.doc_count > 2_000_000_000:
        info.target_shard_count = (info.doc_count // 2_000_000_000) + 1
    else:
        info.target_shard_count = 1

    info.settings = idx_settings
    return info


def get_disk_allocation(client: Elasticsearch) -> list[dict]:
    """Return per-node disk allocation info."""
    return client.cat.allocation(format="json", bytes="b")


def get_warm_hot_nodes(client: Elasticsearch) -> list[dict]:
    """Return warm/hot/content nodes with disk info."""
    nodes = client.cat.nodes(format="json", h="name,disk.avail,disk.total,node.role", bytes="b")
    return [
        n
        for n in nodes
        if isinstance(n, dict)
        and any(role in (n.get("node.role") or "") for role in ("h", "w", "s"))
    ]
