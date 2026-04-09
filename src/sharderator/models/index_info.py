"""Data classes for index metadata."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IndexInfo:
    """Metadata about a frozen-tier index."""

    name: str
    shard_count: int
    doc_count: int
    store_size_bytes: int
    snapshot_name: str = ""
    repository_name: str = ""
    original_index_name: str = ""
    ilm_policy: str = ""
    is_data_stream_member: bool = False
    data_stream_name: str = ""
    has_custom_routing: bool = False
    mappings: dict = field(default_factory=dict)
    settings: dict = field(default_factory=dict)
    target_shard_count: int = 1

    @property
    def store_size_mb(self) -> float:
        return self.store_size_bytes / (1024 * 1024)

    @property
    def shard_reduction(self) -> int:
        return self.shard_count - self.target_shard_count

    @property
    def needs_reindex(self) -> bool:
        """True if shrink API can't reach the target (target must be a factor of source)."""
        if self.target_shard_count == 0:
            return True
        return self.shard_count % self.target_shard_count != 0
