"""Index size tiers, pre-run sizing reports, and operation ordering."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from elasticsearch import Elasticsearch

from sharderator.client.queries import get_warm_hot_nodes
from sharderator.engine.merge_analyzer import MergeGroup
from sharderator.models.index_info import IndexInfo
from sharderator.util.logging import get_logger

log = get_logger(__name__)

_GB = 1024 ** 3


class SizeTier(Enum):
    SMALL = "SMALL"      # < 1 GB
    MEDIUM = "MEDIUM"    # 1 - 50 GB
    LARGE = "LARGE"      # 50 - 500 GB
    HUGE = "HUGE"        # > 500 GB


@dataclass
class TierProfile:
    """Operational profile for a size tier."""
    recovery_timeout_minutes: int
    reindex_rps: float  # -1 = unlimited


TIER_PROFILES: dict[SizeTier, TierProfile] = {
    SizeTier.SMALL:  TierProfile(recovery_timeout_minutes=10,  reindex_rps=-1),
    SizeTier.MEDIUM: TierProfile(recovery_timeout_minutes=30,  reindex_rps=10000),
    SizeTier.LARGE:  TierProfile(recovery_timeout_minutes=120, reindex_rps=5000),
    SizeTier.HUGE:   TierProfile(recovery_timeout_minutes=360, reindex_rps=2000),
}


def classify_index(info: IndexInfo) -> SizeTier:
    """Classify an index into a size tier based on store_size_bytes."""
    gb = info.store_size_bytes / _GB
    if gb < 1:
        return SizeTier.SMALL
    elif gb < 50:
        return SizeTier.MEDIUM
    elif gb < 500:
        return SizeTier.LARGE
    return SizeTier.HUGE


def classify_merge_group(group: MergeGroup) -> SizeTier:
    """Classify a merge group by its total source size."""
    total_gb = group.total_size_bytes / _GB
    if total_gb < 1:
        return SizeTier.SMALL
    elif total_gb < 50:
        return SizeTier.MEDIUM
    elif total_gb < 500:
        return SizeTier.LARGE
    return SizeTier.HUGE


def get_tier_profile(tier: SizeTier) -> TierProfile:
    return TIER_PROFILES[tier]


def sort_indices(
    indices: list[IndexInfo], order: str = "smallest-first"
) -> list[IndexInfo]:
    """Sort indices for processing order."""
    if order == "smallest-first":
        return sorted(indices, key=lambda i: i.store_size_bytes)
    elif order == "largest-first":
        return sorted(indices, key=lambda i: i.store_size_bytes, reverse=True)
    return list(indices)  # as-is


def sort_merge_groups(
    groups: list[MergeGroup], order: str = "smallest-first"
) -> list[MergeGroup]:
    """Sort merge groups for processing order."""
    if order == "smallest-first":
        return sorted(groups, key=lambda g: g.total_size_bytes)
    elif order == "largest-first":
        return sorted(groups, key=lambda g: g.total_size_bytes, reverse=True)
    return list(groups)


def prioritize_groups(
    groups: list[MergeGroup], priority_patterns: list[str]
) -> list[MergeGroup]:
    """Move groups matching priority patterns to the front."""
    if not priority_patterns:
        return groups
    priority = []
    rest = []
    for g in groups:
        if any(p in g.base_pattern for p in priority_patterns):
            priority.append(g)
        else:
            rest.append(g)
    return priority + rest


def compute_dynamic_batch(
    restore_plan: list[tuple[IndexInfo, str]],
    avail_bytes: int,
    safety_margin: float,
    total_disk: int,
) -> list[list[tuple[IndexInfo, str]]]:
    """Split restore plan into batches based on disk budget, not fixed count.

    Each batch fits within the available disk minus safety margin.
    """
    budget = avail_bytes - int(total_disk * safety_margin)
    if budget <= 0:
        return [restore_plan]  # let the disk gate catch it

    batches: list[list[tuple[IndexInfo, str]]] = []
    current_batch: list[tuple[IndexInfo, str]] = []
    current_size = 0

    for item in restore_plan:
        info, name = item
        idx_size = info.store_size_bytes
        if current_batch and current_size + idx_size > budget:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append(item)
        current_size += idx_size

    if current_batch:
        batches.append(current_batch)

    return batches


@dataclass
class SizingReport:
    """Pre-run sizing report for operator review."""
    mode: str
    group_count: int
    source_index_count: int
    total_source_bytes: int
    largest_index_name: str
    largest_index_bytes: int
    working_tier_nodes: int
    working_tier_total_bytes: int
    working_tier_free_bytes: int
    peak_disk_usage_bytes: int
    headroom_after_bytes: int
    headroom_pct: float
    safety_margin: float
    fits: bool
    current_frozen_shards: int
    frozen_shard_limit: int
    projected_shards_after: int
    shards_reclaimed: int

    def to_dict(self) -> dict:
        """Serialize to a dict suitable for JSON export."""
        import dataclasses
        return dataclasses.asdict(self)

    def format(self) -> str:
        lines = [
            "=== Sharderator Pre-Run Report ===",
            "",
            f"Mode:               {self.mode}",
            f"Groups:             {self.group_count}",
            f"Source indices:      {self.source_index_count:,}",
            f"Total source size:  {self.total_source_bytes / _GB:.1f} GB",
            f"Largest index:      {self.largest_index_name} ({self.largest_index_bytes / _GB:.1f} GB)",
            "",
            f"Working tier:       {self.working_tier_nodes} nodes, "
            f"{self.working_tier_total_bytes / _GB:.1f} GB total, "
            f"{self.working_tier_free_bytes / _GB:.1f} GB free",
            f"Peak disk usage:    ~{self.peak_disk_usage_bytes / _GB:.1f} GB",
            f"Headroom after:     {self.headroom_after_bytes / _GB:.1f} GB "
            f"({self.headroom_pct:.0%} free) — "
            f"{'above' if self.fits else 'BELOW'} {self.safety_margin:.0%} safety margin",
            "",
            "Shard budget:",
            f"  Current:          {self.current_frozen_shards:,} / {self.frozen_shard_limit:,}",
            f"  After completion: {self.projected_shards_after:,} / {self.frozen_shard_limit:,}",
            f"  Reclaimed:        {self.shards_reclaimed:,} shards",
        ]
        return "\n".join(lines)


def generate_sizing_report(
    client: Elasticsearch,
    mode: str,
    groups: list[MergeGroup] | None = None,
    indices: list[IndexInfo] | None = None,
    safety_margin: float = 0.30,
    frozen_shards: int = 0,
    frozen_limit: int = 3000,
) -> SizingReport:
    """Generate a pre-run sizing report."""
    nodes = get_warm_hot_nodes(client)
    total_disk = sum(int(n.get("disk.total", 0) or 0) for n in nodes)
    free_disk = sum(int(n.get("disk.avail", 0) or 0) for n in nodes)

    if groups:
        all_indices = [idx for g in groups for idx in g.source_indices]
        group_count = len(groups)
        total_shards_saved = sum(g.shard_reduction for g in groups)
        # Peak = largest group × 2 (sources + merged target)
        largest_group_bytes = max(g.total_size_bytes for g in groups) if groups else 0
        peak = largest_group_bytes * 2
    elif indices:
        all_indices = indices
        group_count = len(indices)
        total_shards_saved = sum(i.shard_reduction for i in indices)
        # Peak: restored copy + shrunk copy coexist on working tier before cleanup.
        # Worst case is ~2x the largest single index.
        peak = max(i.store_size_bytes for i in indices) * 2 if indices else 0
    else:
        all_indices = []
        group_count = 0
        total_shards_saved = 0
        peak = 0

    total_source = sum(i.store_size_bytes for i in all_indices)
    largest = max(all_indices, key=lambda i: i.store_size_bytes) if all_indices else None

    headroom = free_disk - peak
    headroom_pct = headroom / total_disk if total_disk > 0 else 0

    return SizingReport(
        mode=mode,
        group_count=group_count,
        source_index_count=len(all_indices),
        total_source_bytes=total_source,
        largest_index_name=largest.name if largest else "N/A",
        largest_index_bytes=largest.store_size_bytes if largest else 0,
        working_tier_nodes=len(nodes),
        working_tier_total_bytes=total_disk,
        working_tier_free_bytes=free_disk,
        peak_disk_usage_bytes=peak,
        headroom_after_bytes=headroom,
        headroom_pct=headroom_pct,
        safety_margin=safety_margin,
        fits=headroom_pct >= safety_margin,
        current_frozen_shards=frozen_shards,
        frozen_shard_limit=frozen_limit,
        projected_shards_after=frozen_shards - total_shards_saved,
        shards_reclaimed=total_shards_saved,
    )
