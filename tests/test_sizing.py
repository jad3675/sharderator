"""Tests for index size tiers, ordering, and dynamic batching."""

from sharderator.engine.sizing import (
    SizeTier,
    classify_index,
    classify_merge_group,
    sort_indices,
    sort_merge_groups,
    prioritize_groups,
    compute_dynamic_batch,
    get_tier_profile,
)
from sharderator.engine.merge_analyzer import MergeGroup
from sharderator.models.index_info import IndexInfo

_GB = 1024 ** 3


def _idx(name: str, size_gb: float, shards: int = 1) -> IndexInfo:
    return IndexInfo(name=name, shard_count=shards, doc_count=100, store_size_bytes=int(size_gb * _GB))


class TestSizeTiers:
    def test_small(self):
        assert classify_index(_idx("a", 0.5)) == SizeTier.SMALL

    def test_medium(self):
        assert classify_index(_idx("a", 10)) == SizeTier.MEDIUM

    def test_large(self):
        assert classify_index(_idx("a", 100)) == SizeTier.LARGE

    def test_huge(self):
        assert classify_index(_idx("a", 600)) == SizeTier.HUGE

    def test_tier_profiles_exist(self):
        for tier in SizeTier:
            p = get_tier_profile(tier)
            assert p.recovery_timeout_minutes > 0


class TestOrdering:
    def test_smallest_first(self):
        indices = [_idx("big", 100), _idx("small", 1), _idx("mid", 50)]
        result = sort_indices(indices, "smallest-first")
        assert [i.name for i in result] == ["small", "mid", "big"]

    def test_largest_first(self):
        indices = [_idx("big", 100), _idx("small", 1), _idx("mid", 50)]
        result = sort_indices(indices, "largest-first")
        assert [i.name for i in result] == ["big", "mid", "small"]

    def test_as_is(self):
        indices = [_idx("b", 50), _idx("a", 10), _idx("c", 100)]
        result = sort_indices(indices, "as-is")
        assert [i.name for i in result] == ["b", "a", "c"]


class TestPriority:
    def test_priority_moves_to_front(self):
        g1 = MergeGroup(base_pattern="metrics-cpu", time_bucket="2026.02")
        g2 = MergeGroup(base_pattern="metrics-network", time_bucket="2026.02")
        g3 = MergeGroup(base_pattern="logs-app", time_bucket="2026.02")
        result = prioritize_groups([g1, g2, g3], ["logs"])
        assert result[0].base_pattern == "logs-app"

    def test_no_priority_unchanged(self):
        g1 = MergeGroup(base_pattern="a", time_bucket="2026.02")
        g2 = MergeGroup(base_pattern="b", time_bucket="2026.02")
        result = prioritize_groups([g1, g2], [])
        assert result == [g1, g2]


class TestDynamicBatch:
    def test_small_indices_batch_together(self):
        plan = [(_idx(f"i{i}", 0.1), f"r-{i}") for i in range(10)]
        batches = compute_dynamic_batch(plan, avail_bytes=100 * _GB, safety_margin=0.30, total_disk=200 * _GB)
        # 10 × 0.1 GB = 1 GB, budget = 100 - 60 = 40 GB — all fit in one batch
        assert len(batches) == 1
        assert len(batches[0]) == 10

    def test_large_index_gets_own_batch(self):
        plan = [
            (_idx("big", 50), "r-big"),
            (_idx("small1", 1), "r-s1"),
            (_idx("small2", 1), "r-s2"),
        ]
        # Budget = 100 - 60 = 40 GB. big=50 exceeds budget alone.
        batches = compute_dynamic_batch(plan, avail_bytes=100 * _GB, safety_margin=0.30, total_disk=200 * _GB)
        # big gets its own batch (exceeds budget but is first), smalls batch together
        assert len(batches) >= 2
