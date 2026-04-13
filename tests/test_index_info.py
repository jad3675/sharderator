"""Tests for IndexInfo data class properties."""

from sharderator.models.index_info import IndexInfo


def test_store_size_mb():
    info = IndexInfo(name="x", shard_count=1, doc_count=0, store_size_bytes=1024 * 1024 * 500)
    assert info.store_size_mb == 500.0


def test_shard_reduction():
    info = IndexInfo(name="x", shard_count=15, doc_count=0, store_size_bytes=0, target_shard_count=1)
    assert info.shard_reduction == 14


def test_shard_reduction_already_at_target():
    info = IndexInfo(name="x", shard_count=1, doc_count=0, store_size_bytes=0, target_shard_count=1)
    assert info.shard_reduction == 0


def test_needs_reindex_false_when_factor():
    # 15 shards -> 1: 15 % 1 == 0, shrink works
    info = IndexInfo(name="x", shard_count=15, doc_count=0, store_size_bytes=0, target_shard_count=1)
    assert info.needs_reindex is False

    # 15 shards -> 5: 15 % 5 == 0, shrink works
    info2 = IndexInfo(name="x", shard_count=15, doc_count=0, store_size_bytes=0, target_shard_count=5)
    assert info2.needs_reindex is False


def test_needs_reindex_true_when_not_factor():
    # 7 shards -> 3: 7 % 3 != 0, need reindex
    info = IndexInfo(name="x", shard_count=7, doc_count=0, store_size_bytes=0, target_shard_count=3)
    assert info.needs_reindex is True


def test_needs_reindex_zero_target():
    info = IndexInfo(name="x", shard_count=5, doc_count=0, store_size_bytes=0, target_shard_count=0)
    assert info.needs_reindex is True
