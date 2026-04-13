"""Tests for the defrag visualization widget v2 — per-shard cell grid."""

import pytest

from sharderator.gui.defrag_widget import DefragWidget, ShardCell, IndexGroup
from sharderator.models.index_info import IndexInfo


def _make_indices(count: int, shards: int = 5) -> list[IndexInfo]:
    return [
        IndexInfo(
            name=f"idx-{i}",
            shard_count=shards,
            doc_count=100,
            store_size_bytes=1000,
            target_shard_count=1,
        )
        for i in range(count)
    ]


@pytest.fixture
def widget(qtbot):
    w = DefragWidget()
    qtbot.addWidget(w)
    return w


def test_set_indices_creates_cells(widget):
    widget.set_indices(_make_indices(3, shards=5))
    assert len(widget._groups) == 3
    assert len(widget._all_cells) == 15  # 3 indices × 5 shards
    assert all(c.state == "PENDING" for c in widget._all_cells)


def test_set_indices_creates_group_map(widget):
    widget.set_indices(_make_indices(2))
    assert "idx-0" in widget._group_map
    assert "idx-1" in widget._group_map
    assert widget._group_map["idx-0"].shard_count == 5


def test_state_update_sets_group_state(widget):
    widget.set_indices(_make_indices(2))
    widget.update_state("idx-0", "RESTORING")
    assert widget._group_map["idx-0"].state == "RESTORING"
    assert widget._current_index == "idx-0"
    assert widget._current_state == "RESTORING"


def test_state_update_queues_sweep(widget):
    widget.set_indices(_make_indices(1, shards=4))
    widget.update_state("idx-0", "ANALYZING")
    # Sweep queue should have cells (some may have already been popped by timer)
    # But the group state should be set
    assert widget._group_map["idx-0"].state == "ANALYZING"


def test_completed_collapses_cells(widget):
    widget.set_indices(_make_indices(1, shards=7))
    widget.update_state("idx-0", "COMPLETED")
    group = widget._group_map["idx-0"]

    # First cell (target=1) should be COMPLETED
    assert group.cells[0].state == "COMPLETED"
    assert group.cells[0].collapsed is False

    # Remaining 6 cells should be FREED
    for cell in group.cells[1:]:
        assert cell.state == "FREED"
        assert cell.collapsed is True

    assert widget._completed_count == 1
    assert widget._total_shards_saved == 6  # 7 - 1


def test_multiple_completions(widget):
    widget.set_indices(_make_indices(3, shards=5))
    widget.update_state("idx-0", "COMPLETED")
    widget.update_state("idx-1", "COMPLETED")
    assert widget._completed_count == 2
    assert widget._total_shards_saved == 8  # (5-1) + (5-1)


def test_failed_does_not_count_as_completed(widget):
    widget.set_indices(_make_indices(1, shards=10))
    widget.update_state("idx-0", "FAILED")
    assert widget._completed_count == 0
    assert widget._total_shards_saved == 0
    # All cells should be FAILED
    assert all(c.state == "FAILED" for c in widget._group_map["idx-0"].cells)


def test_unknown_index_ignored(widget):
    widget.set_indices([])
    widget.update_state("nonexistent", "COMPLETED")
    assert widget._completed_count == 0


def test_progress_update(widget):
    widget.set_indices(_make_indices(1))
    widget.update_progress("idx-0", 67.5)
    assert widget._group_map["idx-0"].progress == 67.5
    assert widget._current_pct == 67.5


def test_progress_unknown_index(widget):
    widget.set_indices(_make_indices(1))
    widget.update_progress("nonexistent", 50.0)
    assert widget._group_map["idx-0"].progress == 0.0


def test_reset(widget):
    widget.set_indices(_make_indices(5))
    widget.update_state("idx-0", "COMPLETED")
    widget.reset()
    assert len(widget._groups) == 0
    assert len(widget._all_cells) == 0
    assert widget._completed_count == 0
    assert widget._total_shards_saved == 0
    assert widget._current_index == ""


def test_paint_empty(widget, qtbot):
    """Widget should render without error when empty."""
    widget.resize(400, 200)
    widget.repaint()


def test_paint_with_cells(widget, qtbot):
    """Widget should render without error with cells."""
    widget.set_indices(_make_indices(10, shards=8))
    widget.update_state("idx-3", "SHRINKING")
    # Flush the sweep queue so cells have the state
    while widget._sweep_queue:
        widget._tick_sweep()
    widget.resize(400, 200)
    widget.repaint()


def test_paint_with_completed_and_freed(widget, qtbot):
    """Widget should render completed + freed cells correctly."""
    widget.set_indices(_make_indices(3, shards=6))
    widget.update_state("idx-0", "COMPLETED")
    widget.update_state("idx-2", "FAILED")
    widget.resize(400, 200)
    widget.repaint()


def test_cell_count_matches_total_shards(widget):
    """Total cells should equal sum of all shard counts."""
    indices = [
        IndexInfo(name="a", shard_count=3, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="b", shard_count=7, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="c", shard_count=1, doc_count=0, store_size_bytes=0, target_shard_count=1),
    ]
    widget.set_indices(indices)
    assert len(widget._all_cells) == 11  # 3 + 7 + 1
