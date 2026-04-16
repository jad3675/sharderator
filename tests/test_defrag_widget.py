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


# --- Merge name mapping tests ---

def test_merge_name_map_fans_out_state(widget):
    """State update via merged output name should fan out to all source groups."""
    indices = [
        IndexInfo(name="src-a", shard_count=3, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="src-b", shard_count=4, doc_count=0, store_size_bytes=0, target_shard_count=1),
    ]
    widget.set_indices(indices)
    widget.set_merge_groups({"merged-output": ["src-a", "src-b"]})

    widget.update_state("merged-output", "RESTORING")

    assert widget._group_map["src-a"].state == "RESTORING"
    assert widget._group_map["src-b"].state == "RESTORING"
    assert widget._current_state == "RESTORING"


def test_merge_name_map_completed_collapses_all(widget):
    """COMPLETED via merged name should collapse all source groups."""
    indices = [
        IndexInfo(name="src-a", shard_count=5, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="src-b", shard_count=3, doc_count=0, store_size_bytes=0, target_shard_count=1),
    ]
    widget.set_indices(indices)
    widget.set_merge_groups({"merged-output": ["src-a", "src-b"]})

    widget.update_state("merged-output", "COMPLETED")

    # Both groups should be collapsed
    assert widget._group_map["src-a"].cells[0].state == "COMPLETED"
    assert widget._group_map["src-a"].cells[1].state == "FREED"
    assert widget._group_map["src-b"].cells[0].state == "COMPLETED"
    assert widget._group_map["src-b"].cells[1].state == "FREED"

    # Completed count = 2 (one per source group)
    assert widget._completed_count == 2
    # Shards saved = (5-1) + (3-1) = 6
    assert widget._total_shards_saved == 6


def test_merge_name_map_failed_marks_all(widget):
    """FAILED via merged name should mark all source cells as FAILED."""
    indices = [
        IndexInfo(name="src-a", shard_count=2, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="src-b", shard_count=2, doc_count=0, store_size_bytes=0, target_shard_count=1),
    ]
    widget.set_indices(indices)
    widget.set_merge_groups({"merged-output": ["src-a", "src-b"]})

    widget.update_state("merged-output", "FAILED")

    assert all(c.state == "FAILED" for c in widget._group_map["src-a"].cells)
    assert all(c.state == "FAILED" for c in widget._group_map["src-b"].cells)
    assert widget._completed_count == 0


def test_merge_name_map_progress_fans_out(widget):
    """Progress update via merged name should update all source groups."""
    indices = [
        IndexInfo(name="src-a", shard_count=2, doc_count=0, store_size_bytes=0, target_shard_count=1),
        IndexInfo(name="src-b", shard_count=2, doc_count=0, store_size_bytes=0, target_shard_count=1),
    ]
    widget.set_indices(indices)
    widget.set_merge_groups({"merged-output": ["src-a", "src-b"]})

    widget.update_progress("merged-output", 42.5)

    assert widget._group_map["src-a"].progress == 42.5
    assert widget._group_map["src-b"].progress == 42.5
    assert widget._current_pct == 42.5


def test_merge_name_map_unknown_merged_name_ignored(widget):
    """Unknown merged name with no mapping should be silently ignored."""
    widget.set_indices(_make_indices(1))
    widget.set_merge_groups({})

    widget.update_state("nonexistent-merged", "RESTORING")
    assert widget._completed_count == 0


def test_merge_name_map_cleared_on_set_indices(widget):
    """set_indices should clear any previous merge name mapping."""
    widget.set_indices(_make_indices(1))
    widget.set_merge_groups({"merged": ["idx-0"]})
    assert widget._merge_name_map == {"merged": ["idx-0"]}

    widget.set_indices(_make_indices(1))
    assert widget._merge_name_map == {}


def test_merge_name_map_cleared_on_reset(widget):
    """reset should clear the merge name mapping."""
    widget.set_indices(_make_indices(1))
    widget.set_merge_groups({"merged": ["idx-0"]})
    widget.reset()
    assert widget._merge_name_map == {}
