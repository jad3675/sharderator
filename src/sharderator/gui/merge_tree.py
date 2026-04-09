"""Merge candidates tree view for v2 multi-index merge mode."""

from __future__ import annotations

from typing import Callable

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import QTreeWidget, QTreeWidgetItem, QWidget

from sharderator.engine.merge_analyzer import MergeGroup, MergeGroupValidation

# Store MergeGroup key on child items
_GROUP_ROLE = Qt.ItemDataRole.UserRole
# Track whether a child has been validated already
_VALIDATED_ROLE = Qt.ItemDataRole.UserRole + 1


class MergeTreeWidget(QTreeWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setHeaderLabels(["Merge Group", "Indices", "Shards Saved", "Size (MB)"])
        self.setColumnCount(4)
        self.setAlternatingRowColors(True)
        self._groups: list[MergeGroup] = []
        self._validator: Callable[[MergeGroup], MergeGroupValidation] | None = None
        self._log_callback: Callable[[str], None] | None = None
        self.itemChanged.connect(self._on_item_changed)

    def set_validator(
        self,
        validator: Callable[[MergeGroup], MergeGroupValidation],
        log_callback: Callable[[str], None] | None = None,
    ) -> None:
        """Set the lazy validation function called when a group is checked."""
        self._validator = validator
        self._log_callback = log_callback

    def set_groups(self, groups: list[MergeGroup]) -> None:
        self.blockSignals(True)
        self._groups = groups
        self.clear()

        patterns: dict[str, list[MergeGroup]] = {}
        for g in groups:
            patterns.setdefault(g.base_pattern, []).append(g)

        for pattern, buckets in sorted(patterns.items()):
            parent = QTreeWidgetItem(self)
            parent.setText(0, pattern)
            total_indices = sum(len(g.source_indices) for g in buckets)
            total_saved = sum(g.shard_reduction for g in buckets)
            parent.setText(1, str(total_indices))
            parent.setText(2, str(total_saved))
            parent.setFlags(parent.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            parent.setCheckState(0, Qt.CheckState.Unchecked)

            for g in sorted(buckets, key=lambda x: x.time_bucket):
                child = QTreeWidgetItem(parent)
                child.setText(0, f"  {g.time_bucket}")
                child.setText(1, f"{len(g.source_indices)} → 1")
                child.setText(2, str(g.shard_reduction))
                child.setText(3, f"{g.total_size_mb:.1f}")
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, Qt.CheckState.Unchecked)
                child.setData(0, _GROUP_ROLE, f"{g.base_pattern}|{g.time_bucket}")
                child.setData(0, _VALIDATED_ROLE, False)

        self.expandAll()
        for col in range(self.columnCount()):
            self.resizeColumnToContents(col)
        self.blockSignals(False)

    def _on_item_changed(self, item: QTreeWidgetItem, column: int) -> None:
        """Propagate check state and run lazy validation on newly checked groups."""
        if column != 0:
            return

        self.blockSignals(True)

        if item.childCount() > 0:
            # Parent toggled — apply to all children
            state = item.checkState(0)
            for i in range(item.childCount()):
                child = item.child(i)
                # Only propagate to checkable children (non-viable ones are disabled)
                if child.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                    child.setCheckState(0, state)
                    if state == Qt.CheckState.Checked:
                        self._lazy_validate(child)
        else:
            # Child toggled
            if item.checkState(0) == Qt.CheckState.Checked:
                self._lazy_validate(item)

            # Update parent state
            parent = item.parent()
            if parent:
                checked = sum(
                    1
                    for i in range(parent.childCount())
                    if parent.child(i).checkState(0) == Qt.CheckState.Checked
                )
                if checked == 0:
                    parent.setCheckState(0, Qt.CheckState.Unchecked)
                elif checked == parent.childCount():
                    parent.setCheckState(0, Qt.CheckState.Checked)
                else:
                    parent.setCheckState(0, Qt.CheckState.PartiallyChecked)

        self.blockSignals(False)

    def _lazy_validate(self, child: QTreeWidgetItem) -> None:
        """Validate a group when it's first checked. Only runs once per group."""
        if child.data(0, _VALIDATED_ROLE):
            return
        if not self._validator:
            return

        group_key = child.data(0, _GROUP_ROLE)
        by_key = {f"{g.base_pattern}|{g.time_bucket}": g for g in self._groups}
        group = by_key.get(group_key)
        if not group:
            return

        child.setData(0, _VALIDATED_ROLE, True)
        v = self._validator(group)

        if not v.is_viable:
            child.setCheckState(0, Qt.CheckState.Unchecked)
            child.setFlags(child.flags() & ~Qt.ItemFlag.ItemIsUserCheckable)
            child.setForeground(0, QColor(180, 180, 180))
            child.setToolTip(
                0, f"Not viable: {', '.join(v.missing_snapshots[:3])}"
            )
            if self._log_callback:
                self._log_callback(
                    f"⚠ {group.base_pattern}/{group.time_bucket}: not viable "
                    f"({len(v.missing_snapshots)} missing snapshots)"
                )
        elif v.mapping_conflicts:
            child.setForeground(0, QColor(180, 140, 0))
            child.setToolTip(
                0, f"Mapping drift: {'; '.join(v.mapping_conflicts)}"
            )
            if self._log_callback:
                self._log_callback(
                    f"⚠ {group.base_pattern}/{group.time_bucket}: {v.mapping_conflicts[0]}"
                )

    def get_selected_groups(self) -> list[MergeGroup]:
        """Return merge groups whose tree items are checked."""
        by_key = {f"{g.base_pattern}|{g.time_bucket}": g for g in self._groups}
        selected = []

        for pattern_idx in range(self.topLevelItemCount()):
            parent = self.topLevelItem(pattern_idx)
            for child_idx in range(parent.childCount()):
                child = parent.child(child_idx)
                if child.checkState(0) == Qt.CheckState.Checked:
                    group_key = child.data(0, _GROUP_ROLE)
                    if group_key in by_key:
                        selected.append(by_key[group_key])

        return selected

    def get_summary(self) -> tuple[int, int, int]:
        """Return (total_source_indices, total_merged, total_shards_saved) for checked groups."""
        groups = self.get_selected_groups()
        total_sources = sum(len(g.source_indices) for g in groups)
        total_merged = len(groups)
        total_saved = sum(g.shard_reduction for g in groups)
        return total_sources, total_merged, total_saved
