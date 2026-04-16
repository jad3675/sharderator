"""Frozen index table model and view with filter proxy."""

from __future__ import annotations

import fnmatch

from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import QTableView, QHeaderView

from sharderator.models.index_info import IndexInfo

COLUMNS = ["", "Index Name", "Shards", "Size (MB)", "Docs", "Target", "Data Stream", "Routing"]


class IndexTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._indices: list[IndexInfo] = []
        self._checked: set[int] = set()

    def set_indices(self, indices: list[IndexInfo]) -> None:
        self.beginResetModel()
        self._indices = indices
        self._checked.clear()
        self.endResetModel()

    def get_selected(self) -> list[IndexInfo]:
        return [self._indices[i] for i in sorted(self._checked)]

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._indices)

    def columnCount(self, parent=QModelIndex()) -> int:
        return len(COLUMNS)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return COLUMNS[section]
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        flags = super().flags(index)
        if index.column() == 0:
            flags |= Qt.ItemFlag.ItemIsUserCheckable
        return flags

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        row = index.row()
        col = index.column()
        info = self._indices[row]

        if role == Qt.ItemDataRole.CheckStateRole and col == 0:
            return (
                Qt.CheckState.Checked if row in self._checked else Qt.CheckState.Unchecked
            )

        if role == Qt.ItemDataRole.DisplayRole:
            if col == 1:
                return info.name
            if col == 2:
                return str(info.shard_count)
            if col == 3:
                return f"{info.store_size_mb:.1f}"
            if col == 4:
                return f"{info.doc_count:,}"
            if col == 5:
                # Fix 2.7: Show "—" when there's nothing to shrink
                if info.shard_count <= info.target_shard_count:
                    return "—"
                return f"→{info.target_shard_count}"
            if col == 6:
                return info.data_stream_name or ""
            if col == 7:
                return "⚠" if info.has_custom_routing else ""

        # Fix 2.7: Grey out rows where shard count already matches target
        if role == Qt.ItemDataRole.BackgroundRole:
            if info.shard_count <= info.target_shard_count:
                return QColor(240, 240, 240)

        return None

    def setData(self, index: QModelIndex, value, role=Qt.ItemDataRole.EditRole) -> bool:
        if role == Qt.ItemDataRole.CheckStateRole and index.column() == 0:
            if value == Qt.CheckState.Checked.value:
                self._checked.add(index.row())
            else:
                self._checked.discard(index.row())
            self.dataChanged.emit(index, index)
            return True
        return False

    def select_visible(self, source_rows: list[int]) -> None:
        """Check the given source-model rows (additive — doesn't uncheck others)."""
        for row in source_rows:
            if 0 <= row < len(self._indices):
                self._checked.add(row)
        if source_rows:
            top = self.index(min(source_rows), 0)
            bottom = self.index(max(source_rows), 0)
            self.dataChanged.emit(top, bottom)

    def deselect_visible(self, source_rows: list[int]) -> None:
        """Uncheck the given source-model rows."""
        for row in source_rows:
            self._checked.discard(row)
        if source_rows:
            top = self.index(min(source_rows), 0)
            bottom = self.index(max(source_rows), 0)
            self.dataChanged.emit(top, bottom)

    def get_checked_names(self) -> set[str]:
        """Return the set of checked index names (for persistence across reloads)."""
        return {self._indices[i].name for i in self._checked if i < len(self._indices)}

    def restore_checked_names(self, names: set[str]) -> None:
        """Re-check rows whose index name is in the given set."""
        self._checked.clear()
        for i, info in enumerate(self._indices):
            if info.name in names:
                self._checked.add(i)
        if self._indices:
            self.dataChanged.emit(self.index(0, 0), self.index(len(self._indices) - 1, 0))


class IndexFilterProxy(QSortFilterProxyModel):
    """Filter proxy for the frozen index table. Matches against index name (column 1).

    Supports glob wildcards (*). If no wildcards are present, treats the
    filter text as a substring match (implicitly *text*).
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_text: str = ""

    def set_filter_text(self, text: str) -> None:
        """Set the filter pattern and re-filter."""
        self._filter_text = text.strip().lower()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        if not self._filter_text:
            return True
        model = self.sourceModel()
        if model is None:
            return True
        idx = model.index(source_row, 1)  # column 1 = Index Name
        name = (model.data(idx, Qt.ItemDataRole.DisplayRole) or "").lower()
        pattern = self._filter_text
        if "*" in pattern or "?" in pattern:
            return fnmatch.fnmatch(name, pattern)
        return pattern in name

    def visible_source_rows(self) -> list[int]:
        """Return the source-model row indices for all currently visible rows."""
        rows = []
        for proxy_row in range(self.rowCount()):
            source_idx = self.mapToSource(self.index(proxy_row, 0))
            rows.append(source_idx.row())
        return rows


class IndexTableView(QTableView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.setSortingEnabled(True)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
