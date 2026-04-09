"""Frozen index table model and view."""

from __future__ import annotations

from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex
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
