"""Tracker table model and view for the Job Tracker tab."""

from __future__ import annotations

import time

from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex
from PyQt6.QtGui import QAction, QColor
from PyQt6.QtWidgets import (
    QApplication,
    QHeaderView,
    QMenu,
    QTableView,
    QWidget,
)

from sharderator.models.job import JobRecord

COLUMNS = [
    "Index Name", "Type", "State", "Error", "Created", "Updated", "Expected Docs",
]

_STATE_COLORS = {
    "COMPLETED": QColor(200, 240, 200),
    "FAILED": QColor(255, 210, 210),
    "AWAITING_RECOVERY": QColor(255, 255, 210),
    "AWAITING_SHRINK": QColor(255, 255, 210),
    "AWAITING_REINDEX": QColor(255, 255, 210),
    "AWAITING_MERGE": QColor(255, 255, 210),
    "AWAITING_SNAPSHOT": QColor(255, 255, 210),
}


class TrackerTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._jobs: list[JobRecord] = []

    def set_jobs(self, jobs: list[JobRecord]) -> None:
        self.beginResetModel()
        self._jobs = jobs
        self.endResetModel()

    def get_job(self, row: int) -> JobRecord | None:
        if 0 <= row < len(self._jobs):
            return self._jobs[row]
        return None

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._jobs)

    def columnCount(self, parent=QModelIndex()) -> int:
        return len(COLUMNS)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return COLUMNS[section]
        return None

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        job = self._jobs[index.row()]
        col = index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            if col == 0:
                return job.index_name
            if col == 1:
                return job.job_type.value
            if col == 2:
                return job.state.value
            if col == 3:
                return job.error[:80] + "..." if len(job.error) > 80 else job.error
            if col == 4:
                return time.strftime("%Y-%m-%d %H:%M", time.localtime(job.created_at))
            if col == 5:
                return time.strftime("%Y-%m-%d %H:%M", time.localtime(job.updated_at))
            if col == 6:
                return f"{job.expected_doc_count:,}" if job.expected_doc_count else ""

        if role == Qt.ItemDataRole.BackgroundRole:
            return _STATE_COLORS.get(job.state.value)

        if role == Qt.ItemDataRole.ToolTipRole and col == 3:
            return job.error if job.error else None

        return None


class TrackerTableView(QTableView):
    """Table view with right-click context menu for job operations."""

    # Callbacks set by MainWindow
    on_resume: callable = None
    on_delete: callable = None
    on_view_history: callable = None
    on_export_job: callable = None

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self.setSortingEnabled(True)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_context_menu)

    def _show_context_menu(self, pos) -> None:
        index = self.indexAt(pos)
        if not index.isValid():
            return

        model = self.model()
        if not isinstance(model, TrackerTableModel):
            return
        job = model.get_job(index.row())
        if not job:
            return

        menu = QMenu(self)

        if job.state.value not in ("COMPLETED",):
            resume_action = QAction("Resume", self)
            resume_action.triggered.connect(lambda: self._do_resume(job))
            menu.addAction(resume_action)

        delete_action = QAction("Delete from Tracker", self)
        delete_action.triggered.connect(lambda: self._do_delete(job))
        menu.addAction(delete_action)

        if job.error:
            copy_err = QAction("Copy Error", self)
            copy_err.triggered.connect(
                lambda: QApplication.clipboard().setText(job.error)
            )
            menu.addAction(copy_err)

        history_action = QAction("View History", self)
        history_action.triggered.connect(lambda: self._do_history(job))
        menu.addAction(history_action)

        export_action = QAction("Export Job", self)
        export_action.triggered.connect(lambda: self._do_export_job(job))
        menu.addAction(export_action)

        menu.exec(self.viewport().mapToGlobal(pos))

    def _do_resume(self, job: JobRecord) -> None:
        if self.on_resume:
            self.on_resume(job)

    def _do_delete(self, job: JobRecord) -> None:
        if self.on_delete:
            self.on_delete(job)

    def _do_history(self, job: JobRecord) -> None:
        if self.on_view_history:
            self.on_view_history(job)

    def _do_export_job(self, job: JobRecord) -> None:
        if self.on_export_job:
            self.on_export_job(job)
