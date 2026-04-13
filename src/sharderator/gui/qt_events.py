"""Qt adapter for PipelineEvents — bridges engine callbacks to Qt signals."""

from __future__ import annotations

from PyQt6.QtCore import QObject, pyqtSignal

from sharderator.engine.events import PipelineEvents


class QtPipelineEvents(QObject):
    """Implements PipelineEvents by emitting Qt signals.

    Used by the GUI worker threads to forward engine events to the main thread.
    """

    progress = pyqtSignal(str, float)
    state_changed = pyqtSignal(str, str)
    log_message = pyqtSignal(str)
    completed = pyqtSignal(str)
    failed = pyqtSignal(str, str)

    def on_progress(self, index_name: str, pct: float) -> None:
        self.progress.emit(index_name, pct)

    def on_state_changed(self, index_name: str, state: str) -> None:
        self.state_changed.emit(index_name, state)

    def on_log(self, message: str) -> None:
        self.log_message.emit(message)

    def on_completed(self, index_name: str) -> None:
        self.completed.emit(index_name)

    def on_failed(self, index_name: str, error: str) -> None:
        self.failed.emit(index_name, error)
