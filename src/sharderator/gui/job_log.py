"""Job log viewer widget."""

from __future__ import annotations

import time

from PyQt6.QtWidgets import QPlainTextEdit, QWidget


class JobLog(QPlainTextEdit):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setMaximumBlockCount(5000)

    def append_log(self, message: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self.appendPlainText(f"{ts} {message}")
        self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())
