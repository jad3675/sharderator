"""Dialog showing a job's state transition timeline."""

from __future__ import annotations

import time

from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QPlainTextEdit,
    QVBoxLayout,
    QWidget,
)

from sharderator.models.job import JobRecord


class JobHistoryDialog(QDialog):
    def __init__(self, job: JobRecord, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle(f"Job History — {job.index_name}")
        self.setMinimumSize(500, 350)

        layout = QVBoxLayout(self)

        header = (
            f"Index: {job.index_name}\n"
            f"Type: {job.job_type.value}\n"
            f"Current State: {job.state.value}\n"
        )
        if job.error:
            header += f"Error: {job.error}\n"

        layout.addWidget(QLabel(header))

        text = QPlainTextEdit()
        text.setReadOnly(True)

        if job.history:
            for entry in job.history:
                ts = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(entry.get("timestamp", 0)),
                )
                from_state = entry.get("from", "?")
                to_state = entry.get("to", "?")
                line = f"{ts}  {from_state} → {to_state}"
                err = entry.get("error", "")
                if err:
                    line += f"  ⚠ {err}"
                text.appendPlainText(line)
        else:
            text.appendPlainText("No history recorded.")

        layout.addWidget(text)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
