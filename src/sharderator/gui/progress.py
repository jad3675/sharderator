"""Progress bar widget for index operations."""

from __future__ import annotations

from PyQt6.QtWidgets import QHBoxLayout, QLabel, QProgressBar, QWidget


class ProgressWidget(QWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self._bar = QProgressBar()
        self._bar.setRange(0, 100)
        self._bar.setValue(0)
        layout.addWidget(self._bar, stretch=1)

        self._label = QLabel("")
        layout.addWidget(self._label)

    def update_progress(self, index_name: str, pct: float) -> None:
        self._bar.setValue(int(pct))
        self._label.setText(f"{pct:.0f}%  {index_name}")

    def reset(self) -> None:
        self._bar.setValue(0)
        self._label.setText("")
