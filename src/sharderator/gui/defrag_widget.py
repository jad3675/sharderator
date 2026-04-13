"""Defrag visualization widget v2 — per-shard cell grid.

Each shard in every index gets its own 10x10 square cell. Cells flow in a
continuous grid like a disk map. State transitions sweep left-to-right with
a staggered animation. Completed indices collapse: target cells turn green,
excess cells go dark ("freed"), showing the shard budget being reclaimed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from PyQt6.QtCore import Qt, QRect, QTimer
from PyQt6.QtGui import QColor, QPainter, QPen
from PyQt6.QtWidgets import QSizePolicy, QToolTip, QWidget

from sharderator.models.index_info import IndexInfo

STATE_COLORS: dict[str, QColor] = {
    "PENDING": QColor(0x9E, 0x9E, 0x9E),
    "ANALYZING": QColor(0x64, 0xB5, 0xF6),
    "RESTORING": QColor(0x1E, 0x88, 0xE5),
    "AWAITING_RECOVERY": QColor(0x1E, 0x88, 0xE5),
    "SHRINKING": QColor(0xFF, 0xA7, 0x26),
    "AWAITING_SHRINK": QColor(0xFF, 0xA7, 0x26),
    "AWAITING_REINDEX": QColor(0xFF, 0xA7, 0x26),
    "MERGING": QColor(0xFF, 0xA7, 0x26),
    "AWAITING_MERGE": QColor(0xFF, 0xA7, 0x26),
    "FORCE_MERGING": QColor(0xFF, 0xB3, 0x00),
    "SNAPSHOTTING": QColor(0x26, 0xC6, 0xDA),
    "AWAITING_SNAPSHOT": QColor(0x26, 0xC6, 0xDA),
    "REMOUNTING": QColor(0x00, 0x89, 0x7B),
    "VERIFYING": QColor(0x00, 0x89, 0x7B),
    "SWAPPING": QColor(0x00, 0x89, 0x7B),
    "CLEANING_UP": QColor(0x66, 0xBB, 0x6A),
    "COMPLETED": QColor(0x43, 0xA0, 0x47),
    "FAILED": QColor(0xE5, 0x39, 0x35),
    "FREED": QColor(0x2A, 0x2A, 0x2A),
}

PULSING_STATES = {
    "AWAITING_RECOVERY", "AWAITING_SHRINK", "AWAITING_REINDEX",
    "AWAITING_MERGE", "AWAITING_SNAPSHOT",
}

LEGEND_ITEMS = [
    ("Pending", "PENDING"), ("Restoring", "RESTORING"),
    ("Shrinking", "SHRINKING"), ("Snapshotting", "SNAPSHOTTING"),
    ("Verifying", "VERIFYING"), ("Completed", "COMPLETED"),
    ("Freed", "FREED"), ("Failed", "FAILED"),
]

CELL_SIZE = 10
CELL_GAP = 1


@dataclass
class ShardCell:
    """One cell in the defrag grid = one shard."""
    index_name: str
    shard_index: int
    shard_count: int
    target_shard_count: int
    state: str = "PENDING"
    collapsed: bool = False


@dataclass
class IndexGroup:
    """Metadata for a parent index — tracks state and owns its cells."""
    index_name: str
    shard_count: int
    target_shard_count: int
    state: str = "PENDING"
    progress: float = 0.0
    cells: list[ShardCell] = field(default_factory=list)


class DefragWidget(QWidget):
    """Per-shard cell grid visualization inspired by the classic defrag utility."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._groups: list[IndexGroup] = []
        self._group_map: dict[str, IndexGroup] = {}
        self._all_cells: list[ShardCell] = []
        self._cell_rects: list[tuple[QRect, ShardCell]] = []
        self._completed_count: int = 0
        self._total_shards_saved: int = 0
        self._current_index: str = ""
        self._current_state: str = ""
        self._current_pct: float = 0.0
        self._pulse_phase: float = 0.0
        self._sweep_queue: list[ShardCell] = []
        self._sweep_state: str = ""

        self.setMinimumHeight(200)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)
        self.setMouseTracking(True)

        self._pulse_timer = QTimer(self)
        self._pulse_timer.setInterval(50)
        self._pulse_timer.timeout.connect(self._tick_pulse)

        self._sweep_timer = QTimer(self)
        self._sweep_timer.setInterval(30)
        self._sweep_timer.timeout.connect(self._tick_sweep)

    def set_indices(self, indices: list[IndexInfo]) -> None:
        """Initialize the grid — one cell per shard across all indices."""
        self._groups = []
        self._group_map = {}
        self._all_cells = []
        self._cell_rects = []
        self._completed_count = 0
        self._total_shards_saved = 0
        self._current_index = ""
        self._current_state = ""
        self._current_pct = 0.0
        self._sweep_queue = []

        for info in indices:
            cells = [
                ShardCell(
                    index_name=info.name,
                    shard_index=i,
                    shard_count=info.shard_count,
                    target_shard_count=info.target_shard_count,
                )
                for i in range(info.shard_count)
            ]
            group = IndexGroup(
                index_name=info.name,
                shard_count=info.shard_count,
                target_shard_count=info.target_shard_count,
                cells=cells,
            )
            self._groups.append(group)
            self._group_map[info.name] = group
            self._all_cells.extend(cells)

        self._pulse_timer.start()
        self.update()

    def update_state(self, index_name: str, state: str) -> None:
        """Called when state_changed signal fires. Triggers sweep animation."""
        group = self._group_map.get(index_name)
        if not group:
            return
        group.state = state
        self._current_index = index_name
        self._current_state = state

        if state == "COMPLETED":
            self._completed_count += 1
            self._total_shards_saved += group.shard_count - group.target_shard_count
            self._collapse_group(group)
            self.update()
        elif state == "FAILED":
            # Instantly mark all cells as failed — no sweep
            for cell in group.cells:
                cell.state = "FAILED"
            self.update()
        else:
            # Staggered sweep: queue cells for sequential state update
            self._sweep_queue = list(group.cells)
            self._sweep_state = state
            if not self._sweep_timer.isActive():
                self._sweep_timer.start()

    def update_progress(self, index_name: str, pct: float) -> None:
        """Called when progress signal fires."""
        group = self._group_map.get(index_name)
        if group:
            group.progress = pct
        self._current_pct = pct
        self.update()

    def reset(self) -> None:
        """Clear the grid."""
        self._groups.clear()
        self._group_map.clear()
        self._all_cells.clear()
        self._cell_rects.clear()
        self._sweep_queue.clear()
        self._pulse_timer.stop()
        self._sweep_timer.stop()
        self._completed_count = 0
        self._total_shards_saved = 0
        self._current_index = ""
        self.update()

    def _collapse_group(self, group: IndexGroup) -> None:
        """Mark excess cells as freed after completion."""
        for i, cell in enumerate(group.cells):
            if i < group.target_shard_count:
                cell.state = "COMPLETED"
                cell.collapsed = False
            else:
                cell.state = "FREED"
                cell.collapsed = True

    def _tick_sweep(self) -> None:
        """Advance the sweep animation by one cell."""
        if not self._sweep_queue:
            self._sweep_timer.stop()
            return
        cell = self._sweep_queue.pop(0)
        cell.state = self._sweep_state
        self.update()

    def _tick_pulse(self) -> None:
        self._pulse_phase += 0.12
        if self._pulse_phase > math.pi * 2:
            self._pulse_phase -= math.pi * 2
        if any(c.state in PULSING_STATES for c in self._all_cells):
            self.update()

    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)

        rect = self.rect()
        grid_rect = QRect(rect.x() + 4, rect.y() + 4, rect.width() - 8, rect.height() - 52)
        status_rect = QRect(rect.x() + 4, grid_rect.bottom() + 4, rect.width() - 8, 20)
        legend_rect = QRect(rect.x() + 4, status_rect.bottom() + 2, rect.width() - 8, 16)

        # Detect theme
        bg = self.palette().color(self.backgroundRole())
        text_color = QColor(200, 200, 200) if bg.lightness() < 128 else QColor(80, 80, 80)

        if not self._all_cells:
            painter.setPen(QColor(160, 160, 160))
            painter.drawText(grid_rect, Qt.AlignmentFlag.AlignCenter, "No operation running")
            painter.end()
            return

        # Grid layout — fixed-size square cells, wrapping
        self._cell_rects.clear()
        stride = CELL_SIZE + CELL_GAP
        cols = max(1, grid_rect.width() // stride)

        for i, cell in enumerate(self._all_cells):
            col = i % cols
            row = i // cols
            x = grid_rect.x() + col * stride
            y = grid_rect.y() + row * stride

            if y + CELL_SIZE > grid_rect.bottom():
                break

            color = QColor(STATE_COLORS.get(cell.state, STATE_COLORS["PENDING"]))

            if cell.state in PULSING_STATES:
                alpha = int(160 + 95 * math.sin(self._pulse_phase))
                color.setAlpha(alpha)

            cell_rect = QRect(x, y, CELL_SIZE, CELL_SIZE)
            painter.fillRect(cell_rect, color)
            self._cell_rects.append((cell_rect, cell))

        # Highlight + border on active index cells
        if self._current_index:
            group = self._group_map.get(self._current_index)
            if group and group.state not in ("COMPLETED", "FAILED", "PENDING"):
                for cell_rect, cell in self._cell_rects:
                    if cell.index_name == self._current_index and not cell.collapsed:
                        painter.fillRect(cell_rect, QColor(255, 255, 255, 50))
                painter.setPen(QPen(QColor(255, 255, 255, 140), 1))
                for cell_rect, cell in self._cell_rects:
                    if cell.index_name == self._current_index and not cell.collapsed:
                        painter.drawRect(cell_rect)

        # Scan-line highlight on next sweep cell
        if self._sweep_queue:
            next_cell = self._sweep_queue[0]
            for cell_rect, cell in self._cell_rects:
                if cell is next_cell:
                    painter.fillRect(cell_rect, QColor(255, 255, 255, 80))
                    break

        # Status text
        painter.setPen(text_color)
        font = painter.font()
        font.setPointSize(9)
        painter.setFont(font)

        if self._current_index:
            left = f"{self._current_index}  [{self._current_state}]  {self._current_pct:.0f}%"
        else:
            left = "Ready"
        right = f"Processed: {self._completed_count}/{len(self._groups)}   Shards saved: {self._total_shards_saved}"

        painter.drawText(status_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, left)
        painter.drawText(status_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, right)

        # Legend bar
        font.setPointSize(7)
        painter.setFont(font)
        lx = legend_rect.x()
        ly = legend_rect.y()
        fm = painter.fontMetrics()

        for label, state in LEGEND_ITEMS:
            color = STATE_COLORS.get(state, QColor(128, 128, 128))
            painter.fillRect(lx, ly + 2, 8, 8, color)
            painter.setPen(text_color)
            painter.drawText(lx + 11, ly + 10, label)
            lx += fm.horizontalAdvance(label) + 20
            if lx > legend_rect.right() - 40:
                break

        painter.end()

    def mouseMoveEvent(self, event) -> None:
        pos = event.pos()
        for rect, cell in self._cell_rects:
            if rect.contains(pos):
                group = self._group_map.get(cell.index_name)
                state = group.state if group else cell.state
                progress = group.progress if group else 0
                QToolTip.showText(
                    event.globalPosition().toPoint(),
                    f"{cell.index_name}\n"
                    f"Shard: {cell.shard_index + 1}/{cell.shard_count}\n"
                    f"State: {state}\n"
                    f"Target: {cell.shard_count} → {cell.target_shard_count}\n"
                    f"Progress: {progress:.0f}%",
                )
                return
        QToolTip.hideText()
