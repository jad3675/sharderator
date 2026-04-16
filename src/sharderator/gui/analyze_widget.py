"""Frozen Analyze tab — visual frozen tier health report."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QProgressBar,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from sharderator.engine.frozen_analyzer import FrozenAnalysis


class BudgetBar(QProgressBar):
    """Shard budget usage bar with color coding."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setMinimum(0)
        self.setMaximum(1000)  # Use 1000 for finer granularity
        self.setTextVisible(True)
        self.setMinimumHeight(32)
        self.setMaximumHeight(32)
        self._update_style(0)

    def set_budget(self, used: int, limit: int) -> None:
        pct = used / limit * 100 if limit else 0
        self.setValue(min(int(pct * 10), 1000))
        self.setFormat(f"  {used:,} / {limit:,} shards  ({pct:.1f}%)  ")
        self._update_style(pct)

    def _update_style(self, pct: float) -> None:
        if pct >= 100:
            color, bg = "#e53935", "#4a1515"
        elif pct >= 90:
            color, bg = "#ff9800", "#3d2e10"
        elif pct >= 75:
            color, bg = "#ffc107", "#3d3510"
        else:
            color, bg = "#43a047", "#1a2e1a"
        self.setStyleSheet(
            f"QProgressBar {{ "
            f"  border: 1px solid #666; border-radius: 4px; "
            f"  background-color: {bg}; "
            f"  text-align: center; font-weight: bold; font-size: 13px; "
            f"  color: #eee; "
            f"}}"
            f"QProgressBar::chunk {{ "
            f"  background-color: {color}; border-radius: 3px; "
            f"}}"
        )


class SummaryCard(QFrame):
    """A bordered summary card with a title and detail text."""

    def __init__(self, title: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.Box)
        self.setMinimumHeight(56)
        self.setMaximumHeight(72)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 4, 10, 4)
        layout.setSpacing(2)

        self._lbl_title = QLabel(f"<b>{title}</b>")
        self._lbl_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._lbl_title)

        self._lbl_detail = QLabel("")
        self._lbl_detail.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._lbl_detail)

    def set_detail(self, text: str) -> None:
        self._lbl_detail.setText(text)


class AnalyzeWidget(QWidget):
    """Frozen tier analysis tab showing shard budget, categories, and recommendations."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._analysis: FrozenAnalysis | None = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        # Budget bar row
        budget_row = QHBoxLayout()
        budget_row.setSpacing(8)
        lbl = QLabel("Frozen Shard Budget:")
        font = lbl.font()
        font.setPointSize(10)
        font.setBold(True)
        lbl.setFont(font)
        budget_row.addWidget(lbl)
        self._budget_bar = BudgetBar()
        budget_row.addWidget(self._budget_bar, stretch=1)
        self._lbl_status = QLabel("")
        self._lbl_status.setMinimumWidth(80)
        status_font = QFont()
        status_font.setPointSize(12)
        status_font.setBold(True)
        self._lbl_status.setFont(status_font)
        budget_row.addWidget(self._lbl_status)
        layout.addLayout(budget_row)

        # Summary cards row
        cards = QHBoxLayout()
        cards.setSpacing(8)
        self._card_over_sharded = SummaryCard("Over-Sharded")
        cards.addWidget(self._card_over_sharded)
        self._card_mergeable = SummaryCard("Mergeable")
        cards.addWidget(self._card_mergeable)
        self._card_optimal = SummaryCard("Already Optimal")
        cards.addWidget(self._card_optimal)
        self._card_processed = SummaryCard("Processed")
        cards.addWidget(self._card_processed)
        layout.addLayout(cards)

        # Splitter for the two tables
        splitter = QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(splitter, stretch=1)

        # Over-sharded table
        over_container = QWidget()
        over_layout = QVBoxLayout(over_container)
        over_layout.setContentsMargins(0, 4, 0, 0)
        over_layout.addWidget(QLabel("<b>Over-Sharded Indices</b> (shrink candidates):"))
        self._tbl_over_sharded = self._make_table(
            ["Index Name", "Shards", "Target", "Savings", "Size (MB)"]
        )
        over_layout.addWidget(self._tbl_over_sharded)
        splitter.addWidget(over_container)

        # Mergeable patterns table
        merge_container = QWidget()
        merge_layout = QVBoxLayout(merge_container)
        merge_layout.setContentsMargins(0, 4, 0, 0)
        merge_layout.addWidget(QLabel("<b>Mergeable Patterns</b> (merge candidates):"))
        self._tbl_patterns = self._make_table(
            ["Base Pattern", "Indices", "Shards", "→ Monthly", "Savings", "Size (MB)"]
        )
        merge_layout.addWidget(self._tbl_patterns)
        splitter.addWidget(merge_container)

        # Recommendation label at bottom
        self._lbl_recommendation = QLabel("Connect to a cluster and click Refresh to analyze.")
        self._lbl_recommendation.setWordWrap(True)
        rec_font = QFont()
        rec_font.setPointSize(10)
        self._lbl_recommendation.setFont(rec_font)
        layout.addWidget(self._lbl_recommendation)

    def _make_table(self, headers: list[str]) -> QTableWidget:
        tbl = QTableWidget()
        tbl.setColumnCount(len(headers))
        tbl.setHorizontalHeaderLabels(headers)
        tbl.horizontalHeader().setStretchLastSection(True)
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for col in range(1, len(headers)):
            tbl.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        tbl.setAlternatingRowColors(True)
        tbl.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        tbl.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        tbl.verticalHeader().setVisible(False)
        tbl.setSortingEnabled(True)
        return tbl

    def _right_item(self, text: str) -> QTableWidgetItem:
        item = QTableWidgetItem()
        item.setData(Qt.ItemDataRole.DisplayRole, text)
        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return item

    def _right_int_item(self, value: int) -> QTableWidgetItem:
        """Create a right-aligned item that sorts numerically."""
        item = QTableWidgetItem()
        item.setData(Qt.ItemDataRole.DisplayRole, value)
        item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return item

    def set_analysis(self, analysis: FrozenAnalysis) -> None:
        """Populate the widget with analysis results."""
        self._analysis = analysis

        # Budget bar
        self._budget_bar.set_budget(analysis.total_shards, analysis.frozen_limit)
        status = analysis.budget_status()
        colors = {"OK": "#43a047", "WARNING": "#ffc107", "CRITICAL": "#ff9800", "OVER LIMIT": "#e53935"}
        self._lbl_status.setText(status)
        self._lbl_status.setStyleSheet(f"color: {colors.get(status, '#eee')};")

        # Summary cards
        self._card_over_sharded.set_detail(
            f"{len(analysis.over_sharded)} indices\n{analysis.shrink_savings:,} shards reclaimable"
        )
        self._card_mergeable.set_detail(
            f"{len(analysis.mergeable_patterns)} patterns\n{analysis.merge_monthly_savings:,} shards reclaimable"
        )
        self._card_optimal.set_detail(f"{analysis.already_optimal} indices")
        self._card_processed.set_detail(
            f"{analysis.already_sharderated} sharderated\n{analysis.already_merged} merged"
        )

        # Over-sharded table
        self._tbl_over_sharded.setSortingEnabled(False)
        self._tbl_over_sharded.setRowCount(len(analysis.over_sharded))
        for row, idx in enumerate(analysis.over_sharded):
            self._tbl_over_sharded.setItem(row, 0, QTableWidgetItem(idx.name))
            self._tbl_over_sharded.setItem(row, 1, self._right_int_item(idx.shard_count))
            self._tbl_over_sharded.setItem(row, 2, self._right_int_item(idx.target_shard_count))
            self._tbl_over_sharded.setItem(row, 3, self._right_int_item(idx.shard_count - idx.target_shard_count))
            self._tbl_over_sharded.setItem(row, 4, self._right_item(f"{idx.store_size_mb:.1f}"))
        self._tbl_over_sharded.setSortingEnabled(True)

        # Mergeable patterns table
        self._tbl_patterns.setSortingEnabled(False)
        self._tbl_patterns.setRowCount(len(analysis.mergeable_patterns))
        for row, p in enumerate(analysis.mergeable_patterns):
            self._tbl_patterns.setItem(row, 0, QTableWidgetItem(p.base_pattern))
            self._tbl_patterns.setItem(row, 1, self._right_int_item(p.index_count))
            self._tbl_patterns.setItem(row, 2, self._right_int_item(p.total_shards))
            self._tbl_patterns.setItem(row, 3, self._right_int_item(p.shards_after_monthly_merge))
            self._tbl_patterns.setItem(row, 4, self._right_int_item(p.monthly_savings))
            self._tbl_patterns.setItem(row, 5, self._right_item(f"{p.total_size_mb:.1f}"))
        self._tbl_patterns.setSortingEnabled(True)

        # Recommendation
        total_reclaimable = analysis.shrink_savings + analysis.merge_monthly_savings
        if total_reclaimable > 0:
            after = analysis.total_shards - total_reclaimable
            after_pct = after / analysis.frozen_limit * 100 if analysis.frozen_limit else 0
            self._lbl_recommendation.setText(
                f"<b>{total_reclaimable:,} shards reclaimable</b> — "
                f"Shrink: {analysis.shrink_savings:,} from {len(analysis.over_sharded)} indices  |  "
                f"Merge (monthly): {analysis.merge_monthly_savings:,} from {len(analysis.mergeable_patterns)} patterns  |  "
                f"Budget after: {after:,} / {analysis.frozen_limit:,} ({after_pct:.1f}%)"
            )
        else:
            self._lbl_recommendation.setText(
                "Frozen tier is already optimally sharded. No action needed."
            )

    def clear(self) -> None:
        self._analysis = None
        self._budget_bar.set_budget(0, 1)
        self._lbl_status.setText("")
        self._tbl_over_sharded.setRowCount(0)
        self._tbl_patterns.setRowCount(0)
        self._lbl_recommendation.setText("Connect to a cluster and click Refresh to analyze.")
        for card in (self._card_over_sharded, self._card_mergeable, self._card_optimal, self._card_processed):
            card.set_detail("")
