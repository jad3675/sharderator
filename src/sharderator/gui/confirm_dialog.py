"""Pre-execution confirmation dialog."""

from __future__ import annotations

from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QVBoxLayout,
    QWidget,
)

from sharderator.models.index_info import IndexInfo


class ConfirmDialog(QDialog):
    def __init__(
        self,
        indices: list[IndexInfo],
        dry_run: bool,
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Confirm Execution")
        self.setMinimumWidth(450)

        layout = QVBoxLayout(self)

        total_shards = sum(i.shard_count for i in indices)
        target_shards = sum(i.target_shard_count for i in indices)
        total_size = sum(i.store_size_bytes for i in indices)
        size_mb = total_size / (1024 * 1024)

        mode = "DRY RUN" if dry_run else "LIVE EXECUTION"
        ds_count = sum(1 for i in indices if i.is_data_stream_member)
        routing_count = sum(1 for i in indices if i.has_custom_routing)

        summary = (
            f"Mode: {mode}\n\n"
            f"Indices to consolidate: {len(indices)}\n"
            f"Current total shards: {total_shards}\n"
            f"Target total shards: {target_shards}\n"
            f"Shard reduction: {total_shards - target_shards}\n"
            f"Estimated working tier space needed: {size_mb:.1f} MB\n"
        )
        if ds_count:
            summary += f"\n⚠ {ds_count} index(es) are data stream members\n"
        if routing_count:
            summary += f"\n⚠ {routing_count} index(es) use custom routing\n"
        if not dry_run:
            summary += "\nThis will modify your cluster. Proceed?"

        label = QLabel(summary)
        label.setWordWrap(True)
        layout.addWidget(label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)


class MergeConfirmDialog(QDialog):
    """Pre-execution confirmation dialog for merge mode (dry run and live)."""

    def __init__(
        self,
        groups: list,
        dry_run: bool,
        preflight_issues: list[str] | None = None,
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Confirm Merge" if not dry_run else "Merge Dry Run Summary")
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)

        total_sources = sum(len(g.source_indices) for g in groups)
        total_saved = sum(g.shard_reduction for g in groups)
        total_size = sum(g.total_size_bytes for g in groups)
        size_mb = total_size / (1024 * 1024)

        mode = "DRY RUN" if dry_run else "LIVE EXECUTION"

        summary = (
            f"Mode: {mode}\n\n"
            f"Merge groups: {len(groups)}\n"
            f"Source indices: {total_sources}\n"
            f"Target indices: {len(groups)} (one per group)\n"
            f"Shard reduction: {total_saved}\n"
            f"Estimated working tier space needed: ~{size_mb * 2:.0f} MB (2× source)\n"
        )

        # Per-group breakdown
        summary += "\nGroups:\n"
        for g in groups:
            summary += (
                f"  {g.base_pattern}/{g.time_bucket}: "
                f"{len(g.source_indices)} → 1 "
                f"({g.shard_reduction} shards saved)\n"
            )

        # Preflight results
        if preflight_issues is not None:
            summary += "\n"
            if preflight_issues:
                summary += "⛔ Pre-flight issues:\n"
                for issue in preflight_issues:
                    summary += f"  {issue}\n"
            else:
                summary += "✓ Pre-flight checks passed\n"

        if not dry_run:
            summary += "\nThis will modify your cluster. Proceed?"

        label = QLabel(summary)
        label.setWordWrap(True)
        layout.addWidget(label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
