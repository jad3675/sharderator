"""Settings dialog for operation configuration."""

from __future__ import annotations

from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from sharderator.util.config import OperationConfig


class SettingsDialog(QDialog):
    def __init__(
        self,
        config: OperationConfig,
        repos: list[str],
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Operation Settings")
        self.setMinimumWidth(450)
        self._config = config
        self._repos = repos
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        # General settings
        general = QGroupBox("General")
        form = QFormLayout(general)

        self._target_shards = QSpinBox()
        self._target_shards.setRange(1, 100)
        self._target_shards.setValue(self._config.target_shard_count)
        form.addRow("Target shard count:", self._target_shards)

        self._repo = QComboBox()
        self._repo.addItems(self._repos)
        if self._config.snapshot_repo in self._repos:
            self._repo.setCurrentText(self._config.snapshot_repo)
        form.addRow("Snapshot repository:", self._repo)

        self._timeout = QSpinBox()
        self._timeout.setRange(5, 240)
        self._timeout.setSuffix(" min")
        self._timeout.setValue(self._config.recovery_timeout_minutes)
        form.addRow("Recovery timeout:", self._timeout)

        self._delete_old = QCheckBox("Delete old snapshot after verification")
        self._delete_old.setChecked(self._config.delete_old_snapshots)
        form.addRow(self._delete_old)

        layout.addWidget(general)

        # Safety settings
        safety = QGroupBox("Safety")
        safety_form = QFormLayout(safety)

        self._safety_margin = QDoubleSpinBox()
        self._safety_margin.setRange(0.10, 0.50)
        self._safety_margin.setSingleStep(0.05)
        self._safety_margin.setDecimals(2)
        self._safety_margin.setValue(self._config.disk_safety_margin)
        self._safety_margin.setSuffix(" (keep free)")
        safety_form.addRow("Disk safety margin:", self._safety_margin)

        self._batch_size = QSpinBox()
        self._batch_size.setRange(1, 20)
        self._batch_size.setValue(self._config.restore_batch_size)
        safety_form.addRow("Restore batch size:", self._batch_size)

        self._reindex_throttle = QDoubleSpinBox()
        self._reindex_throttle.setRange(-1, 100000)
        self._reindex_throttle.setDecimals(0)
        self._reindex_throttle.setSpecialValueText("Unlimited")
        self._reindex_throttle.setValue(self._config.reindex_requests_per_second)
        self._reindex_throttle.setSuffix(" docs/sec")
        safety_form.addRow("Reindex throttle:", self._reindex_throttle)

        self._allow_yellow = QCheckBox("Allow operations on yellow-health clusters")
        self._allow_yellow.setChecked(self._config.allow_yellow_cluster)
        safety_form.addRow(self._allow_yellow)

        self._ignore_breakers = QCheckBox("Ignore circuit breaker warnings (use with caution)")
        self._ignore_breakers.setChecked(self._config.ignore_circuit_breakers)
        safety_form.addRow(self._ignore_breakers)

        layout.addWidget(safety)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_config(self) -> OperationConfig:
        return OperationConfig(
            target_shard_count=self._target_shards.value(),
            snapshot_repo=self._repo.currentText(),
            recovery_timeout_minutes=self._timeout.value(),
            delete_old_snapshots=self._delete_old.isChecked(),
            dry_run=self._config.dry_run,
            merge_granularity=self._config.merge_granularity,
            disk_safety_margin=self._safety_margin.value(),
            restore_batch_size=self._batch_size.value(),
            reindex_requests_per_second=self._reindex_throttle.value(),
            allow_yellow_cluster=self._allow_yellow.isChecked(),
            ignore_circuit_breakers=self._ignore_breakers.isChecked(),
        )
