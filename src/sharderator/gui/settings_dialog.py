"""Settings dialog for operation configuration."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTextBrowser,
    QVBoxLayout,
    QWidget,
)

from sharderator.util.config import OperationConfig


class SettingsHelpDialog(QDialog):
    """Scrollable help window explaining every setting."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Settings Reference")
        self.setMinimumSize(560, 480)

        layout = QVBoxLayout(self)
        browser = QTextBrowser()
        browser.setOpenExternalLinks(True)
        browser.setHtml(_HELP_HTML)
        layout.addWidget(browser)

        close = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        close.rejected.connect(self.reject)
        layout.addWidget(close)


_HELP_HTML = """
<style>
  body { font-family: sans-serif; font-size: 13px; color: #222; }
  h2 { color: #1a237e; border-bottom: 1px solid #ccc; padding-bottom: 4px; margin-top: 18px; }
  h3 { color: #283593; margin-top: 12px; margin-bottom: 2px; }
  p { margin: 4px 0 8px 0; }
  .default { color: #666; font-style: italic; }
</style>

<h2>General Settings</h2>

<h3>Target shard count</h3>
<p>The number of shards each index should be shrunk to. Almost always 1 —
the only reason to use a higher value is if a single index exceeds the
Lucene 2.1 billion document limit.</p>
<p class="default">Default: 1</p>

<h3>Snapshot repository</h3>
<p>Which snapshot repository to use for creating new snapshots after
shrink/merge. Auto-detected from the cluster. On Elastic Cloud this is
typically <code>found-snapshots</code>.</p>

<h3>Recovery timeout</h3>
<p>Minimum time to wait for a restored index to reach green health before
giving up. Size-tier profiles may increase this automatically for large
indices (up to 6 hours for indices over 500 GB).</p>
<p class="default">Default: 30 minutes</p>

<h3>Working tier</h3>
<p>Where restored indices are placed during shrink/merge operations.
This is a comma-separated tier preference list — Elasticsearch tries
each tier in order and uses the first one with available capacity.</p>
<p>Common choices:</p>
<ul>
  <li><b>data_warm,data_hot,data_content</b> — Warm preferred (default).
      Uses warm nodes if available, falls back to hot. Best for clusters
      with dedicated warm nodes — avoids competing with ingest on hot.</li>
  <li><b>data_hot,data_warm,data_content</b> — Hot preferred.
      Uses hot nodes first. Faster I/O but may compete with ingest.</li>
  <li><b>data_hot</b> — Hot only. Use when there is no warm tier.</li>
  <li><b>data_warm</b> — Warm only. Use when hot nodes are under pressure.</li>
</ul>
<p>The field is editable — you can type any valid tier preference string.</p>
<p class="default">Default: data_warm,data_hot,data_content</p>

<h3>Delete old snapshot after verification</h3>
<p>If checked, the original backing snapshot is deleted after the new
shrunk/merged index is verified and the data stream swap completes.
Only safe if no other indices are mounted from the same snapshot.
Sharderator checks for other references before deleting.</p>
<p class="default">Default: Off</p>

<h2>Safety Settings</h2>

<h3>Disk safety margin</h3>
<p>Minimum free disk percentage to maintain on the working tier during
operations. Before each restore, Sharderator checks that the working
tier will still have at least this much free space after the restore
completes. Prevents filling disks and triggering ES watermarks.</p>
<p class="default">Default: 0.30 (keep 30% free)</p>

<h3>Restore batch size</h3>
<p>Maximum number of indices to restore simultaneously from snapshot.
Dynamic batching may reduce this further based on available disk budget.
Higher values speed up merge operations but increase disk and I/O pressure.</p>
<p class="default">Default: 3</p>

<h3>Reindex throttle</h3>
<p>Rate limit for reindex operations in documents per second. Prevents
Sharderator from starving production ingest by consuming all write
throughput. Set to -1 (Unlimited) for maximum speed on idle clusters.
Size-tier profiles may reduce this automatically for large indices.</p>
<p class="default">Default: 5,000 docs/sec</p>

<h3>Allow operations on yellow-health clusters</h3>
<p>If checked, Sharderator will proceed even when the cluster health is
yellow (some replicas unassigned). Most frozen-tier clusters are yellow
by design since frozen indices have zero replicas. Uncheck only if you
need to enforce green-only operations.</p>
<p class="default">Default: On</p>

<h3>Ignore circuit breaker warnings</h3>
<p>If checked, the pre-flight circuit breaker check is skipped entirely.
Use with caution — circuit breakers protect against out-of-memory crashes.
Useful on trial clusters where tiebreaker nodes have small heaps that
routinely trip the 80% threshold under normal conditions.</p>
<p class="default">Default: Off</p>

<h3>Circuit breaker wait</h3>
<p>When a circuit breaker exceeds 80%, how long to wait (with linear
backoff) for pressure to subside before failing the index. Set to 0
for fail-fast behavior (no waiting). The adaptive backoff starts at
30 seconds and increases by 30 seconds each cycle, capped at 5 minutes
per sleep.</p>
<p class="default">Default: 10 minutes</p>

<h3>Batch backpressure wait</h3>
<p>Between batch items (indices or merge groups), how long to wait if
the cluster is under pressure (circuit breakers above 75%, high JVM
heap, deep thread pool queues). If all checks pass immediately, there
is zero delay. Set to 0 to disable inter-item waiting entirely.</p>
<p class="default">Default: 15 minutes</p>
"""


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

        self._working_tier = QComboBox()
        self._working_tier.setEditable(True)
        self._working_tier.addItems([
            "data_warm,data_hot,data_content",
            "data_hot,data_warm,data_content",
            "data_hot,data_content",
            "data_warm,data_content",
            "data_hot",
            "data_warm",
        ])
        self._working_tier.setCurrentText(self._config.working_tier)
        self._working_tier.setToolTip(
            "Tier preference for restored indices during shrink/merge operations.\n"
            "Comma-separated list — ES tries each tier in order.\n"
            "Default: warm → hot → content."
        )
        form.addRow("Working tier:", self._working_tier)

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

        self._breaker_wait = QSpinBox()
        self._breaker_wait.setRange(0, 60)
        self._breaker_wait.setSuffix(" min")
        self._breaker_wait.setValue(self._config.circuit_breaker_wait_minutes)
        self._breaker_wait.setToolTip(
            "How long to wait for circuit breaker pressure to subside "
            "before failing an index. 0 = fail immediately (old behavior)."
        )
        safety_form.addRow("Circuit breaker wait:", self._breaker_wait)

        self._backpressure_wait = QSpinBox()
        self._backpressure_wait.setRange(0, 60)
        self._backpressure_wait.setSuffix(" min")
        self._backpressure_wait.setValue(self._config.batch_backpressure_timeout_minutes)
        self._backpressure_wait.setToolTip(
            "How long to pause between batch items if the cluster is under "
            "pressure. 0 = no inter-index waiting."
        )
        safety_form.addRow("Batch backpressure wait:", self._backpressure_wait)

        layout.addWidget(safety)

        # Button row: Help | OK | Cancel
        btn_layout = QHBoxLayout()
        help_btn = QPushButton("Help")
        help_btn.clicked.connect(self._show_help)
        btn_layout.addWidget(help_btn)
        btn_layout.addStretch()

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        btn_layout.addWidget(buttons)
        layout.addLayout(btn_layout)

    def _show_help(self) -> None:
        dlg = SettingsHelpDialog(self)
        dlg.exec()

    def get_config(self) -> OperationConfig:
        return OperationConfig(
            target_shard_count=self._target_shards.value(),
            working_tier=self._working_tier.currentText().strip(),
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
            circuit_breaker_wait_minutes=self._breaker_wait.value(),
            batch_backpressure_timeout_minutes=self._backpressure_wait.value(),
        )
