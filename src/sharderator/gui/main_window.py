"""Main application window with Shrink Mode and Merge Mode tabs."""

from __future__ import annotations

from PyQt6.QtCore import Qt, QThread, pyqtSignal, pyqtSlot
from PyQt6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from sharderator.client.connection import ClusterConnection
from sharderator.client.queries import list_frozen_indices, enrich_index_info, get_data_stream_membership
from sharderator.engine.merge_analyzer import MergeGroup, propose_merges, validate_merge_group
from sharderator.engine.merge_orchestrator import MergeOrchestrator
from sharderator.engine.orchestrator import Orchestrator
from sharderator.engine.preflight import run_dry_run_preflight
from sharderator.gui.confirm_dialog import ConfirmDialog, MergeConfirmDialog
from sharderator.gui.connection_dialog import ConnectionDialog
from sharderator.gui.index_table import IndexTableModel, IndexTableView
from sharderator.gui.job_log import JobLog
from sharderator.gui.merge_tree import MergeTreeWidget
from sharderator.gui.progress import ProgressWidget
from sharderator.gui.settings_dialog import SettingsDialog
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord
from sharderator.util.config import AppConfig


class ShrinkWorkerThread(QThread):
    """Runs the shrink orchestrator in a background thread."""

    progress = pyqtSignal(str, float)
    state_changed = pyqtSignal(str, str)
    log_message = pyqtSignal(str)
    completed = pyqtSignal(str)
    failed = pyqtSignal(str, str)
    batch_done = pyqtSignal()

    def __init__(self, orchestrator: Orchestrator, indices: list[IndexInfo]):
        super().__init__()
        self._orchestrator = orchestrator
        self._indices = indices
        orchestrator.progress.connect(self.progress)
        orchestrator.state_changed.connect(self.state_changed)
        orchestrator.log_message.connect(self.log_message)
        orchestrator.completed.connect(self.completed)
        orchestrator.failed.connect(self.failed)

    def run(self) -> None:
        for info in self._indices:
            self._orchestrator.run(info)
        self.batch_done.emit()


class MergeWorkerThread(QThread):
    """Runs the merge orchestrator in a background thread."""

    progress = pyqtSignal(str, float)
    state_changed = pyqtSignal(str, str)
    log_message = pyqtSignal(str)
    completed = pyqtSignal(str)
    failed = pyqtSignal(str, str)
    batch_done = pyqtSignal()

    def __init__(self, orchestrator: MergeOrchestrator, groups: list[MergeGroup]):
        super().__init__()
        self._orchestrator = orchestrator
        self._groups = groups
        orchestrator.progress.connect(self.progress)
        orchestrator.state_changed.connect(self.state_changed)
        orchestrator.log_message.connect(self.log_message)
        orchestrator.completed.connect(self.completed)
        orchestrator.failed.connect(self.failed)

    def run(self) -> None:
        total = len(self._groups)
        for i, group in enumerate(self._groups, 1):
            self.log_message.emit(
                f"=== Merge group {i}/{total}: {group.base_pattern}/{group.time_bucket} "
                f"({len(group.source_indices)} indices) ==="
            )
            self._orchestrator.run(group)
        self.batch_done.emit()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Sharderator — Frozen Tier Shard Consolidation")
        self.setMinimumSize(1000, 700)

        self._config = AppConfig.load()
        self._conn = ClusterConnection()
        self._worker: ShrinkWorkerThread | MergeWorkerThread | None = None
        self._frozen_indices: list[IndexInfo] = []

        self._build_ui()
        self._update_connection_status()

    def _build_ui(self) -> None:
        # Toolbar
        toolbar = QToolBar()
        self.addToolBar(toolbar)

        self._btn_connect = QPushButton("Connect")
        self._btn_connect.clicked.connect(self._on_connect)
        toolbar.addWidget(self._btn_connect)

        self._lbl_cluster = QLabel("  Not connected")
        toolbar.addWidget(self._lbl_cluster)

        toolbar.addSeparator()

        self._btn_settings = QPushButton("Settings")
        self._btn_settings.clicked.connect(self._on_settings)
        self._btn_settings.setEnabled(False)
        toolbar.addWidget(self._btn_settings)

        # Central widget with tabs
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        splitter = QSplitter()
        splitter.setOrientation(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        # Top: tab widget with Shrink Mode and Merge Mode
        self._tabs = QTabWidget()
        splitter.addWidget(self._tabs)

        # --- Shrink Mode Tab ---
        shrink_tab = QWidget()
        shrink_layout = QVBoxLayout(shrink_tab)
        shrink_layout.setContentsMargins(4, 4, 4, 4)

        self._table_model = IndexTableModel()
        self._table_view = IndexTableView()
        self._table_view.setModel(self._table_model)
        shrink_layout.addWidget(self._table_view)

        self._lbl_shrink_summary = QLabel("Select indices to consolidate")
        shrink_layout.addWidget(self._lbl_shrink_summary)

        shrink_btns = QHBoxLayout()
        self._btn_analyze = QPushButton("Analyze")
        self._btn_analyze.clicked.connect(self._on_analyze)
        self._btn_analyze.setEnabled(False)
        shrink_btns.addWidget(self._btn_analyze)

        self._btn_dryrun = QPushButton("Dry Run")
        self._btn_dryrun.clicked.connect(lambda: self._on_shrink_execute(dry_run=True))
        self._btn_dryrun.setEnabled(False)
        shrink_btns.addWidget(self._btn_dryrun)

        self._btn_execute = QPushButton("Execute")
        self._btn_execute.clicked.connect(lambda: self._on_shrink_execute(dry_run=False))
        self._btn_execute.setEnabled(False)
        shrink_btns.addWidget(self._btn_execute)

        shrink_layout.addLayout(shrink_btns)
        self._tabs.addTab(shrink_tab, "Shrink Mode")

        # --- Merge Mode Tab ---
        merge_tab = QWidget()
        merge_layout = QVBoxLayout(merge_tab)
        merge_layout.setContentsMargins(4, 4, 4, 4)

        merge_controls = QHBoxLayout()
        merge_controls.addWidget(QLabel("Granularity:"))
        self._merge_granularity = QComboBox()
        self._merge_granularity.addItems(["monthly", "quarterly", "yearly"])
        self._merge_granularity.setCurrentText(self._config.operation.merge_granularity)
        merge_controls.addWidget(self._merge_granularity)

        self._btn_analyze_merges = QPushButton("Analyze Merges")
        self._btn_analyze_merges.clicked.connect(self._on_analyze_merges)
        self._btn_analyze_merges.setEnabled(False)
        merge_controls.addWidget(self._btn_analyze_merges)
        merge_controls.addStretch()
        merge_layout.addLayout(merge_controls)

        self._merge_tree = MergeTreeWidget()
        merge_layout.addWidget(self._merge_tree)

        self._lbl_merge_summary = QLabel("Connect to a cluster to analyze merge candidates")
        merge_layout.addWidget(self._lbl_merge_summary)

        merge_btns = QHBoxLayout()
        self._btn_merge_dryrun = QPushButton("Dry Run")
        self._btn_merge_dryrun.clicked.connect(lambda: self._on_merge_execute(dry_run=True))
        self._btn_merge_dryrun.setEnabled(False)
        merge_btns.addWidget(self._btn_merge_dryrun)

        self._btn_merge_execute = QPushButton("Execute Merge")
        self._btn_merge_execute.clicked.connect(lambda: self._on_merge_execute(dry_run=False))
        self._btn_merge_execute.setEnabled(False)
        merge_btns.addWidget(self._btn_merge_execute)

        merge_layout.addLayout(merge_btns)
        self._tabs.addTab(merge_tab, "Merge Mode")

        # --- Bottom: shared job log + progress ---
        bottom = QWidget()
        bottom_layout = QVBoxLayout(bottom)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        self._job_log = JobLog()
        bottom_layout.addWidget(self._job_log)

        self._progress = ProgressWidget()
        bottom_layout.addWidget(self._progress)

        self._btn_cancel = QPushButton("Cancel")
        self._btn_cancel.clicked.connect(self._on_cancel)
        self._btn_cancel.setEnabled(False)
        bottom_layout.addWidget(self._btn_cancel)

        splitter.addWidget(bottom)
        splitter.setSizes([450, 200])

        self.setStatusBar(QStatusBar())

    # --- Connection ---

    def _update_connection_status(self) -> None:
        if self._conn.connected:
            health_icon = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(
                self._conn.cluster_health, "⚪"
            )
            self._lbl_cluster.setText(
                f"  {self._conn.cluster_name}  {health_icon}  "
                f"{self._conn.frozen_shard_count}/{self._conn.frozen_shard_limit} frozen shards"
            )
            self._btn_connect.setText("Disconnect")
            self._btn_settings.setEnabled(True)
            self._btn_analyze.setEnabled(True)
            self._btn_analyze_merges.setEnabled(True)
        else:
            self._lbl_cluster.setText("  Not connected")
            self._btn_connect.setText("Connect")
            self._btn_settings.setEnabled(False)
            self._btn_analyze.setEnabled(False)
            self._btn_dryrun.setEnabled(False)
            self._btn_execute.setEnabled(False)
            self._btn_analyze_merges.setEnabled(False)
            self._btn_merge_dryrun.setEnabled(False)
            self._btn_merge_execute.setEnabled(False)

    @pyqtSlot()
    def _on_connect(self) -> None:
        if self._conn.connected:
            self._conn.disconnect()
            self._table_model.set_indices([])
            self._merge_tree.clear()
            self._frozen_indices = []
            self._update_connection_status()
            return

        dlg = ConnectionDialog(self._config.connection, self)
        if dlg.exec() != ConnectionDialog.DialogCode.Accepted:
            return

        conn_cfg = dlg.get_config()
        try:
            self._conn.connect(conn_cfg)
            self._config.connection = conn_cfg
            self._config.save()
            self._update_connection_status()
            self._load_indices()
        except Exception as e:
            QMessageBox.critical(self, "Connection Failed", str(e))

    def _load_indices(self) -> None:
        self._frozen_indices = list_frozen_indices(self._conn.client)
        self._table_model.set_indices(self._frozen_indices)
        self._job_log.append_log(f"Loaded {len(self._frozen_indices)} frozen indices")
        has_indices = bool(self._frozen_indices)
        self._btn_dryrun.setEnabled(has_indices)
        self._btn_execute.setEnabled(has_indices)
        self._btn_merge_dryrun.setEnabled(has_indices)
        self._btn_merge_execute.setEnabled(has_indices)

    @pyqtSlot()
    def _on_settings(self) -> None:
        dlg = SettingsDialog(
            self._config.operation, self._conn.snapshot_repos, self
        )
        if dlg.exec() == SettingsDialog.DialogCode.Accepted:
            self._config.operation = dlg.get_config()
            self._config.save()

    # --- Shrink Mode ---

    @pyqtSlot()
    def _on_analyze(self) -> None:
        selected = self._table_model.get_selected()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select at least one index.")
            return

        enriched = []
        ds_membership = get_data_stream_membership(self._conn.client)
        for info in selected:
            try:
                info = enrich_index_info(self._conn.client, info, ds_membership)
                enriched.append(info)
            except Exception as e:
                self._job_log.append_log(f"⚠ {info.name}: {e}")

        self._table_model.set_indices(
            enriched + [i for i in self._table_model._indices if i not in selected]
        )

        total_reduction = sum(i.shard_reduction for i in enriched)
        self._lbl_shrink_summary.setText(
            f"Selected: {len(enriched)} indices, "
            f"shard reduction: {total_reduction}, "
            f"budget after: {self._conn.frozen_shard_count - total_reduction}"
            f"/{self._conn.frozen_shard_limit}"
        )

    def _on_shrink_execute(self, dry_run: bool) -> None:
        selected = self._table_model.get_selected()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select at least one index.")
            return

        self._config.operation.dry_run = dry_run
        dlg = ConfirmDialog(selected, dry_run, self)
        if dlg.exec() != ConfirmDialog.DialogCode.Accepted:
            return

        if dry_run:
            self._job_log.append_log("=== DRY RUN — no changes will be made ===")

            # Run preflight safety checks against the live cluster
            total_needed = max(i.store_size_bytes for i in selected) if selected else 0
            issues = run_dry_run_preflight(
                self._conn.client,
                allow_yellow=self._config.operation.allow_yellow_cluster,
                disk_safety_margin=self._config.operation.disk_safety_margin,
                needed_bytes=total_needed,
                ignore_circuit_breakers=self._config.operation.ignore_circuit_breakers,
            )

            if issues:
                self._job_log.append_log("⛔ Pre-flight checks FAILED:")
                for issue in issues:
                    self._job_log.append_log(f"  {issue}")
            else:
                self._job_log.append_log("✓ Pre-flight checks passed (cluster health, circuit breakers, disk space)")

            for info in selected:
                self._job_log.append_log(
                    f"  {info.name}: {info.shard_count} → {info.target_shard_count} shards"
                )
            return

        self._set_running(True)
        orch = Orchestrator(self._conn.client, self._config.operation)
        self._worker = ShrinkWorkerThread(orch, selected)
        self._wire_worker_signals(self._worker)
        self._worker.start()

    # --- Merge Mode ---

    @pyqtSlot()
    def _on_analyze_merges(self) -> None:
        if not self._frozen_indices:
            QMessageBox.information(self, "No Data", "No frozen indices loaded.")
            return

        granularity = self._merge_granularity.currentText()
        self._config.operation.merge_granularity = granularity
        self._config.save()

        groups = propose_merges(self._frozen_indices, granularity)

        # Wire up lazy validation — runs per-group when the user checks it
        self._merge_tree.set_validator(
            lambda g: validate_merge_group(self._conn.client, g),
            log_callback=self._job_log.append_log,
        )
        self._merge_tree.set_groups(groups)

        total_sources = sum(len(g.source_indices) for g in groups)
        total_saved = sum(g.shard_reduction for g in groups)
        self._lbl_merge_summary.setText(
            f"Found {len(groups)} merge groups from {total_sources} indices, "
            f"saving {total_saved} shards → "
            f"budget after: {self._conn.frozen_shard_count - total_saved}"
            f"/{self._conn.frozen_shard_limit}"
        )
        self._job_log.append_log(
            f"Merge analysis: {len(groups)} groups, {total_saved} shards saved"
        )

    def _on_merge_execute(self, dry_run: bool) -> None:
        groups = self._merge_tree.get_selected_groups()
        if not groups:
            QMessageBox.information(self, "No Selection", "Select at least one merge group.")
            return

        if dry_run:
            # Run preflight safety checks — merge needs ~2x total source size
            total_source_bytes = sum(g.total_size_bytes for g in groups)
            needed = total_source_bytes * 2
            issues = run_dry_run_preflight(
                self._conn.client,
                allow_yellow=self._config.operation.allow_yellow_cluster,
                disk_safety_margin=self._config.operation.disk_safety_margin,
                needed_bytes=needed,
                ignore_circuit_breakers=self._config.operation.ignore_circuit_breakers,
            )

            # Show confirmation popup with preflight results
            dlg = MergeConfirmDialog(groups, dry_run=True, preflight_issues=issues, parent=self)
            dlg.exec()

            # Also log to the job log
            self._job_log.append_log("=== MERGE DRY RUN — no changes will be made ===")
            if issues:
                self._job_log.append_log("⛔ Pre-flight checks FAILED:")
                for issue in issues:
                    self._job_log.append_log(f"  {issue}")
            else:
                self._job_log.append_log("✓ Pre-flight checks passed (cluster health, circuit breakers, disk space)")

            for g in groups:
                self._job_log.append_log(
                    f"  {g.base_pattern}/{g.time_bucket}: "
                    f"{len(g.source_indices)} indices → 1 "
                    f"({g.shard_reduction} shards saved)"
                )
            return

        # Confirm
        total_sources = sum(len(g.source_indices) for g in groups)
        total_saved = sum(g.shard_reduction for g in groups)
        total_size = sum(g.total_size_bytes for g in groups)
        size_mb = total_size / (1024 * 1024)

        reply = QMessageBox.question(
            self,
            "Confirm Merge",
            f"Merge {total_sources} indices into {len(groups)} merged indices.\n"
            f"Shard reduction: {total_saved}\n"
            f"Working tier space needed: ~{size_mb * 2:.0f} MB (2× source size)\n\n"
            f"This will modify your cluster. Proceed?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._set_running(True)
        orch = MergeOrchestrator(self._conn.client, self._config.operation)
        self._worker = MergeWorkerThread(orch, groups)
        self._wire_worker_signals(self._worker)
        self._worker.start()

    # --- Shared ---

    def _wire_worker_signals(self, worker: ShrinkWorkerThread | MergeWorkerThread) -> None:
        worker.progress.connect(self._progress.update_progress)
        worker.log_message.connect(self._job_log.append_log)
        worker.state_changed.connect(
            lambda name, state: self.statusBar().showMessage(f"{name}: {state}")
        )
        worker.completed.connect(
            lambda name: self._job_log.append_log(f"✓ {name} COMPLETED")
        )
        worker.failed.connect(
            lambda name, err: self._job_log.append_log(f"✗ {name} FAILED: {err}")
        )
        worker.batch_done.connect(self._on_batch_done)

    @pyqtSlot()
    def _on_cancel(self) -> None:
        if self._worker:
            if isinstance(self._worker, ShrinkWorkerThread):
                self._worker._orchestrator.cancel()
            elif isinstance(self._worker, MergeWorkerThread):
                self._worker._orchestrator.cancel()
            self._job_log.append_log("Cancellation requested...")

    @pyqtSlot()
    def _on_batch_done(self) -> None:
        self._set_running(False)
        self._progress.reset()
        self._load_indices()
        self._job_log.append_log("=== Batch complete ===")

    def _set_running(self, running: bool) -> None:
        self._btn_analyze.setEnabled(not running)
        self._btn_dryrun.setEnabled(not running)
        self._btn_execute.setEnabled(not running)
        self._btn_analyze_merges.setEnabled(not running)
        self._btn_merge_dryrun.setEnabled(not running)
        self._btn_merge_execute.setEnabled(not running)
        self._btn_cancel.setEnabled(running)
        self._btn_connect.setEnabled(not running)
        self._btn_settings.setEnabled(not running)
