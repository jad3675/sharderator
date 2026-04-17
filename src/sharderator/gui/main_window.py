"""Main application window with Shrink Mode and Merge Mode tabs."""

from __future__ import annotations

from PyQt6.QtCore import Qt, QThread, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QKeySequence, QShortcut
from PyQt6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
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
from sharderator.client.tracker import delete_job, list_all_jobs
from sharderator.engine.merge_analyzer import MergeGroup, propose_merges, validate_merge_group
from sharderator.engine.merge_orchestrator import MergeOrchestrator
from sharderator.engine.orchestrator import Orchestrator
from sharderator.engine.preflight import run_dry_run_preflight
from sharderator.gui.analyze_widget import AnalyzeWidget
from sharderator.gui.confirm_dialog import ConfirmDialog, MergeConfirmDialog
from sharderator.gui.connection_dialog import ConnectionDialog
from sharderator.gui.defrag_widget import DefragWidget
from sharderator.gui.index_table import IndexFilterProxy, IndexTableModel, IndexTableView
from sharderator.gui.job_log import JobLog
from sharderator.gui.job_history_dialog import JobHistoryDialog
from sharderator.gui.merge_tree import MergeTreeWidget
from sharderator.gui.progress import ProgressWidget
from sharderator.gui.qt_events import QtPipelineEvents
from sharderator.gui.settings_dialog import SettingsDialog
from sharderator.gui.tracker_table import TrackerTableModel, TrackerTableView
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
        # Forward events from the Qt adapter to this thread's signals
        events = orchestrator._events
        if isinstance(events, QtPipelineEvents):
            events.progress.connect(self.progress)
            events.state_changed.connect(self.state_changed)
            events.log_message.connect(self.log_message)
            events.completed.connect(self.completed)
            events.failed.connect(self.failed)

    def run(self) -> None:
        from sharderator.engine.preflight import wait_for_cluster_ready

        cfg = self._orchestrator._config
        client = self._orchestrator._client

        for i, info in enumerate(self._indices):
            if i > 0:
                try:
                    self.log_message.emit("Checking cluster readiness...")
                    wait_for_cluster_ready(
                        client,
                        allow_yellow=cfg.allow_yellow_cluster,
                        ignore_circuit_breakers=cfg.ignore_circuit_breakers,
                        wait_timeout_minutes=cfg.batch_backpressure_timeout_minutes,
                    )
                except Exception as e:
                    self.log_message.emit(f"⚠ Cluster readiness check: {e}")
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
        events = orchestrator._events
        if isinstance(events, QtPipelineEvents):
            events.progress.connect(self.progress)
            events.state_changed.connect(self.state_changed)
            events.log_message.connect(self.log_message)
            events.completed.connect(self.completed)
            events.failed.connect(self.failed)

    def run(self) -> None:
        from sharderator.engine.preflight import wait_for_cluster_ready

        cfg = self._orchestrator._config
        client = self._orchestrator._client
        total = len(self._groups)

        for i, group in enumerate(self._groups):
            if i > 0:
                try:
                    self.log_message.emit("Checking cluster readiness...")
                    wait_for_cluster_ready(
                        client,
                        allow_yellow=cfg.allow_yellow_cluster,
                        ignore_circuit_breakers=cfg.ignore_circuit_breakers,
                        wait_timeout_minutes=cfg.batch_backpressure_timeout_minutes,
                    )
                except Exception as e:
                    self.log_message.emit(f"⚠ Cluster readiness check: {e}")
            self.log_message.emit(
                f"=== Merge group {i + 1}/{total}: {group.base_pattern}/{group.time_bucket} "
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

        self._btn_refresh = QPushButton("Refresh")
        self._btn_refresh.clicked.connect(self._on_refresh)
        self._btn_refresh.setEnabled(False)
        toolbar.addWidget(self._btn_refresh)

        # Central widget with tabs
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        splitter = QSplitter()
        splitter.setOrientation(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)

        # Top: tab widget with Analyze, Shrink Mode, and Merge Mode
        self._tabs = QTabWidget()
        splitter.addWidget(self._tabs)

        # --- Frozen Analyze Tab ---
        self._analyze_widget = AnalyzeWidget()
        self._tabs.addTab(self._analyze_widget, "Frozen Analyze")

        # --- Shrink Mode Tab ---
        shrink_tab = QWidget()
        shrink_layout = QVBoxLayout(shrink_tab)
        shrink_layout.setContentsMargins(4, 4, 4, 4)

        # Filter bar
        shrink_filter_bar = QHBoxLayout()
        self._shrink_filter = QLineEdit()
        self._shrink_filter.setPlaceholderText("Filter indices... (supports * wildcards)")
        self._shrink_filter.setClearButtonEnabled(True)
        shrink_filter_bar.addWidget(self._shrink_filter)

        self._btn_shrink_select_all = QPushButton("Select All")
        self._btn_shrink_select_all.clicked.connect(self._on_shrink_select_all)
        shrink_filter_bar.addWidget(self._btn_shrink_select_all)

        self._btn_shrink_deselect_all = QPushButton("Deselect All")
        self._btn_shrink_deselect_all.clicked.connect(self._on_shrink_deselect_all)
        shrink_filter_bar.addWidget(self._btn_shrink_deselect_all)

        shrink_layout.addLayout(shrink_filter_bar)

        self._table_model = IndexTableModel()
        self._shrink_proxy = IndexFilterProxy()
        self._shrink_proxy.setSourceModel(self._table_model)
        self._table_view = IndexTableView()
        self._table_view.setModel(self._shrink_proxy)
        self._shrink_filter.textChanged.connect(self._shrink_proxy.set_filter_text)
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

        self._btn_export_shrink = QPushButton("Export")
        self._btn_export_shrink.clicked.connect(self._on_export_frozen_indices)
        self._btn_export_shrink.setEnabled(False)
        shrink_btns.addWidget(self._btn_export_shrink)

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

        # Filter bar
        merge_filter_bar = QHBoxLayout()
        self._merge_filter = QLineEdit()
        self._merge_filter.setPlaceholderText("Filter merge groups... (supports * wildcards)")
        self._merge_filter.setClearButtonEnabled(True)
        merge_filter_bar.addWidget(self._merge_filter)

        self._btn_merge_select_all = QPushButton("Select All")
        self._btn_merge_select_all.clicked.connect(self._on_merge_select_all)
        merge_filter_bar.addWidget(self._btn_merge_select_all)

        self._btn_merge_deselect_all = QPushButton("Deselect All")
        self._btn_merge_deselect_all.clicked.connect(self._on_merge_deselect_all)
        merge_filter_bar.addWidget(self._btn_merge_deselect_all)

        merge_layout.addLayout(merge_filter_bar)

        self._merge_tree = MergeTreeWidget()
        self._merge_filter.textChanged.connect(self._merge_tree.apply_filter)
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

        self._btn_export_merge = QPushButton("Export Dry Run")
        self._btn_export_merge.clicked.connect(self._on_export_merge_dryrun)
        self._btn_export_merge.setEnabled(False)
        merge_btns.addWidget(self._btn_export_merge)

        merge_layout.addLayout(merge_btns)
        self._tabs.addTab(merge_tab, "Merge Mode")

        # --- Job Tracker Tab ---
        tracker_tab = QWidget()
        tracker_layout = QVBoxLayout(tracker_tab)
        tracker_layout.setContentsMargins(4, 4, 4, 4)

        tracker_controls = QHBoxLayout()
        self._btn_refresh_tracker = QPushButton("Refresh Tracker")
        self._btn_refresh_tracker.clicked.connect(self._load_tracker)
        self._btn_refresh_tracker.setEnabled(False)
        tracker_controls.addWidget(self._btn_refresh_tracker)

        self._btn_export_tracker = QPushButton("Export")
        self._btn_export_tracker.clicked.connect(self._on_export_tracker)
        self._btn_export_tracker.setEnabled(False)
        tracker_controls.addWidget(self._btn_export_tracker)

        tracker_controls.addStretch()
        tracker_layout.addLayout(tracker_controls)

        self._tracker_model = TrackerTableModel()
        self._tracker_view = TrackerTableView()
        self._tracker_view.setModel(self._tracker_model)
        self._tracker_view.on_resume = self._on_tracker_resume
        self._tracker_view.on_delete = self._on_tracker_delete
        self._tracker_view.on_view_history = self._on_tracker_history
        self._tracker_view.on_export_job = self._on_export_single_job
        tracker_layout.addWidget(self._tracker_view)

        self._lbl_tracker_summary = QLabel("Connect to view tracked jobs")
        tracker_layout.addWidget(self._lbl_tracker_summary)

        self._tabs.addTab(tracker_tab, "Job Tracker")

        # --- Bottom: shared job log + progress ---
        bottom = QWidget()
        bottom_layout = QVBoxLayout(bottom)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        self._job_log = JobLog()
        bottom_layout.addWidget(self._job_log)

        self._defrag = DefragWidget()
        bottom_layout.addWidget(self._defrag)

        self._progress = ProgressWidget()
        bottom_layout.addWidget(self._progress)

        self._btn_cancel = QPushButton("Cancel")
        self._btn_cancel.clicked.connect(self._on_cancel)
        self._btn_cancel.setEnabled(False)
        bottom_layout.addWidget(self._btn_cancel)

        splitter.addWidget(bottom)
        splitter.setSizes([350, 300])

        # F5 = refresh
        refresh_shortcut = QShortcut(QKeySequence("F5"), self)
        refresh_shortcut.activated.connect(self._on_refresh)

        # Ctrl+A = select all visible in active tab
        select_all_shortcut = QShortcut(QKeySequence("Ctrl+A"), self)
        select_all_shortcut.activated.connect(self._on_ctrl_a)

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
            self._btn_refresh.setEnabled(True)
            self._btn_analyze.setEnabled(True)
            self._btn_analyze_merges.setEnabled(True)
            self._btn_refresh_tracker.setEnabled(True)
            self._btn_export_tracker.setEnabled(True)
        else:
            self._lbl_cluster.setText("  Not connected")
            self._btn_connect.setText("Connect")
            self._btn_settings.setEnabled(False)
            self._btn_refresh.setEnabled(False)
            self._btn_analyze.setEnabled(False)
            self._btn_dryrun.setEnabled(False)
            self._btn_execute.setEnabled(False)
            self._btn_export_shrink.setEnabled(False)
            self._btn_analyze_merges.setEnabled(False)
            self._btn_merge_dryrun.setEnabled(False)
            self._btn_merge_execute.setEnabled(False)
            self._btn_export_merge.setEnabled(False)
            self._btn_refresh_tracker.setEnabled(False)
            self._btn_export_tracker.setEnabled(False)

    @pyqtSlot()
    def _on_connect(self) -> None:
        if self._conn.connected:
            self._conn.disconnect()
            self._table_model.set_indices([])
            self._merge_tree.clear()
            self._analyze_widget.clear()
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
        stashed_names = self._table_model.get_checked_names()
        self._frozen_indices = list_frozen_indices(self._conn.client)
        self._table_model.set_indices(self._frozen_indices)
        if stashed_names:
            self._table_model.restore_checked_names(stashed_names)
        self._job_log.append_log(f"Loaded {len(self._frozen_indices)} frozen indices")
        has_indices = bool(self._frozen_indices)
        self._btn_dryrun.setEnabled(has_indices)
        self._btn_execute.setEnabled(has_indices)
        self._btn_export_shrink.setEnabled(has_indices)
        self._btn_merge_dryrun.setEnabled(has_indices)
        self._btn_merge_execute.setEnabled(has_indices)

        # Update the Frozen Analyze tab
        if self._frozen_indices:
            from sharderator.engine.frozen_analyzer import analyze_frozen_tier
            analysis = analyze_frozen_tier(
                self._frozen_indices,
                topology=self._conn.frozen_topology,
            )
            self._analyze_widget.set_analysis(analysis, cluster_name=self._conn.cluster_name)
        else:
            self._analyze_widget.clear()

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
        self._defrag.set_indices(selected)
        events = QtPipelineEvents()
        orch = Orchestrator(self._conn.client, self._config.operation, events)
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

        # Preserve checked groups across re-analysis
        stashed_keys = self._merge_tree.get_checked_keys()

        # Wire up lazy validation — runs per-group when the user checks it
        self._merge_tree.set_validator(
            lambda g: validate_merge_group(self._conn.client, g),
            log_callback=self._job_log.append_log,
        )
        self._merge_tree.set_groups(groups)

        # Restore previously checked groups
        if stashed_keys:
            self._merge_tree.restore_checked_keys(stashed_keys)

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
        self._btn_export_merge.setEnabled(bool(groups))

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
        # Flatten all source indices across groups for the defrag grid
        all_sources = [idx for g in groups for idx in g.source_indices]
        self._defrag.set_indices(all_sources)
        # Register merge name mapping so the defrag widget can map
        # merged output names back to source index cells
        merge_map = {
            g.merged_index_name: [idx.name for idx in g.source_indices]
            for g in groups
        }
        self._defrag.set_merge_groups(merge_map)
        events = QtPipelineEvents()
        orch = MergeOrchestrator(self._conn.client, self._config.operation, events)
        self._worker = MergeWorkerThread(orch, groups)
        self._wire_worker_signals(self._worker)
        self._worker.start()

    # --- Shared ---

    def _wire_worker_signals(self, worker: ShrinkWorkerThread | MergeWorkerThread) -> None:
        # Reset batch counters
        self._batch_completed: list[str] = []
        self._batch_failed: list[tuple[str, str]] = []
        self._batch_total: int = 0
        self._batch_shards_before: int = self._conn.frozen_shard_count

        if isinstance(worker, ShrinkWorkerThread):
            self._batch_total = len(worker._indices)
            self._batch_mode = "Shrink"
        elif isinstance(worker, MergeWorkerThread):
            self._batch_total = len(worker._groups)
            self._batch_mode = "Merge"

        worker.progress.connect(self._progress.update_progress)
        worker.progress.connect(self._defrag.update_progress)
        worker.state_changed.connect(self._defrag.update_state)
        worker.log_message.connect(self._job_log.append_log)
        worker.state_changed.connect(
            lambda name, state: self.statusBar().showMessage(f"{name}: {state}")
        )
        worker.completed.connect(self._on_index_completed)
        worker.failed.connect(self._on_index_failed)
        worker.batch_done.connect(self._on_batch_done)

    def _on_index_completed(self, name: str) -> None:
        self._batch_completed.append(name)
        self._job_log.append_log(f"✓ {name} COMPLETED")

    def _on_index_failed(self, name: str, error: str) -> None:
        self._batch_failed.append((name, error))
        self._job_log.append_log(f"✗ {name} FAILED: {error}")

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
        self._on_refresh()
        self._job_log.append_log("=== Batch complete ===")

        # Show completion summary dialog
        completed = len(self._batch_completed)
        failed = len(self._batch_failed)
        total = self._batch_total
        mode = getattr(self, "_batch_mode", "Operation")

        if failed == 0:
            icon = QMessageBox.Icon.Information
            title = f"{mode} Complete"
            lines = [f"All {completed} of {total} items completed successfully."]
        elif completed == 0:
            icon = QMessageBox.Icon.Critical
            title = f"{mode} Failed"
            lines = [f"All {failed} of {total} items failed."]
        else:
            icon = QMessageBox.Icon.Warning
            title = f"{mode} Complete (with errors)"
            lines = [f"{completed} of {total} completed, {failed} failed."]

        # Shard savings from the defrag widget
        if self._defrag._total_shards_saved > 0:
            lines.append(f"\nShards reclaimed: {self._defrag._total_shards_saved}")
            lines.append(
                f"Budget: {self._conn.frozen_shard_count} / {self._conn.frozen_shard_limit}"
            )

        # List failures (up to 5)
        if self._batch_failed:
            lines.append("\nFailed items:")
            for name, error in self._batch_failed[:5]:
                short_err = error[:120] + "..." if len(error) > 120 else error
                lines.append(f"  • {name}: {short_err}")
            if len(self._batch_failed) > 5:
                lines.append(f"  ... and {len(self._batch_failed) - 5} more (see job log)")

        msg = QMessageBox(icon, title, "\n".join(lines), QMessageBox.StandardButton.Ok, self)
        save_btn = msg.addButton("Save Report", QMessageBox.ButtonRole.ActionRole)
        msg.exec()

        if msg.clickedButton() == save_btn:
            self._save_change_report()

    def _save_change_report(self) -> None:
        """Generate and save a post-operation change report."""
        path, selected_filter = QFileDialog.getSaveFileName(
            self, "Save Change Report",
            f"sharderator-change-report.html",
            "HTML Report (*.html);;JSON (*.json)",
        )
        if not path:
            return

        import json
        import time as _time

        mode = getattr(self, "_batch_mode", "Operation")
        completed = self._batch_completed
        failed = self._batch_failed
        shards_before = getattr(self, "_batch_shards_before", 0)
        shards_after = self._conn.frozen_shard_count
        shards_saved = self._defrag._total_shards_saved
        ts = _time.strftime("%Y-%m-%dT%H:%M:%S%z")
        cluster = self._conn.cluster_name

        if path.endswith(".json") or "JSON" in selected_filter:
            report = {
                "report_type": "change_report",
                "timestamp": ts,
                "cluster": cluster,
                "mode": mode,
                "shards_before": shards_before,
                "shards_after": shards_after,
                "shards_reclaimed": shards_saved,
                "frozen_limit": self._conn.frozen_shard_limit,
                "total_items": self._batch_total,
                "completed": completed,
                "failed": [{"name": n, "error": e} for n, e in failed],
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
        else:
            completed_rows = ""
            for name in completed:
                completed_rows += f"<tr><td>{name}</td><td style='color:#2e7d32'>✓ Completed</td></tr>\n"
            failed_rows = ""
            for name, error in failed:
                short = error[:200] + "..." if len(error) > 200 else error
                failed_rows += f"<tr><td>{name}</td><td style='color:#c62828'>✗ {short}</td></tr>\n"

            pct_before = shards_before / self._conn.frozen_shard_limit * 100 if self._conn.frozen_shard_limit else 0
            pct_after = shards_after / self._conn.frozen_shard_limit * 100 if self._conn.frozen_shard_limit else 0

            html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Sharderator — Change Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         max-width: 960px; margin: 20px auto; color: #333; font-size: 14px; }}
  h1 {{ color: #1a237e; border-bottom: 2px solid #1a237e; padding-bottom: 8px; }}
  h2 {{ color: #283593; margin-top: 24px; }}
  .meta {{ color: #666; font-size: 12px; margin-bottom: 16px; }}
  .summary {{ display: flex; gap: 16px; margin: 16px 0; }}
  .stat {{ flex: 1; border: 1px solid #ccc; border-radius: 6px; padding: 12px; text-align: center; }}
  .stat .label {{ font-size: 12px; color: #666; }}
  .stat .value {{ font-size: 24px; font-weight: bold; color: #1a237e; }}
  .stat .sub {{ font-size: 11px; color: #888; }}
  .good {{ color: #2e7d32 !important; }}
  table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 13px; }}
  th {{ background: #f5f5f5; text-align: left; padding: 6px 8px; border-bottom: 2px solid #ddd; }}
  td {{ padding: 4px 8px; border-bottom: 1px solid #eee; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  @media print {{ body {{ font-size: 11px; }} }}
</style></head><body>
<h1>Sharderator — {mode} Change Report</h1>
<div class="meta">Cluster: {cluster} &nbsp;|&nbsp; Completed: {ts}</div>

<div class="summary">
  <div class="stat"><div class="label">Before</div><div class="value">{shards_before:,}</div><div class="sub">shards ({pct_before:.1f}%)</div></div>
  <div class="stat"><div class="label">After</div><div class="value">{shards_after:,}</div><div class="sub">shards ({pct_after:.1f}%)</div></div>
  <div class="stat"><div class="label">Reclaimed</div><div class="value good">{shards_saved:,}</div><div class="sub">shards freed</div></div>
  <div class="stat"><div class="label">Result</div><div class="value">{len(completed)}/{self._batch_total}</div><div class="sub">{len(failed)} failed</div></div>
</div>

<h2>Completed ({len(completed)})</h2>
<table>
<tr><th>Index / Group</th><th>Status</th></tr>
{completed_rows if completed_rows else '<tr><td colspan="2" style="color:#888">None</td></tr>'}
</table>

{"<h2>Failed (" + str(len(failed)) + ")</h2><table><tr><th>Index / Group</th><th>Error</th></tr>" + failed_rows + "</table>" if failed else ""}

<div class="meta" style="margin-top:24px">Generated by Sharderator</div>
</body></html>"""
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)

        self._job_log.append_log(f"Change report saved to {path}")

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
        self._btn_refresh.setEnabled(not running)

    # --- Refresh ---

    @pyqtSlot()
    def _on_refresh(self) -> None:
        """Reload cluster metadata, frozen indices, and tracker jobs."""
        if not self._conn.connected:
            return
        self._conn._discover_cluster()
        self._update_connection_status()
        self._load_indices()
        self._load_tracker()
        self._job_log.append_log("Refreshed cluster state")

    # --- Filter / Select All ---

    @pyqtSlot()
    def _on_shrink_select_all(self) -> None:
        rows = self._shrink_proxy.visible_source_rows()
        self._table_model.select_visible(rows)

    @pyqtSlot()
    def _on_shrink_deselect_all(self) -> None:
        rows = self._shrink_proxy.visible_source_rows()
        self._table_model.deselect_visible(rows)

    @pyqtSlot()
    def _on_merge_select_all(self) -> None:
        self._merge_tree.select_visible()

    @pyqtSlot()
    def _on_merge_deselect_all(self) -> None:
        self._merge_tree.deselect_visible()

    @pyqtSlot()
    def _on_ctrl_a(self) -> None:
        """Ctrl+A: select all visible in the active tab."""
        current = self._tabs.currentIndex()
        if current == 1:  # Shrink Mode
            self._on_shrink_select_all()
        elif current == 2:  # Merge Mode
            self._on_merge_select_all()

    # --- Job Tracker ---

    def _load_tracker(self) -> None:
        """Load all jobs from the sharderator-tracker index."""
        if not self._conn.connected:
            return
        jobs = list_all_jobs(self._conn.client)
        self._tracker_model.set_jobs(jobs)
        if jobs:
            failed = sum(1 for j in jobs if j.state.value == "FAILED")
            self._lbl_tracker_summary.setText(
                f"{len(jobs)} tracked jobs ({failed} failed)"
            )
        else:
            self._lbl_tracker_summary.setText(
                "No tracked jobs — tracker index will be created on first operation"
            )

    def _on_tracker_resume(self, job: JobRecord) -> None:
        """Resume a tracked job from its last committed state."""
        self._job_log.append_log(f"Resuming {job.index_name} from {job.state.value}...")

        from sharderator.models.job import JobType

        if job.job_type == JobType.SHRINK:
            info = IndexInfo(
                name=job.index_name,
                shard_count=1,
                doc_count=job.expected_doc_count,
                store_size_bytes=0,
            )
            self._set_running(True)
            events = QtPipelineEvents()
            orch = Orchestrator(self._conn.client, self._config.operation, events)
            self._worker = ShrinkWorkerThread(orch, [info])
            self._wire_worker_signals(self._worker)
            self._worker.start()

        elif job.job_type == JobType.MERGE:
            # Reconstruct MergeGroup from persisted job record
            group = MergeGroup.from_job_record(job)

            if not group.source_indices:
                self._job_log.append_log(
                    "⚠ Cannot resume: no source metadata on job record. "
                    "Re-run from Merge Mode tab."
                )
                return

            self._set_running(True)
            self._defrag.set_indices(group.source_indices)
            # Register merge name mapping for defrag widget
            self._defrag.set_merge_groups({
                group.merged_index_name: [idx.name for idx in group.source_indices]
            })
            events = QtPipelineEvents()
            orch = MergeOrchestrator(
                self._conn.client, self._config.operation, events
            )
            self._worker = MergeWorkerThread(orch, [group])
            self._wire_worker_signals(self._worker)
            self._job_log.append_log(
                f"Resuming merge: {group.base_pattern}/{group.time_bucket} "
                f"({len(group.source_indices)} source indices)"
            )
            self._worker.start()

    def _on_tracker_delete(self, job: JobRecord) -> None:
        """Delete a job from the tracker index."""
        reply = QMessageBox.question(
            self,
            "Delete Tracked Job",
            f"Delete tracker record for {job.index_name}?\n\n"
            f"This does NOT clean up intermediate indices. "
            f"The job cannot be resumed after deletion.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        delete_job(self._conn.client, job.index_name)
        self._job_log.append_log(f"Deleted tracker record: {job.index_name}")
        self._load_tracker()

    def _on_tracker_history(self, job: JobRecord) -> None:
        """Show the state transition history for a job."""
        dlg = JobHistoryDialog(job, self)
        dlg.exec()

    # --- Export ---

    def _on_export_frozen_indices(self) -> None:
        """Export the frozen index table to CSV or JSON."""
        if not self._frozen_indices:
            QMessageBox.information(self, "No Data", "No frozen indices to export.")
            return
        path, selected_filter = QFileDialog.getSaveFileName(
            self, "Export Frozen Indices", "frozen-indices.csv",
            "CSV (*.csv);;JSON (*.json)"
        )
        if not path:
            return
        import json, csv as _csv
        if path.endswith(".json") or "JSON" in selected_filter:
            rows = [
                {"name": i.name, "shards": i.shard_count, "docs": i.doc_count,
                 "size_mb": round(i.store_size_mb, 1), "target_shards": i.target_shard_count}
                for i in self._frozen_indices
            ]
            with open(path, "w") as f:
                json.dump(rows, f, indent=2)
        else:
            with open(path, "w", newline="") as f:
                writer = _csv.writer(f)
                writer.writerow(["name", "shards", "docs", "size_mb", "target_shards"])
                for i in self._frozen_indices:
                    writer.writerow([i.name, i.shard_count, i.doc_count,
                                     round(i.store_size_mb, 1), i.target_shard_count])
        self._job_log.append_log(f"Exported {len(self._frozen_indices)} indices to {path}")

    def _on_export_merge_dryrun(self) -> None:
        """Export a merge dry-run report as JSON."""
        groups = self._merge_tree.get_selected_groups()
        if not groups:
            # Export all groups if none selected
            groups = self._merge_tree._groups
        if not groups:
            QMessageBox.information(self, "No Data", "Run Analyze Merges first.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Merge Dry Run", "sharderator-merge-plan.json", "JSON (*.json)"
        )
        if not path:
            return
        import json, time as _time
        from sharderator.engine.sizing import generate_sizing_report
        from sharderator.engine.preflight import run_dry_run_preflight
        report = generate_sizing_report(
            self._conn.client, mode=f"Merge ({self._config.operation.merge_granularity})",
            groups=groups, safety_margin=self._config.operation.disk_safety_margin,
            frozen_shards=self._conn.frozen_shard_count, frozen_limit=self._conn.frozen_shard_limit,
        )
        total_bytes = sum(g.total_size_bytes for g in groups) * 2
        issues = run_dry_run_preflight(
            self._conn.client, allow_yellow=self._config.operation.allow_yellow_cluster,
            disk_safety_margin=self._config.operation.disk_safety_margin,
            needed_bytes=total_bytes,
            ignore_circuit_breakers=self._config.operation.ignore_circuit_breakers,
        )
        report_data = report.to_dict()
        report_data["groups"] = [
            {
                "base_pattern": g.base_pattern,
                "time_bucket": g.time_bucket,
                "source_count": len(g.source_indices),
                "source_indices": [i.name for i in g.source_indices],
                "shard_reduction": g.shard_reduction,
                "total_size_bytes": g.total_size_bytes,
                "mapping_conflicts": g.mapping_conflicts,
            }
            for g in groups
        ]
        report_data["preflight_issues"] = issues
        report_data["preflight_passed"] = len(issues) == 0
        report_data["timestamp"] = _time.strftime("%Y-%m-%dT%H:%M:%S%z")
        with open(path, "w") as f:
            json.dump(report_data, f, indent=2)
        self._job_log.append_log(f"Exported merge plan ({len(groups)} groups) to {path}")

    def _on_export_tracker(self) -> None:
        """Export all tracked jobs as JSON."""
        jobs = list_all_jobs(self._conn.client)
        if not jobs:
            QMessageBox.information(self, "No Jobs", "No tracked jobs to export.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Job History", "sharderator-history.json", "JSON (*.json)"
        )
        if not path:
            return
        import json, time as _time
        output = {
            "exported_at": _time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "cluster": self._conn.cluster_name,
            "job_count": len(jobs),
            "jobs": [j.to_dict() for j in jobs],
        }
        with open(path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        self._job_log.append_log(f"Exported {len(jobs)} jobs to {path}")

    def _on_export_single_job(self, job: JobRecord) -> None:
        """Export a single job record as JSON (from context menu)."""
        safe_name = job.index_name.replace("/", "-").replace(":", "-")
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Job", f"sharderator-job-{safe_name}.json", "JSON (*.json)"
        )
        if not path:
            return
        import json
        with open(path, "w") as f:
            json.dump(job.to_dict(), f, indent=2, default=str)
        self._job_log.append_log(f"Exported job {job.index_name} to {path}")

    def closeEvent(self, event) -> None:
        """Graceful shutdown — cancel running pipeline and wait for thread."""
        if self._worker and self._worker.isRunning():
            reply = QMessageBox.question(
                self,
                "Operation Running",
                "A pipeline is running. Cancel and exit?\n\n"
                "The job can be resumed on next launch.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                event.ignore()
                return

            self._on_cancel()
            self._worker.wait(10000)  # 10s timeout
            if self._worker.isRunning():
                self._worker.terminate()

        event.accept()
