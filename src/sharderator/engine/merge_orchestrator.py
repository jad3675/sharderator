"""Merge orchestrator — drives the multi-index merge pipeline."""

from __future__ import annotations

from PyQt6.QtCore import QObject, pyqtSignal

from elasticsearch import Elasticsearch

from sharderator.client.tracker import save_job, delete_job, load_job
from sharderator.engine.cleaner import cleanup_merge
from sharderator.engine.merge_analyzer import MergeGroup
from sharderator.engine.merger import (
    analyze_merge_group,
    merge_reindex,
    restore_merge_sources,
)
from sharderator.engine.mounter import remount_merged
from sharderator.engine.preflight import (
    check_cluster_health,
    check_circuit_breakers,
    check_cluster_health_mid_operation,
)
from sharderator.engine.snapshotter import snapshot
from sharderator.engine.swapper import swap_data_stream_merged
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState, JobType
from sharderator.util.config import OperationConfig
from sharderator.util.logging import get_logger

log = get_logger(__name__)


class MergeOrchestrator(QObject):
    progress = pyqtSignal(str, float)
    state_changed = pyqtSignal(str, str)
    log_message = pyqtSignal(str)
    completed = pyqtSignal(str)
    failed = pyqtSignal(str, str)

    def __init__(
        self,
        client: Elasticsearch,
        config: OperationConfig,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._client = client
        self._config = config
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self, group: MergeGroup) -> None:
        job_name = group.merged_index_name
        existing = load_job(self._client, job_name)
        if existing and existing.state == JobState.FAILED:
            delete_job(self._client, job_name)
            existing = None
        job = existing or JobRecord(index_name=job_name, job_type=JobType.MERGE)

        try:
            self._run_pipeline(group, job)
            delete_job(self._client, job_name)
        except Exception as e:
            error_msg = str(e)
            try:
                job.transition(JobState.FAILED, error=error_msg)
            except ValueError:
                job.state = JobState.FAILED
                job.error = error_msg
            save_job(self._client, job)
            self._emit_state(job_name, JobState.FAILED)
            self.failed.emit(job_name, error_msg)
            log.error("merge_pipeline_failed", group=job_name, error=error_msg)

    def _run_pipeline(self, group: MergeGroup, job: JobRecord) -> None:
        c = self._client
        cfg = self._config
        job_name = group.merged_index_name

        def _progress(pct: float) -> None:
            self.progress.emit(job_name, pct)

        def _check_cancel() -> None:
            if self._cancelled:
                raise RuntimeError("Operation cancelled by user")
            check_cluster_health_mid_operation(c)

        def _save() -> None:
            save_job(c, job)

        # Pre-flight on fresh jobs
        if job.state == JobState.PENDING:
            check_cluster_health(c, allow_yellow=cfg.allow_yellow_cluster)
            if not cfg.ignore_circuit_breakers:
                check_circuit_breakers(c)
            enriched = analyze_merge_group(c, group, job)
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()
        else:
            if job.enriched_metadata:
                enriched = [IndexInfo(**meta) for meta in job.enriched_metadata]
            else:
                from sharderator.client.queries import enrich_index_info
                enriched = [enrich_index_info(c, i) for i in group.source_indices]

        repo = cfg.snapshot_repo or job.repository_name

        if job.state == JobState.ANALYZING:
            restore_names = restore_merge_sources(
                c, enriched, group, job, cfg.working_tier,
                cfg.recovery_timeout_minutes, _progress,
                cfg.restore_batch_size, cfg.disk_safety_margin,
            )
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()
        else:
            restore_names = job.restore_indices

        if job.state == JobState.AWAITING_RECOVERY:
            merged_name = merge_reindex(
                c, restore_names, enriched, group, job, _progress,
                cfg.reindex_requests_per_second,
            )
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()
        else:
            merged_name = job.shrunk_index

        if job.state == JobState.AWAITING_MERGE:
            snap_name = snapshot(
                c, merged_name, repo, job,
                f"{group.base_pattern}-{group.time_bucket}",
                _progress,
            )
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()
        else:
            snap_name = job.new_snapshot_name

        if job.state == JobState.AWAITING_SNAPSHOT:
            final_name = f"partial-.ds-{group.base_pattern}-{group.time_bucket}-merged"
            remount_merged(c, job, snap_name, repo, merged_name, final_name)
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.REMOUNTING:
            job.transition(JobState.VERIFYING)
            new_stats = c.indices.stats(index=job.new_frozen_index)
            new_docs = new_stats["_all"]["primaries"]["docs"]["count"]
            if new_docs != job.expected_doc_count:
                raise RuntimeError(
                    f"Merge doc count mismatch: expected={job.expected_doc_count}, got={new_docs}"
                )
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.VERIFYING:
            swap_data_stream_merged(c, job, enriched)
            self._emit_state(job_name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.SWAPPING:
            cleanup_merge(c, job, enriched, cfg.delete_old_snapshots)
            self._emit_state(job_name, job.state)
            _save()

        job.transition(JobState.COMPLETED)
        self._emit_state(job_name, JobState.COMPLETED)
        self.completed.emit(job_name)
        log.info("merge_pipeline_completed", group=job_name)

    def _emit_state(self, name: str, state: JobState) -> None:
        self.state_changed.emit(name, state.value)
        self.log_message.emit(f"{name}: {state.value}")
