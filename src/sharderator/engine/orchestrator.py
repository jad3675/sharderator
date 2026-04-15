"""Orchestrator — drives the consolidation pipeline for a single index."""

from __future__ import annotations

import threading

from elasticsearch import Elasticsearch

from sharderator.client.tracker import save_job, load_job, delete_job
from sharderator.engine.analyzer import analyze
from sharderator.engine.cleaner import cleanup
from sharderator.engine.events import PipelineEvents, NullEvents
from sharderator.engine.mounter import remount
from sharderator.engine.preflight import (
    check_cluster_health,
    check_circuit_breakers,
    check_cluster_health_mid_operation,
)
from sharderator.engine.restorer import restore, wait_for_recovery
from sharderator.engine.shrinker import shrink, wait_for_shrink, wait_for_reindex
from sharderator.engine.sizing import classify_index, get_tier_profile
from sharderator.engine.snapshotter import snapshot
from sharderator.engine.swapper import swap_data_stream
from sharderator.engine.verifier import verify
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState, JobType
from sharderator.util.config import OperationConfig
from sharderator.util.logging import get_logger

log = get_logger(__name__)


class Orchestrator:
    """Runs the full consolidation pipeline for one index."""

    def __init__(
        self,
        client: Elasticsearch,
        config: OperationConfig,
        events: PipelineEvents | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._events = events or NullEvents()
        self._cancel_event = threading.Event()

    def cancel(self) -> None:
        self._cancel_event.set()

    def run(self, info: IndexInfo, job: JobRecord | None = None) -> None:
        if job is None:
            existing = load_job(self._client, info.name)
            if existing and existing.state == JobState.FAILED:
                delete_job(self._client, info.name)
                existing = None
            job = existing or JobRecord(index_name=info.name, job_type=JobType.SHRINK)

        try:
            self._run_pipeline(info, job)
            delete_job(self._client, info.name)
        except Exception as e:
            error_msg = str(e)
            try:
                job.transition(JobState.FAILED, error=error_msg)
            except ValueError:
                job.state = JobState.FAILED
                job.error = error_msg
            save_job(self._client, job)
            self._emit_state(info.name, JobState.FAILED)
            self._events.on_failed(info.name, error_msg)
            log.error("pipeline_failed", index=info.name, error=error_msg)

    def _run_pipeline(self, info: IndexInfo, job: JobRecord) -> None:
        c = self._client
        cfg = self._config
        ev = self._events

        def _progress(pct: float) -> None:
            ev.on_progress(info.name, pct)

        def _check_cancel() -> None:
            if self._cancel_event.is_set():
                raise RuntimeError("Operation cancelled by user")
            check_cluster_health_mid_operation(c)

        def _save() -> None:
            save_job(c, job)

        if job.state == JobState.PENDING:
            check_cluster_health(c, allow_yellow=cfg.allow_yellow_cluster)
            if not cfg.ignore_circuit_breakers:
                check_circuit_breakers(c, wait_timeout_minutes=cfg.circuit_breaker_wait_minutes)
            info = analyze(c, info, job)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()

        # Apply size-tier adaptive timeouts and throttle
        tier = classify_index(info)
        tier_profile = get_tier_profile(tier)
        recovery_timeout = max(cfg.recovery_timeout_minutes, tier_profile.recovery_timeout_minutes)
        reindex_rps = tier_profile.reindex_rps if tier_profile.reindex_rps > 0 else cfg.reindex_requests_per_second

        repo = cfg.snapshot_repo or job.repository_name

        if job.state == JobState.ANALYZING:
            restore_name = restore(c, info, job, cfg.working_tier, cfg.disk_safety_margin)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()
            wait_for_recovery(c, restore_name, job, recovery_timeout, _progress)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()
        else:
            restore_name = job.restore_index

        if job.state == JobState.AWAITING_RECOVERY:
            shrunk_name = shrink(c, info, job, restore_name, reindex_rps)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()
            if info.needs_reindex:
                wait_for_reindex(c, job, recovery_timeout, _progress)
            else:
                wait_for_shrink(c, shrunk_name, job, recovery_timeout)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()
        else:
            shrunk_name = job.shrunk_index

        if job.state in (JobState.AWAITING_SHRINK, JobState.AWAITING_REINDEX):
            snap_name = snapshot(c, shrunk_name, repo, job, info.original_index_name, _progress)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()
        else:
            snap_name = job.new_snapshot_name

        if job.state == JobState.AWAITING_SNAPSHOT:
            remount(c, info, job, snap_name, repo)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.REMOUNTING:
            verify(c, info, job)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.VERIFYING:
            swap_data_stream(c, info, job)
            self._emit_state(info.name, job.state)
            _save()
            _check_cancel()

        if job.state == JobState.SWAPPING:
            cleanup(c, info, job, cfg.delete_old_snapshots)
            self._emit_state(info.name, job.state)
            _save()

        job.transition(JobState.COMPLETED)
        self._emit_state(info.name, JobState.COMPLETED)
        self._events.on_completed(info.name)
        log.info("pipeline_completed", index=info.name)

    def _emit_state(self, index_name: str, state: JobState) -> None:
        self._events.on_state_changed(index_name, state.value)
        self._events.on_log(f"{index_name}: {state.value}")
