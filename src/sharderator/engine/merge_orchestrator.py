"""Merge orchestrator — drives the multi-index merge pipeline."""

from __future__ import annotations

import threading
import time

from elasticsearch import Elasticsearch

from sharderator.client.tracker import save_job, delete_job, load_job
from sharderator.engine.cleaner import cleanup_merge
from sharderator.engine.events import PipelineEvents, NullEvents
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
from sharderator.engine.sizing import classify_merge_group, get_tier_profile
from sharderator.engine.snapshotter import snapshot
from sharderator.engine.swapper import swap_data_stream_merged
from sharderator.models.index_info import IndexInfo
from sharderator.models.job import JobRecord, JobState, JobType
from sharderator.util.config import OperationConfig
from sharderator.util.logging import get_logger

log = get_logger(__name__)


class MergeOrchestrator:
    """Runs the merge pipeline for a MergeGroup."""

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
            self._events.on_failed(job_name, error_msg)
            log.error("merge_pipeline_failed", group=job_name, error=error_msg)

    def _run_pipeline(self, group: MergeGroup, job: JobRecord) -> None:
        c = self._client
        cfg = self._config
        ev = self._events
        job_name = group.merged_index_name

        def _progress(pct: float) -> None:
            ev.on_progress(job_name, pct)

        def _check_cancel() -> None:
            if self._cancel_event.is_set():
                raise RuntimeError("Operation cancelled by user")
            check_cluster_health_mid_operation(c)

        def _save() -> None:
            save_job(c, job)

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

        # Apply size-tier adaptive timeouts and throttle
        tier = classify_merge_group(group)
        tier_profile = get_tier_profile(tier)
        recovery_timeout = max(cfg.recovery_timeout_minutes, tier_profile.recovery_timeout_minutes)
        reindex_rps = tier_profile.reindex_rps if tier_profile.reindex_rps > 0 else cfg.reindex_requests_per_second

        if job.state == JobState.ANALYZING:
            restore_names = restore_merge_sources(
                c, enriched, group, job, cfg.working_tier,
                recovery_timeout, _progress,
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
                reindex_rps,
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
            new_docs = _wait_for_frozen_stats(c, job.new_frozen_index)
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
        self._events.on_completed(job_name)
        log.info("merge_pipeline_completed", group=job_name)

    def _emit_state(self, name: str, state: JobState) -> None:
        self._events.on_state_changed(name, state.value)
        self._events.on_log(f"{name}: {state.value}")


def _wait_for_frozen_stats(
    client: Elasticsearch, index: str, timeout_minutes: int = 5,
) -> int:
    """Wait for a newly mounted frozen index to report doc stats."""
    deadline = time.time() + timeout_minutes * 60
    last_error = ""
    while time.time() < deadline:
        try:
            client.cluster.health(index=index, wait_for_status="yellow", timeout="10s")
            stats = client.indices.stats(index=index)
            primaries = stats.get("_all", {}).get("primaries", {})
            docs = primaries.get("docs")
            if docs is not None and "count" in docs:
                return docs["count"]
            last_error = f"stats missing docs key"
        except Exception as e:
            last_error = str(e)
        time.sleep(5)
    raise RuntimeError(
        f"Timed out waiting for frozen index {index} stats after {timeout_minutes}m: {last_error}"
    )
