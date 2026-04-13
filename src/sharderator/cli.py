"""CLI entry point for Sharderator — headless operation, no PyQt6 required."""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import sys

from sharderator.client.connection import ClusterConnection
from sharderator.client.queries import list_frozen_indices
from sharderator.engine.events import PipelineEvents
from sharderator.engine.merge_analyzer import propose_merges
from sharderator.engine.preflight import run_dry_run_preflight
from sharderator.util.config import AppConfig, ConnectionConfig, OperationConfig
from sharderator.util.logging import setup_logging, get_logger

log = get_logger(__name__)

# Terminal defrag rendering
ANSI_COLORS = {
    "PENDING": "\033[90m", "ANALYZING": "\033[94m", "RESTORING": "\033[34m",
    "AWAITING_RECOVERY": "\033[34m", "SHRINKING": "\033[33m",
    "AWAITING_SHRINK": "\033[33m", "AWAITING_REINDEX": "\033[33m",
    "MERGING": "\033[33m", "AWAITING_MERGE": "\033[33m",
    "FORCE_MERGING": "\033[93m", "SNAPSHOTTING": "\033[36m",
    "AWAITING_SNAPSHOT": "\033[36m", "REMOUNTING": "\033[96m",
    "VERIFYING": "\033[96m", "SWAPPING": "\033[96m",
    "CLEANING_UP": "\033[92m", "COMPLETED": "\033[32m", "FAILED": "\033[31m",
}
RESET = "\033[0m"


class TerminalDefrag:
    """Terminal block visualization with grid wrapping for wide workloads."""

    def __init__(self):
        self._states: dict[str, str] = {}
        self._shards: dict[str, int] = {}
        self._order: list[str] = []
        self._last_line_count: int = 0

    def set_indices(self, names_and_shards: list[tuple[str, int]]) -> None:
        self._order = [n for n, _ in names_and_shards]
        self._shards = {n: s for n, s in names_and_shards}
        self._states = {n: "PENDING" for n in self._order}
        self._last_line_count = 0

    def update_state(self, name: str, state: str) -> None:
        if name in self._states:
            self._states[name] = state
            self._render()

    def _render(self) -> None:
        cols = shutil.get_terminal_size().columns - 4
        lines: list[str] = []
        current_line: list[str] = []
        line_len = 0

        for name in self._order:
            state = self._states.get(name, "PENDING")
            color = ANSI_COLORS.get(state, "\033[90m")
            width = min(self._shards.get(name, 1), 20)
            block = f"{color}{'▓' * width}{RESET}"

            if line_len + width > cols and current_line:
                lines.append("".join(current_line))
                current_line = []
                line_len = 0

            current_line.append(block)
            line_len += width

        if current_line:
            lines.append("".join(current_line))

        # Clear previous output and redraw
        if self._last_line_count > 0:
            sys.stdout.write(f"\033[{self._last_line_count}A\033[J")

        output = "\n".join(f"  {line}" for line in lines)
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
        self._last_line_count = len(lines)


class CliEvents:
    """PipelineEvents implementation for CLI — prints to stdout."""

    def __init__(self, defrag: TerminalDefrag | None = None):
        self._defrag = defrag

    def on_progress(self, index_name: str, pct: float) -> None:
        pass

    def on_state_changed(self, index_name: str, state: str) -> None:
        if self._defrag:
            self._defrag.update_state(index_name, state)

    def on_log(self, message: str) -> None:
        print(f"  {message}")

    def on_completed(self, index_name: str) -> None:
        print(f"\n  >> {index_name} COMPLETED")

    def on_failed(self, index_name: str, error: str) -> None:
        print(f"\n  !! {index_name} FAILED: {error}")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        prog="sharderator-cli",
        description="Frozen tier shard consolidation for Elasticsearch",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List frozen indices and shard counts")
    _add_connection_args(p_list)
    p_list.add_argument("--json", action="store_true", help="Output as JSON")
    p_list.add_argument("--csv", action="store_true", help="Output as CSV")

    p_status = sub.add_parser("status", help="Show cluster frozen shard budget")
    _add_connection_args(p_status)

    p_shrink = sub.add_parser("shrink", help="Shrink individual frozen indices")
    _add_connection_args(p_shrink)
    _add_operation_args(p_shrink)
    p_shrink.add_argument("--index", "-i", nargs="+", help="Specific index names")
    p_shrink.add_argument("--min-shards", type=int, default=2)
    p_shrink.add_argument("--max-indices", type=int, default=0)
    p_shrink.add_argument("--dry-run", action="store_true")
    p_shrink.add_argument("--order", choices=["smallest-first", "largest-first", "as-is"], default="smallest-first")
    p_shrink.add_argument("--output", choices=["table", "json"], default="table", help="Dry-run output format")

    p_merge = sub.add_parser("merge", help="Merge frozen indices by time bucket")
    _add_connection_args(p_merge)
    _add_operation_args(p_merge)
    p_merge.add_argument("--granularity", choices=["monthly", "quarterly", "yearly"], default="monthly")
    p_merge.add_argument("--pattern", help="Only merge groups matching this base pattern")
    p_merge.add_argument("--max-groups", type=int, default=0)
    p_merge.add_argument("--dry-run", action="store_true")
    p_merge.add_argument("--order", choices=["smallest-first", "largest-first", "as-is"], default="smallest-first")
    p_merge.add_argument("--priority", help="Comma-separated base patterns to process first")
    p_merge.add_argument("--output", choices=["table", "json"], default="table", help="Dry-run output format")

    p_report = sub.add_parser("report", help="Export job history from tracker")
    _add_connection_args(p_report)
    p_report.add_argument("--index", help="Specific job by index name")
    p_report.add_argument("--state", choices=["COMPLETED", "FAILED", "all"], default="all",
                          help="Filter by job state")

    args = parser.parse_args()
    conn = _connect(args)

    try:
        if args.command == "list":
            _cmd_list(conn, args)
        elif args.command == "status":
            _cmd_status(conn, args)
        elif args.command == "shrink":
            _cmd_shrink(conn, args)
        elif args.command == "merge":
            _cmd_merge(conn, args)
        elif args.command == "report":
            _cmd_report(conn, args)
    finally:
        conn.disconnect()


def _cmd_list(conn: ClusterConnection, args) -> None:
    indices = list_frozen_indices(conn.client)
    if getattr(args, "json", False):
        import json
        rows = [
            {"name": i.name, "shards": i.shard_count, "docs": i.doc_count, "size_mb": round(i.store_size_mb, 1)}
            for i in indices
        ]
        print(json.dumps(rows, indent=2))
    elif getattr(args, "csv", False):
        import csv
        writer = csv.writer(sys.stdout)
        writer.writerow(["name", "shards", "docs", "size_mb"])
        for i in indices:
            writer.writerow([i.name, i.shard_count, i.doc_count, round(i.store_size_mb, 1)])
    else:
        print(f"{'Index':<60} {'Shards':>7} {'Docs':>12} {'Size MB':>10}")
        print("-" * 93)
        for i in indices:
            print(f"{i.name:<60} {i.shard_count:>7} {i.doc_count:>12,} {i.store_size_mb:>10.1f}")
        print(f"\nTotal: {len(indices)} frozen indices, {sum(i.shard_count for i in indices)} shards")


def _cmd_status(conn: ClusterConnection, args) -> None:
    print(f"Cluster:        {conn.cluster_name}")
    print(f"Health:         {conn.cluster_health}")
    print(f"Frozen shards:  {conn.frozen_shard_count} / {conn.frozen_shard_limit}")
    print(f"Snapshot repos: {', '.join(conn.snapshot_repos)}")
    from sharderator.client.tracker import TRACKING_INDEX
    try:
        count = conn.client.count(index=TRACKING_INDEX)
        print(f"Pending jobs:   {count['count']}")
    except Exception:
        print("Pending jobs:   0 (no tracker index)")


def _cmd_shrink(conn: ClusterConnection, args) -> None:
    cfg = _build_config(args)
    indices = list_frozen_indices(conn.client)
    if args.index:
        indices = [i for i in indices if i.name in args.index]
    indices = [i for i in indices if i.shard_count >= args.min_shards]
    if args.max_indices > 0:
        indices = indices[: args.max_indices]
    if not indices:
        print("No eligible indices found.")
        return

    # Item 6: Size-based ordering
    from sharderator.engine.sizing import sort_indices, generate_sizing_report
    indices = sort_indices(indices, getattr(args, "order", "smallest-first"))

    print(f"Processing {len(indices)} indices...")
    if args.dry_run:
        # Item 5: Pre-run sizing report
        report = generate_sizing_report(
            conn.client, mode="Shrink", indices=indices,
            safety_margin=cfg.disk_safety_margin,
            frozen_shards=conn.frozen_shard_count,
            frozen_limit=conn.frozen_shard_limit,
        )
        needed = max(i.store_size_bytes for i in indices) if indices else 0
        issues = run_dry_run_preflight(
            conn.client, allow_yellow=cfg.allow_yellow_cluster,
            disk_safety_margin=cfg.disk_safety_margin, needed_bytes=needed,
            ignore_circuit_breakers=cfg.ignore_circuit_breakers,
        )
        output_format = getattr(args, "output", "table")
        if output_format == "json":
            import json, time as _time
            report_data = report.to_dict()
            report_data["indices"] = [
                {
                    "name": i.name,
                    "shard_count": i.shard_count,
                    "target_shard_count": i.target_shard_count,
                    "shard_reduction": i.shard_reduction,
                    "store_size_bytes": i.store_size_bytes,
                    "needs_reindex": i.needs_reindex,
                }
                for i in indices
            ]
            report_data["preflight_issues"] = issues
            report_data["preflight_passed"] = len(issues) == 0
            report_data["timestamp"] = _time.strftime("%Y-%m-%dT%H:%M:%S%z")
            print(json.dumps(report_data, indent=2))
        else:
            print(report.format())
            print()
            if issues:
                print("\n!! Pre-flight issues:")
                for issue in issues:
                    print(f"  {issue}")
            else:
                print("\n>> All pre-flight checks passed")
            for i in indices:
                print(f"  {i.name}: {i.shard_count} -> {i.target_shard_count} shards")
        return

    defrag = TerminalDefrag()
    defrag.set_indices([(i.name, i.shard_count) for i in indices])
    events = CliEvents(defrag)

    from sharderator.engine.orchestrator import Orchestrator
    orch = Orchestrator(conn.client, cfg, events)
    _setup_signal_handlers(orch)
    for idx, info in enumerate(indices, 1):
        print(f"\n[{idx}/{len(indices)}] {info.name} ({info.shard_count} shards)")
        orch.run(info)
    print()


def _cmd_merge(conn: ClusterConnection, args) -> None:
    cfg = _build_config(args)
    indices = list_frozen_indices(conn.client)
    groups = propose_merges(indices, args.granularity)
    if args.pattern:
        groups = [g for g in groups if args.pattern in g.base_pattern]
    if args.max_groups > 0:
        groups = groups[: args.max_groups]
    if not groups:
        print("No eligible merge groups found.")
        return

    # Item 7: Priority queuing
    from sharderator.engine.sizing import (
        sort_merge_groups, prioritize_groups, generate_sizing_report,
    )
    if args.priority:
        priority_patterns = [p.strip() for p in args.priority.split(",")]
        groups = prioritize_groups(groups, priority_patterns)

    # Item 6: Size-based ordering (within priority tiers)
    groups = sort_merge_groups(groups, getattr(args, "order", "smallest-first"))

    total_saved = sum(g.shard_reduction for g in groups)
    print(f"Found {len(groups)} merge groups, saving {total_saved} shards")
    if args.dry_run:
        # Item 5: Pre-run sizing report
        report = generate_sizing_report(
            conn.client, mode=f"Merge ({args.granularity})", groups=groups,
            safety_margin=cfg.disk_safety_margin,
            frozen_shards=conn.frozen_shard_count,
            frozen_limit=conn.frozen_shard_limit,
        )
        needed = sum(g.total_size_bytes for g in groups) * 2
        issues = run_dry_run_preflight(
            conn.client, allow_yellow=cfg.allow_yellow_cluster,
            disk_safety_margin=cfg.disk_safety_margin, needed_bytes=needed,
            ignore_circuit_breakers=cfg.ignore_circuit_breakers,
        )
        output_format = getattr(args, "output", "table")
        if output_format == "json":
            import json, time as _time
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
            print(json.dumps(report_data, indent=2))
        else:
            print(report.format())
            print()
            if issues:
                print("\n!! Pre-flight issues:")
                for issue in issues:
                    print(f"  {issue}")
            else:
                print("\n>> All pre-flight checks passed")
            for g in groups:
                line = f"  {g.base_pattern}/{g.time_bucket}: {len(g.source_indices)} -> 1 ({g.shard_reduction} saved)"
                print(line)
                for conflict in g.mapping_conflicts:
                    print(f"    ⚠ {conflict}")
        return

    all_sources = [(idx.name, idx.shard_count) for g in groups for idx in g.source_indices]
    defrag = TerminalDefrag()
    defrag.set_indices(all_sources)
    events = CliEvents(defrag)

    from sharderator.engine.merge_orchestrator import MergeOrchestrator
    orch = MergeOrchestrator(conn.client, cfg, events)
    _setup_signal_handlers(orch)
    for idx, group in enumerate(groups, 1):
        print(f"\n[{idx}/{len(groups)}] {group.base_pattern}/{group.time_bucket} ({len(group.source_indices)} indices)")
        orch.run(group)
    print()


def _cmd_report(conn: ClusterConnection, args) -> None:
    """Export job history from the tracker index."""
    import json
    import time as _time
    from sharderator.client.tracker import list_all_jobs, load_job

    if args.index:
        job = load_job(conn.client, args.index)
        if job:
            print(json.dumps(job.to_dict(), indent=2, default=str))
        else:
            print(f"No job found for {args.index}", file=sys.stderr)
        return

    jobs = list_all_jobs(conn.client)
    if args.state != "all":
        jobs = [j for j in jobs if j.state.value == args.state]

    output = {
        "exported_at": _time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "cluster": conn.cluster_name,
        "job_count": len(jobs),
        "jobs": [j.to_dict() for j in jobs],
    }
    print(json.dumps(output, indent=2, default=str))


# --- Helpers ---

def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("connection")
    g.add_argument("--cloud-id", help="Elastic Cloud deployment ID")
    g.add_argument("--hosts", nargs="+", help="Elasticsearch host URLs")
    g.add_argument("--api-key", help="API key (or set ES_API_KEY env var)")
    g.add_argument("--username", help="Basic auth username")
    g.add_argument("--password", help="Basic auth password (or set ES_PASSWORD env var)")
    g.add_argument("--no-verify-certs", action="store_true")


def _add_operation_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("operation")
    g.add_argument("--snapshot-repo", help="Override snapshot repository")
    g.add_argument("--working-tier", default="data_warm,data_hot,data_content")
    g.add_argument("--safety-margin", type=float, default=0.30)
    g.add_argument("--recovery-timeout", type=int, default=30)
    g.add_argument("--reindex-rps", type=float, default=5000)
    g.add_argument("--restore-batch-size", type=int, default=3)
    g.add_argument("--allow-yellow", action="store_true", default=True)
    g.add_argument("--no-allow-yellow", action="store_false", dest="allow_yellow")
    g.add_argument("--delete-old-snapshots", action="store_true")
    g.add_argument("--ignore-circuit-breakers", action="store_true")


def _connect(args) -> ClusterConnection:
    conn = ClusterConnection()
    api_key = getattr(args, "api_key", None) or os.environ.get("ES_API_KEY")
    password = getattr(args, "password", None) or os.environ.get("ES_PASSWORD")
    cfg = ConnectionConfig(
        cloud_id=getattr(args, "cloud_id", "") or "",
        hosts=getattr(args, "hosts", None) or [],
        username=getattr(args, "username", "") or "",
        use_api_key=bool(api_key),
        verify_certs=not getattr(args, "no_verify_certs", False),
    )
    if api_key:
        AppConfig.store_secret("api_key", api_key)
    if password:
        AppConfig.store_secret("password", password)
    conn.connect(cfg)
    return conn


def _build_config(args) -> OperationConfig:
    return OperationConfig(
        working_tier=getattr(args, "working_tier", "data_warm,data_hot,data_content"),
        snapshot_repo=getattr(args, "snapshot_repo", "") or "",
        recovery_timeout_minutes=getattr(args, "recovery_timeout", 30),
        disk_safety_margin=getattr(args, "safety_margin", 0.30),
        restore_batch_size=getattr(args, "restore_batch_size", 3),
        reindex_requests_per_second=getattr(args, "reindex_rps", 5000),
        allow_yellow_cluster=getattr(args, "allow_yellow", True),
        delete_old_snapshots=getattr(args, "delete_old_snapshots", False),
        ignore_circuit_breakers=getattr(args, "ignore_circuit_breakers", False),
    )


def _run_preflight(conn, cfg, needed_bytes):
    issues = run_dry_run_preflight(
        conn.client, allow_yellow=cfg.allow_yellow_cluster,
        disk_safety_margin=cfg.disk_safety_margin, needed_bytes=needed_bytes,
        ignore_circuit_breakers=cfg.ignore_circuit_breakers,
    )
    if issues:
        print("\n!! Pre-flight issues:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n>> All pre-flight checks passed")


def _setup_signal_handlers(orchestrator):
    def handler(signum, frame):
        print("\nCaught signal, cancelling after current stage...")
        orchestrator.cancel()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


if __name__ == "__main__":
    main()
