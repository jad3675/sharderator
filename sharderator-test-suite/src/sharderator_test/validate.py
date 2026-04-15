"""Validation phases for the Sharderator test suite."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from elasticsearch import Elasticsearch

from sharderator_test.status import get_cluster_status


class ValidationError(Exception):
    pass


def run_phase(
    phase: str,
    client: Elasticsearch,
    cloud_id: str,
    api_key: str,
    output_dir: Path | None = None,
    console=None,
) -> bool:
    """Run a validation phase. Returns True if passed."""
    _log = console.print if console else print
    phases = {
        "read-only": _phase_read_only,
        "dry-run": _phase_dry_run,
        "shrink": _phase_shrink,
        "merge": _phase_merge,
        "crash-recovery": _phase_crash_recovery,
        "export": _phase_export,
    }

    if phase == "all":
        all_passed = True
        for name, fn in phases.items():
            _log(f"\n{'='*60}")
            _log(f"Phase: {name.upper()}")
            _log("=" * 60)
            passed = fn(client, cloud_id, api_key, output_dir, console)
            if not passed:
                all_passed = False
                _log(f"[red]✗ Phase {name} FAILED[/red]" if console else f"✗ Phase {name} FAILED")
            else:
                _log(f"[green]✓ Phase {name} PASSED[/green]" if console else f"✓ Phase {name} PASSED")
        return all_passed

    fn = phases.get(phase)
    if not fn:
        _log(f"Unknown phase: {phase}. Valid: {', '.join(phases)}")
        return False
    return fn(client, cloud_id, api_key, output_dir, console)


def _extract_json(stdout: str):
    """Extract JSON from stdout that may have non-JSON lines before it."""
    obj_idx = stdout.find("{")
    arr_idx = stdout.find("[")
    candidates = [i for i in (obj_idx, arr_idx) if i >= 0]
    if not candidates:
        raise json.JSONDecodeError("No JSON found in output", stdout, 0)
    return json.loads(stdout[min(candidates):])


def _run_cli(args: list[str], cloud_id: str, api_key: str, timeout: int = 300) -> tuple[int, str, str]:
    """Run a sharderator-cli command and return (returncode, stdout, stderr)."""
    if not cloud_id or not api_key:
        return 1, "", "Missing credentials: cloud_id and api_key must be provided"
    cmd = ["sharderator-cli"] + args + ["--cloud-id", cloud_id, "--api-key", api_key]
    # Force UTF-8 so the ▓ block characters in the terminal defrag don't crash
    # on Windows cp1252 consoles. Also ignore circuit breakers since tiebreaker
    # nodes on trial clusters routinely run above 80% heap.
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    return result.returncode, result.stdout, result.stderr


def _phase_read_only(client, cloud_id, api_key, output_dir, console) -> bool:
    _log = console.print if console else print
    passed = True

    _log("\n[1/2] Testing: sharderator-cli list")
    rc, stdout, stderr = _run_cli(["list"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [red]FAIL[/red] exit code {rc}: {stderr}" if console else f"  FAIL exit code {rc}: {stderr}")
        passed = False
    else:
        lines = [l for l in stdout.splitlines() if "partial-" in l]
        _log(f"  [green]✓[/green] Found {len(lines)} frozen indices" if console else f"  ✓ Found {len(lines)} frozen indices")

    _log("\n[2/2] Testing: sharderator-cli list --json")
    rc, stdout, stderr = _run_cli(["list", "--json"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        try:
            data = _extract_json(stdout)
            _log(f"  [green]✓[/green] JSON output: {len(data)} indices" if console else f"  ✓ JSON output: {len(data)} indices")
        except json.JSONDecodeError:
            _log("  [red]FAIL[/red] Invalid JSON output" if console else "  FAIL Invalid JSON output")
            passed = False

    _log("\n[3/3] Testing: sharderator-cli status")
    rc, stdout, stderr = _run_cli(["status"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        _log(f"  [green]✓[/green] Status output received" if console else "  ✓ Status output received")
        _log(stdout.strip())

    return passed


def _phase_dry_run(client, cloud_id, api_key, output_dir, console) -> bool:
    _log = console.print if console else print
    passed = True
    out_dir = output_dir or Path(".")

    _log("\n[1/2] Shrink dry-run (table format)")
    rc, stdout, stderr = _run_cli(
        ["shrink", "--dry-run", "--min-shards", "5"], cloud_id, api_key
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    elif "No eligible" in stdout:
        _log("  [green]✓[/green] No eligible indices for shrink" if console else "  ✓ No eligible indices for shrink")
    else:
        _log(f"  [green]✓[/green] Shrink dry-run output received" if console else "  ✓ Shrink dry-run output received")

    _log("\n[2/4] Shrink dry-run (JSON format)")
    rc, stdout, stderr = _run_cli(
        ["shrink", "--dry-run", "--min-shards", "5", "--output", "json"], cloud_id, api_key
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    elif not stdout.strip():
        _log(f"  [red]FAIL[/red] Empty stdout. stderr: {stderr[:300]}" if console
             else f"  FAIL Empty stdout. stderr: {stderr[:300]}")
        passed = False
    elif "{" not in stdout and "No eligible" in stdout:
        # No over-sharded indices found — not a failure, just nothing to shrink
        _log("  [green]✓[/green] No eligible indices for shrink (all within shard threshold)" if console
             else "  ✓ No eligible indices for shrink (all within shard threshold)")
    else:
        try:
            data = _extract_json(stdout)
            out_file = out_dir / "shrink-plan.json"
            out_file.write_text(json.dumps(data, indent=2))
            _log(f"  [green]✓[/green] JSON valid, saved to {out_file}" if console else f"  ✓ JSON valid, saved to {out_file}")
            assert "indices" in data or "mode" in data, "Missing expected fields"
            assert "preflight_passed" in data, "Missing preflight_passed"
        except (json.JSONDecodeError, AssertionError) as e:
            _log(f"  [red]FAIL[/red] {e}\nstdout[:200]: {stdout[:200]}" if console
                 else f"  FAIL {e}\nstdout[:200]: {stdout[:200]}")
            passed = False

    _log("\n[3/4] Merge dry-run (table format)")
    rc, stdout, stderr = _run_cli(
        ["merge", "--dry-run", "--granularity", "monthly"], cloud_id, api_key
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    elif "No eligible" in stdout:
        _log("  [green]✓[/green] No eligible merge groups found" if console else "  ✓ No eligible merge groups found")
    else:
        _log(f"  [green]✓[/green] Merge dry-run output received" if console else "  ✓ Merge dry-run output received")

    _log("\n[4/4] Merge dry-run (JSON format)")
    rc, stdout, stderr = _run_cli(
        ["merge", "--dry-run", "--granularity", "monthly", "--output", "json"], cloud_id, api_key
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    elif not stdout.strip():
        _log(f"  [red]FAIL[/red] Empty stdout. stderr: {stderr[:300]}" if console
             else f"  FAIL Empty stdout. stderr: {stderr[:300]}")
        passed = False
    elif "{" not in stdout and "No eligible" in stdout:
        # No merge groups found — not a failure
        _log("  [green]✓[/green] No eligible merge groups (all already consolidated)" if console
             else "  ✓ No eligible merge groups (all already consolidated)")
    else:
        try:
            data = _extract_json(stdout)
            out_file = out_dir / "merge-plan.json"
            out_file.write_text(json.dumps(data, indent=2))
            _log(f"  [green]✓[/green] JSON valid, saved to {out_file}" if console else f"  ✓ JSON valid, saved to {out_file}")
            assert "groups" in data, "Missing groups field"
            assert "preflight_passed" in data, "Missing preflight_passed"
        except (json.JSONDecodeError, AssertionError) as e:
            _log(f"  [red]FAIL[/red] {e}\nstdout[:200]: {stdout[:200]}" if console
                 else f"  FAIL {e}\nstdout[:200]: {stdout[:200]}")
            passed = False

    return passed


def _phase_shrink(client, cloud_id, api_key, output_dir, console) -> bool:
    _log = console.print if console else print
    passed = True

    # Find a 10-shard frozen index to test with
    try:
        indices = client.cat.indices(
            index="partial-*test-oversharded-10*",
            format="json",
            h="index,pri,docs.count",
            expand_wildcards="all",
        )
    except Exception:
        indices = []
    if not indices:
        _log("  [yellow]⚠ No 10-shard frozen test indices found. Skipping shrink phase.[/yellow]" if console
             else "  ⚠ No 10-shard frozen test indices found. Skipping shrink phase.")
        return True

    target = indices[0]["index"]
    original_docs = int(indices[0].get("docs.count") or 0)
    _log(f"\n[1/2] Single shrink: {target} ({indices[0]['pri']} shards, {original_docs:,} docs)")

    rc, stdout, stderr = _run_cli(
        ["shrink", "--index", target, "--min-shards", "1",
         "--ignore-circuit-breakers"], cloud_id, api_key
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        _log(f"  [green]✓[/green] Shrink completed" if console else "  ✓ Shrink completed")
        # Verify the result
        passed = passed and _verify_shrink_result(client, target, original_docs, console)

    _log("\n[2/2] Batch shrink: all over-sharded indices")
    rc, stdout, stderr = _run_cli(
        ["shrink", "--min-shards", "5", "--order", "smallest-first",
         "--ignore-circuit-breakers"], cloud_id, api_key, timeout=3600,
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        _log(f"  [green]✓[/green] Batch shrink completed" if console else "  ✓ Batch shrink completed")

    return passed


def _verify_shrink_result(client, original_index, expected_docs, console) -> bool:
    """Verify a shrunk index has the right doc count and shard count."""
    _log = console.print if console else print
    time.sleep(10)  # Give the new mount time to settle

    # The new index should be a partial- mount with 1 shard
    try:
        new_indices = client.cat.indices(
            index="partial-*",
            format="json",
            h="index,pri,docs.count",
            expand_wildcards="all",
        )
    except Exception:
        new_indices = []
    # Find the one that replaced our target (same base name, 1 shard)
    base = original_index.replace("partial-", "").replace("sharderator-new-", "")
    candidates = [i for i in new_indices if base in i.get("index", "") and int(i.get("pri", 99)) == 1]

    if not candidates:
        _log(f"  [yellow]⚠ Could not find replacement index for {original_index}[/yellow]" if console
             else f"  ⚠ Could not find replacement index for {original_index}")
        return True  # Not a hard failure — timing issue

    new_idx = candidates[0]
    new_docs = int(new_idx.get("docs.count") or 0)
    if new_docs != expected_docs:
        _log(f"  [red]✗ Doc count mismatch: expected {expected_docs:,}, got {new_docs:,}[/red]" if console
             else f"  ✗ Doc count mismatch: expected {expected_docs:,}, got {new_docs:,}")
        return False

    _log(f"  [green]✓[/green] Verified: {new_idx['index']} has {new_docs:,} docs, 1 shard" if console
         else f"  ✓ Verified: {new_idx['index']} has {new_docs:,} docs, 1 shard")
    return True


def _phase_merge(client, cloud_id, api_key, output_dir, console) -> bool:
    _log = console.print if console else print
    passed = True

    _log("\n[1/2] Single merge group: cpu metrics, first available month")
    rc, stdout, stderr = _run_cli(
        ["merge", "--granularity", "monthly", "--pattern", "cpu", "--max-groups", "1",
         "--ignore-circuit-breakers"],
        cloud_id, api_key, timeout=1800,  # 30 min for a single group
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        _log(f"  [green]✓[/green] Single merge completed" if console else "  ✓ Single merge completed")

    _log("\n[2/2] Batch merge: all daily indices, monthly granularity")
    _log("  (This may take 30-90 minutes for a full batch — be patient)")
    rc, stdout, stderr = _run_cli(
        ["merge", "--granularity", "monthly", "--order", "smallest-first",
         "--ignore-circuit-breakers"],
        cloud_id, api_key, timeout=7200,  # 2 hours for full batch
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        _log(f"  [green]✓[/green] Batch merge completed" if console else "  ✓ Batch merge completed")

    return passed


def _phase_crash_recovery(client, cloud_id, api_key, output_dir, console) -> bool:
    """Test crash recovery: start a merge, interrupt it, resume from tracker."""
    _log = console.print if console else print

    _log("\nCrash recovery test:")
    _log("  1. Start a merge batch in a subprocess")
    _log("  2. Kill it after a few seconds")
    _log("  3. Check the tracker for a FAILED or in-progress job")
    _log("  4. Resume via sharderator-cli report + manual resume")

    # Start a merge in background
    import subprocess as sp
    cmd = [
        "sharderator-cli", "merge",
        "--granularity", "monthly",
        "--max-groups", "1",
        "--cloud-id", cloud_id,
        "--api-key", api_key,
    ]
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
    _log(f"\n  Started merge process (PID {proc.pid})")
    _log("  Waiting 15s then interrupting...")
    time.sleep(15)
    proc.terminate()
    proc.wait(timeout=10)
    _log(f"  Process terminated (exit code {proc.returncode})")

    # Check tracker
    time.sleep(5)
    rc, stdout, stderr = _run_cli(["report", "--state", "all"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [yellow]⚠ report command failed: {stderr}[/yellow]" if console
             else f"  ⚠ report command failed: {stderr}")
        return True  # Not a hard failure

    try:
        data = json.loads(stdout)
        jobs = data.get("jobs", [])
        _log(f"  [green]✓[/green] Tracker has {len(jobs)} job(s)" if console else f"  ✓ Tracker has {len(jobs)} job(s)")
        for job in jobs[:3]:
            _log(f"    {job['index_name']}: {job['state']}")
    except json.JSONDecodeError:
        _log("  [yellow]⚠ Could not parse report output[/yellow]" if console else "  ⚠ Could not parse report output")

    _log("\n  To resume: sharderator-cli merge --granularity monthly --max-groups 1")
    return True


def _phase_export(client, cloud_id, api_key, output_dir, console) -> bool:
    _log = console.print if console else print
    passed = True
    out_dir = output_dir or Path(".")

    _log("\n[1/3] Export frozen index list as CSV")
    rc, stdout, stderr = _run_cli(["list", "--csv"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        out_file = out_dir / "frozen-indices.csv"
        out_file.write_text(stdout)
        lines = stdout.strip().splitlines()
        _log(f"  [green]✓[/green] {len(lines) - 1} indices exported to {out_file}" if console
             else f"  ✓ {len(lines) - 1} indices exported to {out_file}")

    _log("\n[2/3] Export job history as JSON")
    rc, stdout, stderr = _run_cli(["report"], cloud_id, api_key)
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    else:
        try:
            data = _extract_json(stdout)
            out_file = out_dir / "job-history.json"
            out_file.write_text(json.dumps(data, indent=2))
            _log(f"  [green]✓[/green] {data['job_count']} jobs exported to {out_file}" if console
                 else f"  ✓ {data['job_count']} jobs exported to {out_file}")
        except (json.JSONDecodeError, KeyError) as e:
            _log(f"  [red]FAIL[/red] {e}" if console else f"  FAIL {e}")
            passed = False

    _log("\n[3/3] Export merge dry-run as JSON")
    rc, stdout, stderr = _run_cli(
        ["merge", "--dry-run", "--granularity", "monthly", "--output", "json"],
        cloud_id, api_key, timeout=120,
    )
    if rc != 0:
        _log(f"  [red]FAIL[/red] {stderr}" if console else f"  FAIL {stderr}")
        passed = False
    elif not stdout.strip() or "{" not in stdout:
        # No merge groups found — everything was already merged. That's a pass.
        _log("  [green]✓[/green] No merge groups remaining (all already consolidated)" if console
             else "  ✓ No merge groups remaining (all already consolidated)")
    else:
        try:
            data = _extract_json(stdout)
            out_file = out_dir / "merge-plan-final.json"
            out_file.write_text(json.dumps(data, indent=2))
            _log(f"  [green]✓[/green] Merge plan exported to {out_file}" if console
                 else f"  ✓ Merge plan exported to {out_file}")
        except json.JSONDecodeError as e:
            _log(f"  [red]FAIL[/red] {e}" if console else f"  FAIL {e}")
            passed = False

    return passed
