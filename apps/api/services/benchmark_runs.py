"""Proofs → Scale: the benchmark as a background run with progress (R7).

QA (production 557f556c): the 100k benchmark failed with "Request timed out"
after ~3 minutes, peak memory read "0 MB", and the comparison column said
competitors were "5.6x slower" while listing a higher rows/sec than ours.

* ``start_run`` returns at once; the benchmark runs in a child process
  (``benchmarks.local_bench_worker``) and the page polls ``get_run``.
* Peak memory is the child's own peak RSS — a real measurement.
* ``comparison`` is computed from the two numbers, from Datawrap's side, so
  the label cannot be inverted.

One run at a time per API process (the benchmark is CPU-bound and would only
measure itself contending with a second copy). Runs are kept in memory: the
API service runs one replica (deploy/railway/api.toml).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

API_ROOT = Path(__file__).resolve().parents[1]
RUN_TIMEOUT_SECONDS = 30 * 60
#: Within this ratio band the two numbers are reported as about the same.
SIMILAR_BAND = 0.10

_LOCK = threading.Lock()
_RUNS: dict[str, dict[str, Any]] = {}
_KEEP = 20


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def baseline_competitors() -> list[dict[str, Any]]:
    """Approximate mid-market ELT baselines for procurement context (synthetic).

    Figures are illustrative industry ranges — not named vendor SLAs or guarantees.
    """
    return [
        {
            "product": "Managed ELT (high)",
            "typical_rps": 4000,
            "memory_mb": 2048,
            "resume_from_checkpoint": True,
            "observed_max_rows": 1_000_000_000,
            "notes": "Throughput depends on source API rate limits and destination load",
        },
        {
            "product": "Open-source ELT workers",
            "typical_rps": 2500,
            "memory_mb": 1024,
            "resume_from_checkpoint": True,
            "observed_max_rows": 100_000_000,
            "notes": "Connector pods; scale limited by worker memory",
        },
        {
            "product": "Lightweight Singer-class",
            "typical_rps": 1800,
            "memory_mb": 1024,
            "resume_from_checkpoint": False,
            "observed_max_rows": 10_000_000,
            "notes": "Row-by-row logging overhead on smaller pipelines",
        },
    ]


def comparison(datawrap_rps: float, other_rps: float) -> dict[str, Any]:
    """How Datawrap's measured rows/sec compares with another figure.

    ``label`` always names Datawrap as the subject: "Datawrap 5.6× slower"
    when the other number is 5.6 times ours.
    """
    ours = max(float(datawrap_rps or 0), 0.0)
    theirs = max(float(other_rps or 0), 0.0)
    if ours <= 0 or theirs <= 0:
        return {"verdict": "unknown", "ratio": None, "label": "No measurement"}
    ratio = ours / theirs
    if abs(ratio - 1) <= SIMILAR_BAND:
        return {"verdict": "similar", "ratio": round(ratio, 3), "label": "About the same"}
    if ratio > 1:
        return {"verdict": "faster", "ratio": round(ratio, 3), "label": f"Datawrap {ratio:.1f}× faster"}
    return {"verdict": "slower", "ratio": round(ratio, 3), "label": f"Datawrap {1 / ratio:.1f}× slower"}


def with_comparisons(report: dict[str, Any], competitors: list[dict[str, Any]]) -> dict[str, Any]:
    ours = float(report.get("records_per_second") or 0) if report.get("success") else 0.0
    out = dict(report)
    out["competitors"] = [
        {**c, "comparison": comparison(ours, float(c.get("typical_rps") or 0))} for c in competitors
    ]
    return out


def _public(run: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in run.items() if not k.startswith("_")}


class BenchmarkBusy(RuntimeError):
    """Another workspace's benchmark holds the (single, CPU-bound) runner."""


def get_run(
    run_id: str, *, workspace_id: str | None = None, unrestricted: bool = False
) -> dict[str, Any] | None:
    """The run, or ``None`` — also when it belongs to another workspace.

    ``workspace_id=None`` is the internal (unscoped) read; routes always pass
    the request's workspace so a run id from another workspace reads as absent.
    """
    with _LOCK:
        run = _RUNS.get(run_id)
        if run is None:
            return None
        if workspace_id is not None and not unrestricted:
            if str(run.get("workspace_id") or "") != str(workspace_id or ""):
                return None
        return _public(run)


def active_run() -> dict[str, Any] | None:
    with _LOCK:
        for run in _RUNS.values():
            if run["status"] in {"queued", "running"}:
                return _public(run)
    return None


def start_run(rows: int, *, workspace_id: str = "", started_by: str = "") -> tuple[dict[str, Any], bool]:
    """Start a benchmark run, or return this workspace's run in flight. ``(run, started)``.

    One run at a time per deployment (it saturates a core). A run in flight for
    another workspace is never handed back — that would show one tenant another
    tenant's run — so the caller is told the runner is busy instead.
    """
    with _LOCK:
        for run in _RUNS.values():
            if run["status"] in {"queued", "running"}:
                if str(run.get("workspace_id") or "") == str(workspace_id or ""):
                    return _public(run), False
                raise BenchmarkBusy("A benchmark is already running; try again when it finishes")
        run_id = uuid.uuid4().hex
        run = {
            "id": run_id,
            "rows": int(rows),
            "status": "queued",
            "phase": "queued",
            "progress_pct": 0.0,
            "rows_done": 0,
            "elapsed_seconds": 0.0,
            "started_at": _now(),
            "finished_at": None,
            "error": "",
            "report": None,
            "workspace_id": str(workspace_id or ""),
            "started_by": started_by,
        }
        _RUNS[run_id] = run
        for old in list(_RUNS)[:-_KEEP]:
            if _RUNS[old]["status"] not in {"queued", "running"}:
                _RUNS.pop(old, None)
    threading.Thread(target=_execute, args=(run_id,), daemon=True, name=f"bench-{run_id[:8]}").start()
    return get_run(run_id) or {}, True


def run_blocking(rows: int) -> dict[str, Any]:
    """Run to completion (the legacy synchronous route / CLI)."""
    run, _ = start_run(rows)
    deadline = time.monotonic() + RUN_TIMEOUT_SECONDS + 30
    while time.monotonic() < deadline:
        current = get_run(run["id"]) or {}
        if current.get("status") in {"succeeded", "failed"}:
            return current
        time.sleep(0.25)
    return get_run(run["id"]) or {}


def _update(run_id: str, **fields: Any) -> None:
    with _LOCK:
        if run_id in _RUNS:
            _RUNS[run_id].update(fields)


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["DATAFLOW_JOB_STORE"] = "memory"
    env["DATAFLOW_DISABLE_OBJECT_STORE"] = "1"
    paths = [str(API_ROOT)]
    preflight = API_ROOT.parents[1] / "packages" / "preflight" / "src"
    if preflight.is_dir():
        paths.append(str(preflight))
    if env.get("PYTHONPATH"):
        paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _execute(run_id: str) -> None:
    run = get_run(run_id) or {}
    started = time.monotonic()
    _update(run_id, status="running", phase="starting")
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "benchmarks.local_bench_worker", str(run["rows"])],
            cwd=str(API_ROOT),
            env=_child_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        _update(run_id, status="failed", error=f"Could not start the benchmark: {exc}",
                finished_at=_now())
        return
    killer = threading.Timer(RUN_TIMEOUT_SECONDS, proc.kill)
    killer.start()
    stderr_tail: list[str] = []

    def drain_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_tail.append(line.rstrip())
            del stderr_tail[:-20]

    threading.Thread(target=drain_stderr, daemon=True).start()
    report: dict[str, Any] | None = None
    assert proc.stdout is not None
    for line in proc.stdout:
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("type") == "progress":
            _update(
                run_id,
                phase=str(msg.get("phase") or "running"),
                progress_pct=float(msg.get("progress_pct") or 0),
                rows_done=int(msg.get("rows_done") or 0),
                elapsed_seconds=round(time.monotonic() - started, 1),
            )
        elif msg.get("type") == "result":
            report = msg.get("report") or {}
    code = proc.wait()
    killer.cancel()
    elapsed = round(time.monotonic() - started, 1)
    if report is None:
        timed_out = elapsed >= RUN_TIMEOUT_SECONDS
        _update(
            run_id,
            status="failed",
            phase="failed",
            elapsed_seconds=elapsed,
            finished_at=_now(),
            error=(
                f"The benchmark did not finish within {RUN_TIMEOUT_SECONDS // 60} minutes."
                if timed_out
                else f"The benchmark process exited ({code}) without a result."
            ),
            stderr_tail=stderr_tail[-5:],
        )
        return
    final = with_comparisons(report, baseline_competitors())
    _update(
        run_id,
        status="succeeded" if report.get("success") else "failed",
        phase="done",
        progress_pct=100.0 if report.get("success") else run.get("progress_pct", 0),
        rows_done=int((report.get("destination_summary") or {}).get("row_count") or 0),
        elapsed_seconds=elapsed,
        finished_at=_now(),
        error=str(report.get("error") or ""),
        report=final,
    )
