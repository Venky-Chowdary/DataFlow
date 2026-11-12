"""Plan-scoped Validate as a pollable run, not a blocking HTTP request.

``run_plan_preflight`` is the SSOT. Walking a stored 1M-row upload (or probing
Snowflake) on the FastAPI event loop freezes ``/health``: nginx 504s, Studio
shows "Plan preflight failed" / Validate Not run, and Execute stays locked with
no verdict. The HTTP handler starts this job on a worker thread and returns
202; Studio polls GET until the same SSOT finishes.

CDC default remains at-least-once upsert. A running job is not Approve.
"""

from __future__ import annotations

import contextvars
import threading
import time
import uuid
from typing import Any

from services.workspace_filter import bound_allows, current_workspace_stamp

_LOCK = threading.Lock()
# Latest run per (owning workspace, plan id). The workspace is part of the key
# so a caller who names another workspace's plan id starts (and polls) a run of
# their own that fails "not found", and can neither read nor displace the
# owner's run (audit H-1).
_LATEST: dict[tuple[str, str], dict[str, Any]] = {}
_BY_RUN: dict[str, dict[str, Any]] = {}


def _owning_workspace(plan: Any) -> str:
    """The plan's workspace when the caller can see it, else the caller's own."""
    if plan is not None:
        return str(plan.workspace_id or "")
    return current_workspace_stamp(for_write=False)


def start_plan_preflight_job(
    plan_id: str,
    *,
    acknowledgments: Any = None,
    shape_recipe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from services.transfer_plan_service import run_plan_preflight
    from services.transfer_plan_store import get_plan

    plan = get_plan(plan_id)
    rows = 0
    if plan is not None:
        try:
            rows = int(plan.row_count_estimate or 0)
        except (TypeError, ValueError):
            rows = 0
    run_id = f"pf_{uuid.uuid4().hex[:12]}"
    key = (_owning_workspace(plan), plan_id)
    job: dict[str, Any] = {
        "plan_id": plan_id,
        "workspace_id": key[0],
        "run_id": run_id,
        "status": "running",
        "started_at": time.time(),
        "rows_estimate": rows,
        "rows_scanned": 0,
        "phase": "queued",
        "error": "",
        "result": None,
    }
    with _LOCK:
        _LATEST[key] = job
        _BY_RUN[run_id] = job
    # 202 is the accept snapshot. The worker may finish before the handler
    # returns — GET /preflight is the live view; do not race status here.
    accepted = job_public_view(job)

    def _run() -> None:
        try:
            result = run_plan_preflight(
                plan_id,
                acknowledgments=acknowledgments,
                shape_recipe=shape_recipe,
            )
            with _LOCK:
                if _LATEST.get(key) is not job:
                    return
                job["status"] = "complete"
                job["result"] = result
                job["finished_at"] = time.time()
        except Exception as exc:
            with _LOCK:
                if _LATEST.get(key) is not job:
                    return
                job["status"] = "failed"
                job["error"] = str(exc) or exc.__class__.__name__
                job["finished_at"] = time.time()

    threading.Thread(
        target=contextvars.copy_context().run,
        args=(_run,),
        name=f"plan-preflight-{plan_id[:8]}",
        daemon=True,
    ).start()
    return accepted


def get_plan_preflight_job(plan_id: str, run_id: str = "") -> dict[str, Any] | None:
    """The latest (or the named) Validate run of a plan in the caller's scope.

    A run id is not a bearer handle: it is served only under its own plan and
    only to a caller whose tenant scope covers the run's workspace.
    """
    from services.transfer_plan_store import get_plan

    key = (_owning_workspace(get_plan(plan_id)), plan_id)
    with _LOCK:
        job = _BY_RUN.get(run_id) if run_id else _LATEST.get(key)
        if job is None or job["plan_id"] != plan_id:
            return None
        if not bound_allows(job.get("workspace_id")):
            return None
        return job_public_view(job)


def job_public_view(job: dict[str, Any]) -> dict[str, Any]:
    started = float(job.get("started_at") or time.time())
    view: dict[str, Any] = {
        "plan_id": job["plan_id"],
        "run_id": job["run_id"],
        "status": job["status"],
        "rows_estimate": int(job.get("rows_estimate") or 0),
        "rows_scanned": int(job.get("rows_scanned") or 0),
        "phase": str(job.get("phase") or ""),
        "elapsed_ms": int(max(0.0, time.time() - started) * 1000),
        "error": str(job.get("error") or ""),
    }
    result = job.get("result")
    if job.get("status") == "complete" and isinstance(result, dict):
        merged = {**result, **view}
        # The SSOT run_id (citable Execute handle) wins over the job handle
        # when they differ — Execute unlocks against the persisted verdict.
        if result.get("run_id"):
            merged["run_id"] = result["run_id"]
            merged["job_run_id"] = job["run_id"]
        return merged
    return view


def report_plan_preflight_progress(
    plan_id: str,
    *,
    rows_scanned: int | None = None,
    rows_estimate: int | None = None,
    phase: str = "",
) -> None:
    """Heartbeat for Studio polling — never invents a verdict."""
    with _LOCK:
        # The worker thread carries the starter's tenant scope (copied
        # context), so the heartbeat lands on that caller's run of this plan.
        job = next(
            (
                j
                for (_ws, pid), j in _LATEST.items()
                if pid == plan_id
                and j.get("status") == "running"
                and bound_allows(j.get("workspace_id"))
            ),
            None,
        )
        if job is None:
            return
        if rows_scanned is not None:
            job["rows_scanned"] = int(rows_scanned)
        if rows_estimate is not None:
            job["rows_estimate"] = int(rows_estimate)
        if phase:
            job["phase"] = phase


def reset_plan_preflight_jobs() -> None:
    """Test isolation only."""
    with _LOCK:
        _LATEST.clear()
        _BY_RUN.clear()
