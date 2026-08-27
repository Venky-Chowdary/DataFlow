"""Plan-scoped Validate as a pollable run, not a blocking HTTP request.

``run_plan_preflight`` is the SSOT. Walking a stored 1M-row upload (or probing
Snowflake) on the FastAPI event loop freezes ``/health``: nginx 504s, Studio
shows "Plan preflight failed" / Validate Not run, and Execute stays locked with
no verdict. The HTTP handler starts this job on a worker thread and returns
202; Studio polls GET until the same SSOT finishes.

CDC default remains at-least-once upsert. A running job is not Approve.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

_LOCK = threading.Lock()
_LATEST: dict[str, dict[str, Any]] = {}
_BY_RUN: dict[str, dict[str, Any]] = {}


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
    job: dict[str, Any] = {
        "plan_id": plan_id,
        "run_id": run_id,
        "status": "running",
        "started_at": time.time(),
        "rows_estimate": rows,
        "error": "",
        "result": None,
    }
    with _LOCK:
        _LATEST[plan_id] = job
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
                if _LATEST.get(plan_id) is not job:
                    return
                job["status"] = "complete"
                job["result"] = result
                job["finished_at"] = time.time()
        except Exception as exc:
            with _LOCK:
                if _LATEST.get(plan_id) is not job:
                    return
                job["status"] = "failed"
                job["error"] = str(exc) or exc.__class__.__name__
                job["finished_at"] = time.time()

    threading.Thread(
        target=_run,
        name=f"plan-preflight-{plan_id[:8]}",
        daemon=True,
    ).start()
    return accepted


def get_plan_preflight_job(plan_id: str, run_id: str = "") -> dict[str, Any] | None:
    with _LOCK:
        job = _BY_RUN.get(run_id) if run_id else _LATEST.get(plan_id)
        if job is None:
            return None
        return job_public_view(job)


def job_public_view(job: dict[str, Any]) -> dict[str, Any]:
    started = float(job.get("started_at") or time.time())
    view: dict[str, Any] = {
        "plan_id": job["plan_id"],
        "run_id": job["run_id"],
        "status": job["status"],
        "rows_estimate": int(job.get("rows_estimate") or 0),
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


def reset_plan_preflight_jobs() -> None:
    """Test isolation only."""
    with _LOCK:
        _LATEST.clear()
        _BY_RUN.clear()
