"""Per-step stall watchdog for live jobs (D10).

The orphan sweeper only settles a job when every owner signal is gone. A
worker thread blocked inside a driver call (Snowflake connect on a dead
network path, a stuck COPY) keeps heart-beating its lease, so the job reads
``running`` forever while the operator watches a frozen progress bar — the
MySQL→Snowflake run that sat at 5 % for ten minutes after a deploy restart.

This module measures the active *step*: when the phase a job sits in goes
quiet past that step's threshold the job is stamped ``stalled`` +
``stall_seconds`` + ``stall_step``; past the step's hard limit the job is
settled ``failed`` with typed finding ``step_stalled`` so a restarted worker
does not leave the run in limbo. Failing is fail-closed but honest: the
message says the worker may still hold the destination write, and the
quarantine/undo paths run as for any failed run.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from services.brand_env import getenv_brand
from services.job_status import UNFINISHED_JOB_STATUSES

_logger = logging.getLogger(__name__)

STALL_ERROR_CODE = "step_stalled"

#: Built-in per-step thresholds (seconds). Any of them is overridable with
#: ``JOB_STEP_STALL_<STEP>_SEC``; ``JOB_STEP_STALL_SEC`` sets the default for
#: a step not named here. Connect/DDL are the steps the incident showed stuck.
_STEP_STALL_DEFAULTS: dict[str, float] = {
    "connect": 120.0,
    "connecting": 120.0,
    "create_table": 120.0,
    "ddl": 120.0,
    "preflight": 300.0,
    "extract": 900.0,
    "extracting": 900.0,
    "load": 900.0,
    "loading": 900.0,
    "write": 900.0,
    "reconcile": 600.0,
    "verify": 600.0,
    "upload": 600.0,
}
_DEFAULT_STEP_STALL = 900.0
_HARD_FAIL_MULTIPLIER = 3.0

_stall_sweeper_stop: threading.Event | None = None
_stall_sweeper_thread: threading.Thread | None = None


def _int_env(name: str, default: float, *, floor: float) -> float:
    try:
        return max(floor, float(getenv_brand(name, str(default)) or default))
    except (TypeError, ValueError):
        return default


def step_stall_threshold(step: str) -> float:
    """Flag threshold for one step name, env-overridable."""
    key = (step or "").strip().lower().replace(" ", "_")
    for needle, secs in _STEP_STALL_DEFAULTS.items():
        if needle in key:
            return _int_env(f"JOB_STEP_STALL_{needle.upper()}_SEC", secs, floor=1.0)
    return _int_env("JOB_STEP_STALL_SEC", _DEFAULT_STEP_STALL, floor=1.0)


def stall_fail_seconds(step: str) -> float:
    """Hard-fail age for a stalled step (flag is set well before this)."""
    return _int_env(
        "JOB_STEP_STALL_FAIL_SEC", step_stall_threshold(step) * _HARD_FAIL_MULTIPLIER,
        floor=step_stall_threshold(step),
    )


def _as_utc(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _active_step(job: dict[str, Any]) -> tuple[str, datetime | None, str]:
    """(step name, step start, step message) of the live phase."""
    phases = job.get("phases") or []
    if isinstance(phases, list):
        for ph in phases:
            if isinstance(ph, dict) and str(ph.get("status")) == "active":
                return (
                    str(ph.get("name") or job.get("phase") or "running"),
                    _as_utc(ph.get("started_at")),
                    str(ph.get("message") or ""),
                )
    return (
        str(job.get("phase") or "running"),
        _as_utc(job.get("started_at")),
        str(job.get("phase_message") or job.get("message") or ""),
    )


def job_step_stall(
    job: dict[str, Any], *, now: datetime | None = None
) -> dict[str, Any] | None:
    """Why this live job is stalled, or ``None``.

    A stall is *silence on the active step*: the phase start and the job's
    own updated_at are both older than the step threshold, so a step that is
    legitimately long but still writing progress does not flag.
    """
    if not isinstance(job, dict):
        return None
    if str(job.get("status") or "").strip().lower() not in UNFINISHED_JOB_STATUSES:
        return None
    clock = now or datetime.now(timezone.utc)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    step, started, message = _active_step(job)
    threshold = step_stall_threshold(step)
    marks = [t for t in (started, _as_utc(job.get("updated_at"))) if t]
    if not marks:
        return None
    quiet_seconds = (clock - max(marks)).total_seconds()
    if quiet_seconds < threshold:
        return None
    return {
        "step": step,
        "step_message": message,
        "stall_seconds": quiet_seconds,
        "threshold_seconds": threshold,
        "fail_seconds": stall_fail_seconds(step),
    }


def _settle_stalled(mongo: Any, job: dict[str, Any], info: dict[str, Any]) -> bool:
    """Fail a job whose step is past the hard limit — typed finding."""
    job_id = str(job.get("_id") or job.get("id") or "")
    step = info["step"]
    secs = int(info["stall_seconds"])
    message = (
        f"Step '{step}' produced no progress for {secs}s — past its "
        f"{int(info['fail_seconds'])}s limit. The worker may still hold the "
        "destination write; committed rows were not rolled back here — "
        "quarantine/undo applies as for any failed run."
    )
    try:
        return bool(
            mongo.update_job_status(
                job_id,
                "failed",
                only_from_status=[str(job.get("status") or "")],
                message=message,
                error=message,
                error_code=STALL_ERROR_CODE,
                failed_at_phase=step,
                stalled=True,
                stall_seconds=secs,
                stall_step=step,
            )
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning("stall settle %s failed: %s", job_id, exc)
        return False


def _flag_stalled(mongo: Any, job: dict[str, Any], info: dict[str, Any]) -> None:
    job_id = str(job.get("_id") or job.get("id") or "")
    try:
        # The flag write is bookkeeping, not job progress — stamping
        # updated_at here would reset the quiet clock the next sweep
        # measures, so a stalled job could never escalate to failed.
        mongo.update_job_fields(
            job_id,
            {
                "stalled": True,
                "stall_seconds": int(info["stall_seconds"]),
                "stall_step": info["step"],
            },
            touch_updated_at=False,
        )
    except Exception as exc:  # noqa: BLE001 — flagging must not kill the sweep
        _logger.debug("stall flag %s failed: %s", job_id, exc)


def sweep_stalled_jobs(
    *,
    mongo: Any | None = None,
    now: datetime | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Flag every quietly-stalled live job; hard-fail past the step limit."""
    if mongo is None:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
    try:
        jobs = mongo.list_unfinished_jobs(limit=limit)
    except Exception as exc:  # noqa: BLE001
        _logger.warning("stall sweep could not list jobs: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for job in jobs:
        job_id = str(job.get("_id") or job.get("id") or "")
        if not job_id:
            continue
        info = job_step_stall(job, now=now)
        if info is None:
            continue
        if info["stall_seconds"] >= info["fail_seconds"]:
            ok = _settle_stalled(mongo, job, info)
            out.append(
                {
                    "job_id": job_id,
                    "action": "failed" if ok else "flagged",
                    "step": info["step"],
                    "stall_seconds": int(info["stall_seconds"]),
                    "error_code": STALL_ERROR_CODE if ok else "",
                }
            )
            if ok:
                _logger.warning(
                    "Stalled job %s failed on step '%s' after %ss",
                    job_id,
                    info["step"],
                    int(info["stall_seconds"]),
                )
        else:
            _flag_stalled(mongo, job, info)
            out.append(
                {
                    "job_id": job_id,
                    "action": "flagged",
                    "step": info["step"],
                    "stall_seconds": int(info["stall_seconds"]),
                }
            )
    return out


def start_stall_watchdog(*, interval_seconds: float | None = None) -> bool:
    """Run :func:`sweep_stalled_jobs` on a daemon thread until stopped."""
    global _stall_sweeper_stop, _stall_sweeper_thread
    if _stall_sweeper_thread is not None and _stall_sweeper_thread.is_alive():
        return True
    try:
        interval = float(
            interval_seconds
            if interval_seconds is not None
            else (getenv_brand("JOB_STALL_SWEEP_SEC", "30") or "30")
        )
    except ValueError:
        interval = 30.0
    interval = max(5.0, interval)
    stop = threading.Event()
    _stall_sweeper_stop = stop

    def _run() -> None:
        while not stop.is_set():
            try:
                sweep_stalled_jobs()
            except Exception:  # noqa: BLE001
                _logger.exception("stall sweep failed")
            stop.wait(interval)

    _stall_sweeper_thread = threading.Thread(
        target=_run, name="df-stall-sweep", daemon=True
    )
    _stall_sweeper_thread.start()
    return True


def stop_stall_watchdog() -> None:
    global _stall_sweeper_stop, _stall_sweeper_thread
    if _stall_sweeper_stop is not None:
        _stall_sweeper_stop.set()
    _stall_sweeper_stop = None
    _stall_sweeper_thread = None
