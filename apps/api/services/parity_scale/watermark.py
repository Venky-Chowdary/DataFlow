"""Schedules and verification after the write (owner decision 2).

* The next scheduled run waits while the previous run's verification runs
  (``wait_for_verification``, default true).
* An incremental watermark advances only once verification has not failed: a
  run still being verified parks its watermark on the job and the
  verification applies it when it passes; a failed, incomplete or stopped
  verification never moves it.
"""

from __future__ import annotations

from typing import Any

from services.parity_scale import parity_scale_enabled

PENDING_FIELD = "parity_scale_pending_cursor"
NOT_PASSED = frozenset({"failed", "incomplete", "error"})


def _service():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _verification_pending(job: dict[str, Any] | None) -> bool:
    """Running, or about to start (the beat can finalize before the terminal hook ran)."""
    from services.parity_scale.verification import block_of, is_running, needs_scale_verification

    if is_running(job):
        return True
    return parity_scale_enabled() and not block_of(job) and needs_scale_verification(job or {})


def hold_cursor_until_verified(schedule_id: str, job_id: str, job_doc: dict[str, Any] | None,
                               cursor_value: Any) -> Any:
    """The watermark the schedule may advance to now (``None`` = keep the old one)."""
    from services.parity_scale.verification import live_state

    if cursor_value in (None, ""):
        return cursor_value
    if _verification_pending(job_doc):
        _service().update_job_fields(job_id, {PENDING_FIELD: {
            "schedule_id": schedule_id, "cursor_value": str(cursor_value)}}, touch_updated_at=False)
        return None
    if live_state(job_doc) in NOT_PASSED:
        return None
    return cursor_value


def release_cursor(job_id: str, job: dict[str, Any], state: str) -> None:
    """Apply (on a pass) or drop (otherwise) the watermark this job parked."""
    pending = job.get(PENDING_FIELD)
    if not isinstance(pending, dict) or state == "running":
        return
    _service().update_job_fields(job_id, {PENDING_FIELD: None}, touch_updated_at=False)
    if state != "passed":
        return
    from services.schedule_store import get_schedule, update_schedule

    sched = get_schedule(str(pending.get("schedule_id") or ""))
    # Only the run that is still the schedule's latest moves its watermark.
    if sched is None or str(sched.last_job_id or "") != str(job_id):
        return
    update_schedule(sched.id, {"cursor_value": str(pending.get("cursor_value") or "")})


def schedule_waits_for_verification(sched: Any) -> str:
    """Why this tick waits for the previous tick's verification, or ``""``."""
    from services.parity_scale.verification import is_running

    if not parity_scale_enabled() or getattr(sched, "wait_for_verification", True) is False:
        return ""
    last = str(getattr(sched, "last_job_id", "") or "")
    if not last:
        return ""
    if not is_running(_service().get_job(last) or {}):
        return ""
    return "Waiting for the previous run's verification to finish before the next run starts."
