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


def _comparable(value: Any) -> Any:
    from datetime import datetime, timezone
    from decimal import Decimal, InvalidOperation

    text = str(value or "").strip()
    try:
        return (0, Decimal(text))
    except (InvalidOperation, ValueError):
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return (1, parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc))
    except ValueError:
        return None


def newer(parked: Any, current: Any) -> bool | None:
    """True when ``parked`` is past ``current``; ``None`` when the two cannot be ordered."""
    if str(current or "").strip() == "":
        return True
    a, b = _comparable(parked), _comparable(current)
    if a is None or b is None or a[0] != b[0]:
        return None
    return a[1] > b[1]


def release_cursor(job_id: str, job: dict[str, Any], state: str) -> None:
    """Apply (on a pass) or drop (otherwise) the watermark this job parked.

    The watermark only ever moves forward: it is applied when it is past the
    schedule's current one, whichever run is the schedule's latest now (with
    ``wait_for_verification`` off a later run may have started, and the run
    being verified may finish before its own run is recorded on the
    schedule). Values that cannot be ordered fall back to "this run is the
    schedule's latest".
    """
    pending = job.get(PENDING_FIELD)
    if not isinstance(pending, dict) or state == "running":
        return
    _service().update_job_fields(job_id, {PENDING_FIELD: None}, touch_updated_at=False)
    if state != "passed":
        return
    from services.schedule_store import get_schedule, update_schedule

    sched = get_schedule(str(pending.get("schedule_id") or ""))
    if sched is None:
        return
    parked = str(pending.get("cursor_value") or "")
    forward = newer(parked, getattr(sched, "cursor_value", ""))
    if forward is None:
        forward = str(getattr(sched, "last_job_id", "") or "") == str(job_id)
    if forward:
        update_schedule(sched.id, {"cursor_value": parked})


def schedule_waits_for_verification(sched: Any) -> str:
    """Why this tick waits for the previous tick's verification, or ``""``."""
    from services.parity_scale.verification import is_running

    if not parity_scale_enabled() or getattr(sched, "wait_for_verification", True) is False:
        return ""
    last = str(getattr(sched, "last_job_id", "") or "")
    if not last:
        return ""
    job = _service().get_job(last) or {}
    if not is_running(job):
        return ""
    from services.parity_scale.verification import effective_block, is_queued

    if is_queued(job):
        # A verification that is only queued holds the schedule up to its own limit, not forever.
        limit_min = int(getattr(sched, "verification_queue_wait_minutes", 30) or 0)
        since = _parse(effective_block(job).get("queued_since"))
        if since is None or (_utcnow() - since).total_seconds() >= limit_min * 60:
            return ""
        return "Waiting for the previous run's verification (queued) before the next run starts."
    return "Waiting for the previous run's verification to finish before the next run starts."


def _parse(value: Any) -> Any:
    from datetime import datetime, timezone

    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _utcnow() -> Any:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)
