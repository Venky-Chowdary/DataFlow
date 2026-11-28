"""Why an unattended scheduled run may start now — one rule (D433).

A scheduler is part of the trust layer: data moves only on an explicit cadence
or an explicit human/agent action. An unattended run therefore starts for one
of a few named reasons, and only when that reason holds on what is stored at
the moment the run claims the schedule:

* ``cron_window`` — the stored next run (a cadence boundary) has come;
* ``missed_window_catch_up`` — the stored slot is older than one window (the
  scheduler was down or the schedule was parked), or the operator queued a
  catch-up; one run covers it and the skipped windows are counted;
* ``retry`` — a failed run's declared retry is due, or the back-off a
  transient failure put past the next cron slot has come;
* a human decision that re-armed the schedule at once (``approval``,
  ``plan_change``, ``rebaseline``), recorded on the schedule by that decision.

The beat reads a snapshot and dispatches outside its fleet lock, so the
decision is taken again on the freshly loaded document when the run claims
the schedule. A Run now that finished in between has moved the slot to the
next window (and cleared any retry): the beat's stale decision then starts
nothing. A schedule with no stored slot is not "due now"; the beat arms it to
its next window instead of firing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

FIRE_CRON_WINDOW = "cron_window"
FIRE_CATCH_UP = "missed_window_catch_up"
FIRE_RETRY = "retry"
FIRE_MANUAL = "manual"

#: Stored with a transient failure's back-off (later than the cron slot): the run it starts is a retry.
TRANSIENT_BACKOFF = "transient_backoff"

#: Reasons a human decision stores on the schedule when it re-arms it at once.
HUMAN_REARM_REASONS = frozenset({"approval", "plan_change", "rebaseline"})

#: What the history shows for each reason.
FIRE_REASON_LABELS: dict[str, str] = {
    FIRE_CRON_WINDOW: "Cron window",
    FIRE_CATCH_UP: "Missed-window catch-up",
    FIRE_RETRY: "Retry of a failed run",
    FIRE_MANUAL: "Run now",
    "approval": "Re-run after an approved finding",
    "plan_change": "Re-run after a plan change",
    "rebaseline": "Re-run after a destination rebaseline",
}


def _ts(value: Any) -> datetime | None:
    from services.schedule_store import _parse_ts

    return _parse_ts(value)


def _now(now: datetime | None) -> datetime:
    from services import schedule_clock

    current = now or schedule_clock.utc_now()
    return current if current.tzinfo else current.replace(tzinfo=timezone.utc)


def fire_reason(sched: Any, now: datetime | None = None) -> str:
    """The reason an unattended run of ``sched`` may start now, or ``""``.

    Pure: reads the schedule as given. Paused, parked (an open finding) and
    unmapped schedules are judged by the caller, which records why it skipped.
    """
    if sched is None or not bool(getattr(sched, "enabled", True)):
        return ""
    current = _now(now)
    retry_at = _ts(getattr(sched, "retry_at", None))
    if retry_at is not None:
        # A parked retry owns the schedule until it runs: the cadence must not
        # start a fresh attempt on top of the one still owed.
        return FIRE_RETRY if retry_at <= current else ""
    slot = _ts(getattr(sched, "next_run_at", None))
    if slot is None or slot > current:
        return ""
    armed_by = str(getattr(sched, "next_run_reason", "") or "").strip()
    if armed_by in HUMAN_REARM_REASONS:
        return armed_by
    if armed_by == TRANSIENT_BACKOFF:
        # The back-off a transient failure put past the next cron slot.
        return FIRE_RETRY
    if int(getattr(sched, "catch_up_windows_left", 0) or 0) > 0:
        return FIRE_CATCH_UP
    from services.schedule_store import count_missed_windows

    missed = count_missed_windows(
        cron=str(getattr(sched, "cron", "") or ""),
        interval=str(getattr(sched, "interval", "") or ""),
        tz=str(getattr(sched, "timezone", "") or "UTC"),
        next_run_at=getattr(sched, "next_run_at", None),
        now=current,
    )
    return FIRE_CATCH_UP if missed else FIRE_CRON_WINDOW


def needs_slot(sched: Any) -> bool:
    """An enabled schedule with no stored slot and no retry: arm, never fire."""
    return (
        bool(getattr(sched, "enabled", True))
        and _ts(getattr(sched, "next_run_at", None)) is None
        and _ts(getattr(sched, "retry_at", None)) is None
    )


def not_due_detail(sched: Any) -> str:
    """Why a beat that judged this schedule due started nothing."""
    slot = str(getattr(sched, "next_run_at", "") or "")
    when = f" The next run is the cron window at {slot}." if slot else ""
    return (
        "Not started: another run of this schedule finished after the beat "
        "judged it due, so no cron window or retry is owed now." + when
    )


def watermark_used(job_doc: dict[str, Any] | None) -> dict[str, Any]:
    """Which watermark a finished run read from, for its run record (D433 AC-3).

    ``scope`` is ``delta`` (rows past ``from``), ``full_source`` (no watermark
    was stored, or a full pass repaired a failed run), or empty when the job
    reports neither. ``to`` is the watermark the run committed, if any.
    """
    doc = job_doc or {}
    report = doc.get("incremental_watermark") if isinstance(doc.get("incremental_watermark"), dict) else {}
    scope = doc.get("read_scope") if isinstance(doc.get("read_scope"), dict) else {}
    if scope.get("scope") == "full_source":
        return {
            "scope": "full_source",
            "from": None,
            "to": None,
            "reason": str(scope.get("reason") or ""),
        }
    if not report:
        return {}
    before = report.get("before")
    return {
        "scope": "delta" if before not in (None, "") else "full_source",
        "from": before if before not in (None, "") else None,
        "to": report.get("after") if report.get("advanced") else before,
        "cursor_column": str(report.get("cursor_column") or ""),
        "rows_selected": report.get("rows_selected"),
        "reason": "" if before not in (None, "") else (
            "No watermark was stored for this route yet, so the whole source was read."
        ),
    }
