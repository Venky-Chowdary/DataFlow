"""Canonical transfer-job status vocabulary.

A run that completed but altered or dropped data must NOT be reported as a clean
``completed``. ``completed_with_quarantine`` is a *successful terminal* status
(the transfer finished, data landed) that also signals rejected rows and/or
values coerced to NULL, so dashboards, polling, and reconciliation stay honest.
"""

from __future__ import annotations

COMPLETED = "completed"
COMPLETED_WITH_QUARANTINE = "completed_with_quarantine"

# Statuses that mean "the transfer succeeded" (data is in the destination).
COMPLETED_STATUSES = frozenset({COMPLETED, COMPLETED_WITH_QUARANTINE})

# Statuses that mean "the job will not change again".
TERMINAL_STATUSES = frozenset({COMPLETED, COMPLETED_WITH_QUARANTINE, "failed", "cancelled"})

# Statuses a live worker owns. ``paused`` is an operator state and is not
# resumed by the orphan sweep.
UNFINISHED_JOB_STATUSES = frozenset({"pending", "queued", "running", "retrying"})

# A cancel request must not be rewritten to success or live progress. ``failed``
# is still allowed (the run actually broke). ``cancelled`` is the requested terminal.
CANCEL_BLOCKS_STATUSES = frozenset(
    {"running", "pending", COMPLETED, COMPLETED_WITH_QUARANTINE}
)


def refuse_job_status_write(
    *,
    previous_status: str | None,
    next_status: str,
    cancel_requested: bool = False,
    allow_terminal_exit: bool = False,
) -> str | None:
    """Return why a status write must be dropped, or None to allow it.

    Terminal statuses are sticky: ``cancelled`` must not become ``completed``
    when a fast COPY finishes after the operator clicked Cancel. Resume is
    the documented exit (``allow_terminal_exit=True``).

    Does not claim the wire is interruptible — only that the control-plane
    status cannot be rewritten to success after cancel.
    """
    if allow_terminal_exit:
        return None
    prev = (previous_status or "").strip()
    nxt = (next_status or "").strip()
    if prev in TERMINAL_STATUSES and nxt != prev:
        return f"already terminal ({prev})"
    if cancel_requested and nxt in CANCEL_BLOCKS_STATUSES:
        return "cancel_requested blocks success/progress rewrite"
    return None


def job_timestamp_iso(value: object) -> str:
    """ISO-8601 UTC with ``Z`` for a job timestamp; ``""`` when unset.

    Job timestamps are written in UTC, but Mongo hands them back naive, so
    ``str()`` printed ``2026-10-10 11:27:40.535000`` with no offset (QA MX3-19).
    """
    from datetime import datetime, timezone

    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return str(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def cancel_outcome_for(rows_committed: int | None) -> dict:
    """What a cancel left behind: committed rows are never rolled back.

    Cancel is honoured at the next checkpoint, so a chunk (or the whole load)
    can already be committed when the job turns ``cancelled``. The job must
    say so instead of reading as a clean stop (QA MXD10).
    """
    try:
        rows = max(0, int(rows_committed or 0))
    except (TypeError, ValueError):
        rows = 0
    if rows:
        message = (
            f"Cancelled after {rows:,} row(s) were already committed to the "
            "destination — they were not rolled back. Re-run with "
            "full_refresh_overwrite or clear the destination table to undo them."
        )
    else:
        message = "Transfer cancelled by user"
    return {
        "partial_write": bool(rows),
        "rows_committed": rows,
        "rolled_back": False,
        "message": message,
    }


def terminal_status_for(rejected_rows: int = 0, coerced_null_rows: int = 0) -> str:
    """Pick the success terminal status based on data-integrity accounting."""
    if int(rejected_rows or 0) > 0 or int(coerced_null_rows or 0) > 0:
        return COMPLETED_WITH_QUARANTINE
    return COMPLETED


def is_completed(status: str | None) -> bool:
    return (status or "") in COMPLETED_STATUSES


def is_terminal(status: str | None) -> bool:
    return (status or "") in TERMINAL_STATUSES


def job_stall_seconds(job: dict | None, *, threshold_seconds: float = 900.0) -> float:
    """Seconds a live job has gone without a control-plane write.

    A run blocked on a lock or a stalled wire sits at ``running`` with
    ``records_processed`` frozen — the pg→MongoDB transfer that sat at 0
    records for ~49 minutes reported no signal anywhere (QA hang report).
    Surfacing *silence itself* is the honest operator signal: the job did
    not fail, it stopped making progress. Returns ``0`` for a job that is
    terminal, fresh, or has no usable timestamp.
    """
    if not isinstance(job, dict):
        return 0.0
    status = str(job.get("status") or "").strip().lower()
    if status not in UNFINISHED_JOB_STATUSES:
        return 0.0
    from datetime import datetime, timezone

    last = job.get("updated_at") or job.get("started_at") or job.get("created_at")
    if isinstance(last, (int, float)):
        age = datetime.now(timezone.utc).timestamp() - float(last)
    else:
        try:
            ts = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
        except (TypeError, ValueError):
            return 0.0
    return max(0.0, age) if age >= threshold_seconds else 0.0
