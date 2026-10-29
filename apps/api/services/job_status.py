"""Canonical transfer-job status vocabulary.

A run that completed but altered or dropped data must NOT be reported as a clean
``completed``. ``completed_with_quarantine`` is a *successful terminal* status
(the transfer finished, data landed) that also signals rejected rows and/or
values coerced to NULL, so dashboards, polling, and reconciliation stay honest.
"""

from __future__ import annotations

COMPLETED = "completed"
COMPLETED_WITH_QUARANTINE = "completed_with_quarantine"
# The load wrote nothing because today's file has not arrived. The job
# finished; the operator still has to see the warning.
COMPLETED_WITH_WARNINGS = "completed_with_warnings"

# Statuses that mean "the transfer succeeded" (data is in the destination).
COMPLETED_STATUSES = frozenset({COMPLETED, COMPLETED_WITH_QUARANTINE, COMPLETED_WITH_WARNINGS})

# Statuses that mean "the job will not change again".
TERMINAL_STATUSES = frozenset(
    {COMPLETED, COMPLETED_WITH_QUARANTINE, COMPLETED_WITH_WARNINGS, "failed", "cancelled"}
)

# Statuses a live worker owns. ``paused`` is an operator state and is not
# resumed by the orphan sweep.
UNFINISHED_JOB_STATUSES = frozenset({"pending", "queued", "running", "retrying"})

# A cancel request must not be rewritten to success or live progress. ``failed``
# is still allowed (the run actually broke). ``cancelled`` is the requested terminal.
CANCEL_BLOCKS_STATUSES = frozenset(
    {"running", "pending", COMPLETED, COMPLETED_WITH_QUARANTINE, COMPLETED_WITH_WARNINGS}
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


def _count(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return max(0, int(value))  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return None


def measure_cancel_landing(
    exc: BaseException,
    *,
    dest_type: str,
    dest_cfg: dict,
    table: str,
    sync_mode: str,
    rows_before: object,
    table_existed_before: object,
    keyed: bool,
    atomic_overwrite: bool,
) -> None:
    """D199 — stamp what the destination holds once a cancelled run stopped.

    The committed-batch tally under-counts a pooled load: a chunk writer that
    was already inside its commit when the cancel landed adds rows the
    progress reporter never applied (QA: "23,552 left behind", MySQL held
    25,120). By the time the cancel reaches here every writer has stopped, so
    a COUNT is the real number. It is attributed to *this run* only when the
    arithmetic is exact: an append (rows before known) or an overwrite that
    cleared or created the table. A keyed upsert's COUNT growth is new keys
    only, and an atomic overwrite rolls back, so those keep the tally,
    labelled as a lower bound, beside the destination's own count.
    """
    try:
        from services.dest_precount import precount_table

        now = precount_table(dest_type, dest_cfg, table)
    except Exception:  # noqa: BLE001 - an unreadable destination keeps the tally
        now = None
    if now is None:
        return
    exc.destination_rows_now = int(now)  # type: ignore[attr-defined]
    from services.sync_cursor import is_overwrite_sync

    by_run: int | None = None
    if is_overwrite_sync(sync_mode):
        if not atomic_overwrite or table_existed_before is False:
            by_run = int(now)
    elif not keyed:
        before = _count(rows_before)
        if before is not None and now >= before:
            by_run = int(now) - before
    if by_run is not None:
        exc.destination_rows_by_run = by_run  # type: ignore[attr-defined]


def cancel_outcome_for(
    rows_committed: int | None,
    *,
    destination_rows_by_run: int | None = None,
    destination_rows_now: int | None = None,
    tally_is_exact: bool = False,
) -> dict:
    """What a cancel left behind: committed rows are never rolled back.

    Cancel is honoured at the next checkpoint, so a chunk (or the whole load)
    can already be committed when the job turns ``cancelled``. The job must
    say so instead of reading as a clean stop (QA MXD10).

    D199: the number comes from the destination when it was re-counted after
    the writers stopped; otherwise it is the committed-batch tally and says
    it is a lower bound.
    """
    tally = _count(rows_committed) or 0
    by_run = _count(destination_rows_by_run)
    now = _count(destination_rows_now)
    if by_run is not None:
        rows, source = by_run, "destination_count"
    elif tally_is_exact:
        # The run finished and Gate-8 counted it before the cancel landed.
        rows, source = tally, "completed_run"
    else:
        rows, source = tally, "committed_batches"
    if rows and source == "completed_run":
        message = (
            f"Cancelled after {rows:,} row(s) were already committed to the "
            "destination — the load had finished when the cancel landed; they "
            "were not rolled back. Re-run with full_refresh_overwrite or clear "
            "the destination table to undo them."
        )
    elif rows and source == "destination_count":
        message = (
            f"Cancelled after {rows:,} row(s) were already committed to the "
            "destination (counted on the destination after the writers "
            "stopped) — they were not rolled back. Re-run with "
            "full_refresh_overwrite or clear the destination table to undo them."
        )
    elif rows:
        held = (
            f"; the destination table now holds {now:,} row(s)" if now is not None else ""
        )
        message = (
            f"Cancelled after at least {rows:,} row(s) were already committed to "
            "the destination (a lower bound: counted from the committed batches "
            f"the job saw, not re-counted on the destination{held}) — they were "
            "not rolled back. Re-run with full_refresh_overwrite or clear the "
            "destination table to undo them."
        )
    else:
        message = "Transfer cancelled by user"
    out = {
        "partial_write": bool(rows),
        "rows_committed": rows,
        "rows_committed_source": source,
        "rolled_back": False,
        "message": message,
    }
    if source == "committed_batches" and rows:
        out["rows_committed_is_lower_bound"] = True
    if now is not None:
        out["destination_rows_now"] = now
    if by_run is not None and tally != by_run:
        out["committed_batches_tally"] = tally
    return out


def terminal_status_for(rejected_rows: int = 0, coerced_null_rows: int = 0) -> str:
    """Pick the success terminal status based on data-integrity accounting."""
    if int(rejected_rows or 0) > 0 or int(coerced_null_rows or 0) > 0:
        return COMPLETED_WITH_QUARANTINE
    return COMPLETED


#: What a terminal job record carries that a job leaving that state must not
#: keep (D166): the cancelled / failed phase, the failure text, the finish stamp.
TERMINAL_EXIT_RESETS = {
    "phase": "queued",
    "error": None,
    "error_details": None,
    "completed_at": None,
    "cancel_outcome": None,
}


def terminal_exit_resets(
    previous_status: str | None, next_status: str | None, allow_terminal_exit: bool
) -> dict:
    """Fields to clear when a write moves a job out of a terminal status."""
    if allow_terminal_exit and is_terminal(previous_status) and not is_terminal(next_status):
        return dict(TERMINAL_EXIT_RESETS)
    return {}


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
