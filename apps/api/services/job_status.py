"""Canonical transfer-job status vocabulary.

A run that completed but altered or dropped data must NOT be reported as a clean
``completed``. ``completed_with_quarantine`` is a *successful terminal* status
(the transfer finished, data landed) that also signals rejected rows and/or
values coerced to NULL, so dashboards, polling, and reconciliation stay honest.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

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
    """D199 / C16 — stamp what the destination holds once a cancelled run stopped.

    The committed-batch tally is the writer's acknowledgement, not the
    destination. It under-counts a pooled load (a chunk writer already inside
    its commit when the cancel landed: QA "23,552 left behind", MySQL held
    25,120) and over-counts a write that was rolled back: a COPY inside one
    transaction, or a run-created table dropped by the copy's undo, reported
    "1,024 rows retained" / "2,048 rows written and retained" for a table that
    no longer existed (QA C16, MySQL D11). By the time the cancel reaches here
    every writer has stopped, so the destination is measured:

    * the table does not exist → nothing of this run is there (0, exact);
    * the run created it → every row there is this run's;
    * an append with the rows before known → the growth;
    * an overwrite that clears first → every row now there; one that runs in a
      single transaction (or a staged swap) and still holds exactly what it
      held before was rolled back → 0;
    * a keyed upsert into a table that was empty before → every row there.

    A keyed upsert into a populated table grows by new keys only, so it keeps
    the tally, labelled as a lower bound, beside the destination's own count.
    """
    try:
        from services.dest_precount import precount_table

        now = precount_table(dest_type, dest_cfg, table)
    except Exception:  # noqa: BLE001 - an unreadable destination keeps the tally
        now = None
    if now is None:
        return
    exists_now: bool | None = None
    if int(now) == 0:
        # ``precount_table`` answers 0 for a missing table: tell the two apart,
        # the record says which one the operator is looking at.
        try:
            from services.dest_precount import destination_table_exists
            from services.dialect_profiles import schema_from_cfg

            exists_now = destination_table_exists(
                dest_type, dest_cfg, schema=schema_from_cfg(dest_type, dest_cfg),
                table_name=table,
            )
        except Exception:  # noqa: BLE001 - unknown existence stays unknown
            exists_now = None
    exc.destination_rows_now = int(now)  # type: ignore[attr-defined]
    if exists_now is False:
        exc.destination_table_exists = False  # type: ignore[attr-defined]
        exc.destination_rows_by_run = 0  # type: ignore[attr-defined]
        return
    from services.sync_cursor import is_overwrite_sync

    before = _count(rows_before)
    by_run: int | None = None
    if table_existed_before is False:
        # Created by this run: no older row can be there.
        by_run = int(now)
    elif is_overwrite_sync(sync_mode):
        if not atomic_overwrite:
            by_run = int(now)
        elif before is not None:
            # One transaction / staged swap: either it committed (every row is
            # the replacement) or it rolled back and the table is as before.
            by_run = 0 if int(now) == before else int(now)
    elif not keyed:
        if before is not None and now >= before:
            by_run = int(now) - before
    elif before == 0:
        # A keyed upsert into a table that was empty before its first pass has
        # no older key a write could have replaced: every row there is this
        # job's. On a resumed pass ``rows_before`` is the first pass's
        # pre-write count, so the number covers every pass (QA: "121,856
        # committed" while MySQL held 151,744).
        by_run = int(now)
    if by_run is not None:
        exc.destination_rows_by_run = by_run  # type: ignore[attr-defined]


def checkpoint_partial_chunk_on_cancel(
    exc: BaseException,
    *,
    checkpoint: Any,
    checkpoint_service: Any,
    committed_offset: int,
    rows_applied: int,
    rejected_applied: int,
    write_mode: str,
    sync_mode: str,
    dest_type: str,
    job_id: str,
    connector: str = "",
    next_chunk_idx: int | None = None,
    use_keyset: bool = False,
    keyset_col: str = "",
    incremental: bool = False,
) -> bool:
    """Make the committed prefix of a cancelled chunk a durable checkpoint.

    A transactional writer rolls its chunk back on cancel, so the last saved
    checkpoint already matches the destination. A schemaless destination
    (MongoDB) commits every batch on its own: the documents a cancelled chunk
    had written stay, while no checkpoint covered them. Retry was refused as
    a duplicate and Resume as "No durable checkpoint" — the operator had no
    way on (QA r12 Mongo D5).

    The writer reports the source rows its committed batches covered
    (``chunk_source_rows_consumed``) and the quarantine rows among them. The
    checkpoint is advanced only when D199's destination count proves that
    prefix is exactly what landed (``destination_rows_by_run`` equals the
    applied tally plus this chunk's writes): a later chunk that also committed
    out of order, a keyed upsert (no row offset), or an overwrite (the resumed
    run re-parks the collection) leave the checkpoint as it was.

    The writer counts rows of the page it was handed, so the offset it implies
    is a source-row offset only when the chunk is the next one to apply
    (``next_chunk_idx``; a pool may have finished a later chunk first) and the
    page was not shaped (a recipe that filters or expands rows breaks the
    one-to-one mapping). Under keyset pagination the resume bookmark must be
    the key after the consumed prefix, which the chunk stamps on the cancel;
    an incremental run keeps its per-page watermark semantics and is left to
    the existing resume. Returns True when a checkpoint was saved.
    """
    consumed = getattr(exc, "chunk_source_rows_consumed", None)
    rows_written = getattr(exc, "rows_written", None)
    by_run = getattr(exc, "destination_rows_by_run", None)
    if (
        checkpoint is None
        or checkpoint_service is None
        or not consumed
        or rows_written is None
        or by_run is None
    ):
        return False
    from services.sync_cursor import is_overwrite_sync
    from src.transfer.engine_family import is_schemaless_family

    if str(write_mode or "").lower() == "upsert" or is_overwrite_sync(sync_mode):
        return False
    if incremental or not is_schemaless_family(dest_type):
        return False
    if int(by_run) != int(rows_applied) + int(rows_written):
        return False
    if next_chunk_idx is None or getattr(exc, "chunk_idx", None) != int(next_chunk_idx):
        return False
    if getattr(exc, "chunk_shaped", True):
        return False
    bookmark = getattr(exc, "chunk_keyset_bookmark", None)
    if use_keyset and not bookmark:
        return False
    details = [
        d for d in (getattr(exc, "chunk_rejected_details", None) or []) if isinstance(d, dict)
    ]
    if details and job_id:
        from services.quarantine_dlq import persist_rejected_rows

        # The chunk never finished, so its quarantine rows were not persisted
        # with it; the resumed pass starts after them and would not see them.
        persist_rejected_rows(
            job_id=str(job_id),
            rejected_details=details,
            source="stream_batch_cancel",
            connector=str(connector or ""),
        )
    checkpoint.offset = int(committed_offset) + int(consumed)
    checkpoint.rows_processed = int(rows_applied) + int(rows_written)
    if use_keyset:
        checkpoint.cursor_value = bookmark
        checkpoint.cursor_column = str(keyset_col or "") or checkpoint.cursor_column
    checkpoint.rejected_rows = int(rejected_applied) + len(details)
    checkpoint.phase = "writing"
    checkpoint.status = "cancelled"
    record = getattr(checkpoint_service, "record_cancelled_progress", None)
    if not callable(record):
        return False
    try:
        saved = bool(record(checkpoint))
    except Exception as save_exc:  # noqa: BLE001 - the cancel itself still stands
        logger.warning("partial-chunk checkpoint not saved on cancel: %s", save_exc)
        return False
    if saved:
        exc.partial_chunk_checkpointed = True  # type: ignore[attr-defined]
    return saved


def cancel_outcome_for(
    rows_committed: int | None,
    *,
    destination_rows_by_run: int | None = None,
    destination_rows_now: int | None = None,
    destination_table_exists: bool | None = None,
    tally_is_exact: bool = False,
) -> dict:
    """What a cancel left behind, stated as the destination holds it.

    Cancel is honoured at the next checkpoint, so a chunk (or the whole load)
    can already be committed when the job turns ``cancelled``; committed rows
    are not rolled back and the job must say so (QA MXD10).

    D199 / C16: the number comes from the destination when it was re-counted
    after the writers stopped. ``rows_committed`` (the writer's tally) is only
    the fallback, labelled a lower bound — and never stated for rows a
    rollback removed: when the destination holds none of this run's rows the
    outcome says the write was rolled back (or the run-created table is gone),
    whatever the writer had acknowledged before the cancel.
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
    # Nothing of this run is there although it wrote (a tally), or the table it
    # wrote into is gone: the write was undone, not merely never started.
    rolled_back = (
        source == "destination_count"
        and rows == 0
        and (tally > 0 or destination_table_exists is False)
    )
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
    elif source == "destination_count" and destination_table_exists is False:
        message = (
            "Transfer cancelled by user. No row of this run was kept: the "
            "destination table does not exist (counted on the destination "
            "after the writers stopped"
            + ("; the rows the writer had sent were rolled back with the table"
               if tally else "")
            + "). A retry starts from zero."
        )
    elif source == "destination_count":
        message = (
            "Transfer cancelled by user. No row of this run was kept"
            + (": the write was rolled back" if tally else "")
            + f" — the destination table holds {now or 0:,} row(s), as it did "
            "before the run (counted on the destination after the writers "
            "stopped). A retry starts from zero."
        )
    else:
        message = "Transfer cancelled by user"
    out = {
        "partial_write": bool(rows),
        "rows_committed": rows,
        "rows_committed_source": source,
        "rolled_back": rolled_back,
        "message": message,
    }
    if source == "committed_batches" and rows:
        out["rows_committed_is_lower_bound"] = True
    if now is not None:
        out["destination_rows_now"] = now
    if destination_table_exists is not None:
        out["destination_table_exists"] = bool(destination_table_exists)
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
