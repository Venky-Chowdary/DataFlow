"""Reconcile-phase job heartbeats (extracted from engine for F8 size budgets)."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

from services.batch_progress import reconcile_display_pct, reconcile_scan_pct
from services.reconcile_progress import (
    ReconcileScan,
    bind_reconcile_scan,
    reset_reconcile_scan,
)


def reconcile_heartbeat_scope(dest_summary: Any) -> dict[str, Any]:
    """Kwargs for :func:`reconcile_phase_heartbeat` from the summary already built.

    ``processed`` on a multi-table run is the job row total. Gate-8 checksums
    the restored endpoint. Passing that scope stops the heartbeat from calling
    the job total a checksum proof.
    """
    if not isinstance(dest_summary, dict):
        return {"proof_kind": "full"}
    scope: dict[str, Any] = {
        "proof_kind": str(dest_summary.get("checksum_mode") or "full"),
    }
    if dest_summary.get("multi_stream") is not True:
        return scope
    streams = dest_summary.get("streams")
    if not isinstance(streams, list):
        return scope
    names = [
        str(row.get("name") or "").strip()
        for row in streams
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    ]
    if len(names) < 2:
        return scope
    table = str(dest_summary.get("table") or names[-1]).strip() or names[-1]
    scope["checksum_scope"] = "last_stream"
    scope["checksum_table"] = table
    scope["stream_count"] = len(names)
    return scope


def _first_row_wait_phrase(elapsed: int, bound: int) -> str:
    """What the checksum is waiting on before its first destination row.

    The pulse used to promise "a metadata lock fails within 120s" for as long
    as the scan sat at 0%: at 467s on 30 rows, with no query on either
    database, the promise was false and pointed at a lock that did not exist
    (D280). The bound is the configured lock wait; past it the sentence says
    the destination has not answered instead of repeating the promise.
    """
    if elapsed <= bound:
        return (
            " No destination row has been read yet; a lock wait on the "
            f"destination ends within {bound}s."
        )
    return (
        f" No destination row has been read after {elapsed}s, longer than the "
        f"{bound}s lock-wait bound: the destination has not answered the "
        "checksum read. Check the destination's sessions and the connection "
        "to it; cancelling the job stops the scan."
    )


@contextmanager
def reconcile_phase_heartbeat(
    mongo: Any,
    job_id: str,
    *,
    processed: int,
    total: int,
    interval_s: float = 1.0,
    proof_kind: str = "full",
    checksum_scope: str = "",
    checksum_table: str = "",
    stream_count: int = 0,
) -> Iterator[None]:
    """Move progress through 98.0–98.99 while the checksum runs.

    The phase-only percentage stays 99 (``compute_transfer_progress_pct``).
    This heartbeat paints tenths from hashed rows and stores ``reconcile_pct``
    (0–99) so the theater and the jobs list move during a long scan. 100 is
    Gate-8's to publish when the run finishes.

    Reconciliation can take minutes on large tables (COUNT + checksum queries).
    Without heartbeats the theater freezes on the last write event and looks stuck.

    ``proof_kind=write_pass`` is file/stream write-pass compare — dest COUNT +
    dest fingerprint vs the write-pass hash. It cannot earn migration_proven.

    ``checksum_scope=last_stream`` means ``processed`` is the job population and
    the digest is one table. The sentence names that table.
    """
    write_pass = str(proof_kind or "").strip().lower() in {
        "write_pass",
        "inline_write_pass",
        "write_pass_fingerprints",
    }
    last_stream = str(checksum_scope or "").strip().lower() == "last_stream"
    table = str(checksum_table or "the last stream").strip() or "the last stream"
    table_phrase = (
        f"{int(stream_count)} tables" if int(stream_count or 0) >= 2 else "the selected tables"
    )
    scan = ReconcileScan()
    token = bind_reconcile_scan(scan)
    if write_pass:
        enter_msg = (
            "All rows written — dest COUNT + dest fingerprint vs write-pass "
            f"({processed:,} rows; not migration_proven)…"
        )
    elif last_stream:
        enter_msg = (
            "All rows written — reconciling destination "
            f"({processed:,} rows written across {table_phrase}). "
            f"Checksum proof is the last stream ({table}), not this total…"
        )
    else:
        enter_msg = (
            "All rows written — reconciling destination "
            f"({processed:,} rows: counts + checksum proof)…"
        )
    try:
        mongo.update_job_status(
            job_id,
            "running",
            phase="reconcile",
            progress_pct=reconcile_display_pct(0, total),
            reconcile_pct=0,
            records_processed=processed,
            total_rows=total,
            message=enter_msg,
        )
        from connectors.write_resilience import destination_lock_wait_seconds

        # Resolved here, not in the pulse thread: the bound the sentence names.
        lock_bound = destination_lock_wait_seconds()
        stop = threading.Event()
        started = time.monotonic()
        last_hashed = [0]

        def _pulse() -> None:
            while not stop.wait(interval_s):
                elapsed = int(time.monotonic() - started)
                hashed = scan.rows_hashed
                scan_pct = reconcile_scan_pct(hashed, total)
                hashed_phrase = (
                    f" Hashed {hashed:,} row fingerprints so far."
                    if hashed > 0
                    else ""
                )
                checksum_phrase = f" Checksum {scan_pct}%."
                if write_pass:
                    pulse = (
                        f"Reconciling ({elapsed}s) — dest COUNT and fingerprint for "
                        f"{processed:,} rows (write-pass compare, not migration_proven)…"
                        f"{hashed_phrase}{checksum_phrase}"
                    )
                elif last_stream:
                    pulse = (
                        f"Reconciling data ({elapsed}s) — {processed:,} rows written "
                        f"across the job. Checksum is the last stream ({table}), "
                        f"not the whole job…{hashed_phrase}{checksum_phrase}"
                    )
                else:
                    waiting = _first_row_wait_phrase(elapsed, lock_bound) if hashed <= 0 else ""
                    pulse = (
                        f"Reconciling data ({elapsed}s) — checksum scan still "
                        f"running for {processed:,} rows.{hashed_phrase}{waiting} "
                        "The job has not stalled."
                        f"{checksum_phrase}"
                    )
                if hashed > last_hashed[0]:
                    # Rows hashed since the last pulse: real progress.
                    last_hashed[0] = hashed
                    mongo.update_job_status(
                        job_id,
                        "running",
                        phase="reconcile",
                        progress_pct=reconcile_display_pct(hashed, total),
                        reconcile_pct=scan_pct,
                        records_processed=processed,
                        total_rows=total,
                        message=pulse,
                    )
                else:
                    # Nothing hashed since the last pulse: the worker is alive,
                    # but updated_at (the stall signal) must not move (D280).
                    _stamp_heartbeat(
                        mongo,
                        job_id,
                        {
                            "phase": "reconcile",
                            "progress_pct": reconcile_display_pct(hashed, total),
                            "reconcile_pct": scan_pct,
                            "records_processed": processed,
                            "total_rows": total,
                            "message": pulse,
                        },
                    )

        # Run under a copy of the caller's context so the pulse's logs keep the
        # job_id/correlation_id bound by the engine; raw threads start empty.
        from services.tracing import thread_in_context

        thread = thread_in_context(_pulse, name=f"reconcile-heartbeat-{job_id[:8]}")
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=1.0)
    finally:
        reset_reconcile_scan(token)


def _stamp_heartbeat(mongo: Any, job_id: str, fields: dict[str, Any]) -> None:
    """Write a liveness stamp and message without moving ``updated_at``.

    ``updated_at`` is the operator's silence signal (``job_stall_seconds`` in
    get_job and the job list). A pulse that refreshed it hid a worker blocked
    for good behind a silent source: the job looked alive forever. The pulse
    writes ``heartbeat_at`` instead; ``updated_at`` moves on real progress.
    """
    from datetime import datetime, timezone

    fields = {**fields, "heartbeat_at": datetime.now(timezone.utc).isoformat()}
    try:
        mongo.update_job_fields(job_id, fields, touch_updated_at=False)
    except TypeError:
        # A store without the flag (tests' doubles): the stamp still lands.
        mongo.update_job_fields(job_id, fields)


def _phase_pulse_message(
    message: str, elapsed: float, bound: float | None, overdue: str | None
) -> str:
    shown = int(elapsed)
    if bound is not None and elapsed > bound:
        # Past the bound the pulse stops asserting liveness: the worker is
        # alive, but the system it waits on has not answered.
        return overdue or (
            f"{message} — no answer after {shown}s (longer than {bound:g}s). "
            "The system this step waits on has not answered; check it, or "
            "cancel the job."
        )
    return f"{message} ({shown}s elapsed — the job has not stalled)"


@contextmanager
def job_phase_heartbeat(
    mongo: Any,
    job_id: str,
    *,
    phase: str,
    message: str,
    interval_s: float = 15.0,
    progress_pct: Any = None,
    records_processed: int | None = None,
    total_rows: int | None = None,
    until: Any = None,
    bound_seconds: float | None = None,
    overdue_message: str | None = None,
) -> Iterator[None]:
    """Pulse a long phase so the job does not look stalled (D19).

    The first write sets the phase and message through ``update_job_status``
    (real progress: the run reached this step). Every later pulse stamps
    ``heartbeat_at`` and the elapsed-time message *without* touching
    ``updated_at``, so ``job_stall_seconds`` keeps counting a worker that is
    blocked for good (a silent source) while the stamp shows the worker is
    alive. Past ``bound_seconds`` the message says the system has not answered
    (``overdue_message`` or a default) instead of "has not stalled".

    ``until`` (optional, no arguments) ends the pulse early once it returns
    true, e.g. when committed batches start reporting their own progress or
    the attempt was superseded.

    ``reconcile_phase_heartbeat`` owns the 98–99 band; this is the same
    contract for any other phase — connect, destination create, slow source
    reads — whose progress_pct/records_processed must not regress or claim
    work it cannot measure.
    """
    from services.tracing import thread_in_context

    stop = threading.Event()
    started = time.monotonic()
    fixed: dict[str, Any] = {"phase": phase}
    if progress_pct is not None:
        fixed["progress_pct"] = progress_pct
    if records_processed is not None:
        fixed["records_processed"] = records_processed
    if total_rows is not None:
        fixed["total_rows"] = total_rows

    def _pulse() -> None:
        while not stop.wait(interval_s):
            if until is not None and until():
                return
            elapsed = time.monotonic() - started
            try:
                _stamp_heartbeat(
                    mongo,
                    job_id,
                    {
                        **fixed,
                        "message": _phase_pulse_message(
                            message, elapsed, bound_seconds, overdue_message
                        ),
                    },
                )
            except Exception:
                return

    thread = thread_in_context(_pulse, name=f"job-heartbeat-{job_id[:8]}")
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
