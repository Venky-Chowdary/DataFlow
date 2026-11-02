"""Reconcile-phase job heartbeats (extracted from engine for F8 size budgets)."""

from __future__ import annotations

import contextvars
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
        stop = threading.Event()
        started = time.monotonic()

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
                    waiting = ""
                    if hashed <= 0:
                        waiting = (
                            " No destination row has been read yet; a metadata "
                            "lock fails within 120s."
                        )
                    pulse = (
                        f"Reconciling data ({elapsed}s) — checksum scan still "
                        f"running for {processed:,} rows.{hashed_phrase}{waiting} "
                        "The job has not stalled."
                        f"{checksum_phrase}"
                    )
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

        # Run under a copy of the caller's context so the pulse's logs keep the
        # job_id/correlation_id bound by the engine; raw threads start empty.
        thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(_pulse,),
            name=f"reconcile-heartbeat-{job_id[:8]}",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=1.0)
    finally:
        reset_reconcile_scan(token)


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
) -> Iterator[None]:
    """Pulse a long phase so the job does not look stalled (D19).

    ``reconcile_phase_heartbeat`` owns the 98–99 band; this is the same
    contract for any other phase — connect, destination create, slow source
    reads — whose progress_pct/records_processed must not regress or claim
    work it cannot measure. The pulse keeps the phase and stamps elapsed
    seconds; the stored heartbeat timestamp itself proves liveness.
    """
    stop = threading.Event()
    started = time.monotonic()

    def _pulse() -> None:
        while not stop.wait(interval_s):
            elapsed = int(time.monotonic() - started)
            kwargs: dict[str, Any] = {
                "phase": phase,
                "message": f"{message} ({elapsed}s elapsed — the job has not stalled)",
            }
            if progress_pct is not None:
                kwargs["progress_pct"] = progress_pct
            if records_processed is not None:
                kwargs["records_processed"] = records_processed
            if total_rows is not None:
                kwargs["total_rows"] = total_rows
            try:
                mongo.update_job_status(job_id, "running", **kwargs)
            except Exception:
                return

    thread = threading.Thread(
        target=contextvars.copy_context().run,
        args=(_pulse,),
        name=f"job-heartbeat-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
