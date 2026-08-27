"""Reconcile-phase job heartbeats (extracted from engine for F8 size budgets)."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator


@contextmanager
def reconcile_phase_heartbeat(
    mongo: Any,
    job_id: str,
    *,
    processed: int,
    total: int,
    interval_s: float = 8.0,
    proof_kind: str = "full",
) -> Iterator[None]:
    """Hold progress at 99% and keep live UI messaging fresh during reconcile.

    Reconciliation can take minutes on large tables (COUNT + checksum queries).
    Without heartbeats the theater freezes on the last write event and looks stuck.

    ``proof_kind=write_pass`` is file/stream write-pass compare — dest COUNT +
    dest fingerprint vs the write-pass hash. It cannot earn migration_proven.
    """
    write_pass = str(proof_kind or "").strip().lower() in {
        "write_pass",
        "inline_write_pass",
        "write_pass_fingerprints",
    }
    enter_msg = (
        "All rows written — dest COUNT + dest fingerprint vs write-pass "
        f"({processed:,} rows; not migration_proven)…"
        if write_pass
        else (
            "All rows written — reconciling destination "
            f"({processed:,} rows: counts + checksum proof)…"
        )
    )
    mongo.update_job_status(
        job_id,
        "running",
        phase="reconcile",
        progress_pct=99,
        records_processed=processed,
        total_rows=total,
        message=enter_msg,
    )
    stop = threading.Event()
    started = time.monotonic()

    def _pulse() -> None:
        while not stop.wait(interval_s):
            elapsed = int(time.monotonic() - started)
            pulse = (
                f"Reconciling ({elapsed}s) — dest COUNT and fingerprint for "
                f"{processed:,} rows (write-pass compare, not migration_proven)…"
                if write_pass
                else (
                    f"Reconciling data ({elapsed}s) — verifying row counts "
                    f"and checksums for {processed:,} rows…"
                )
            )
            mongo.update_job_status(
                job_id,
                "running",
                phase="reconcile",
                progress_pct=99,
                records_processed=processed,
                total_rows=total,
                message=pulse,
            )

    thread = threading.Thread(
        target=_pulse,
        name=f"reconcile-heartbeat-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1.0)
