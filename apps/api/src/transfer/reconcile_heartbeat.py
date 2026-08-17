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
) -> Iterator[None]:
    """Hold progress at 99% and keep live UI messaging fresh during reconcile.

    Reconciliation can take minutes on large tables (COUNT + checksum queries).
    Without heartbeats the theater freezes on the last write event and looks stuck.
    """
    mongo.update_job_status(
        job_id,
        "running",
        phase="reconcile",
        progress_pct=99,
        records_processed=processed,
        total_rows=total,
        message=(
            "All rows written — reconciling destination "
            f"({processed:,} rows: counts + checksum proof)…"
        ),
    )
    stop = threading.Event()
    started = time.monotonic()

    def _pulse() -> None:
        while not stop.wait(interval_s):
            elapsed = int(time.monotonic() - started)
            mongo.update_job_status(
                job_id,
                "running",
                phase="reconcile",
                progress_pct=99,
                records_processed=processed,
                total_rows=total,
                message=(
                    f"Reconciling data ({elapsed}s) — verifying row counts "
                    f"and checksums for {processed:,} rows…"
                ),
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
