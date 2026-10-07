"""Reconcile-phase job heartbeats (extracted from engine for F8 size budgets)."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator


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
    interval_s: float = 8.0,
    proof_kind: str = "full",
    checksum_scope: str = "",
    checksum_table: str = "",
    stream_count: int = 0,
) -> Iterator[None]:
    """Hold progress at 99% and keep live UI messaging fresh during reconcile.

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
            if write_pass:
                pulse = (
                    f"Reconciling ({elapsed}s) — dest COUNT and fingerprint for "
                    f"{processed:,} rows (write-pass compare, not migration_proven)…"
                )
            elif last_stream:
                pulse = (
                    f"Reconciling data ({elapsed}s) — {processed:,} rows written "
                    f"across the job. Checksum is the last stream ({table}), "
                    "not the whole job…"
                )
            else:
                pulse = (
                    f"Reconciling data ({elapsed}s) — checksum scan still "
                    f"running for {processed:,} rows. The job has not stalled."
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
