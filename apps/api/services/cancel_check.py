"""Shared cancellation probe for transfer write paths (D102).

The engine's ``on_checkpoint`` callback fires only after a batch commits and
is further throttled — a fast copy can finish before the first effective
check runs, so a cancelled job still lands every row. This module is the
single cancel probe both the row writers and the COPY fast paths call
*between* batches (and inside long writes where the engine allows), so a
cancel stops further writes instead of being noticed at the end.
"""

from __future__ import annotations

import contextvars
import logging
from typing import Any, Callable

from services.error_handling import TransferCancelled

logger = logging.getLogger(__name__)

# The transfer engine registers the active job's probe here so COPY fast paths
# and writers can poll it without threading a callback through ~30 call sites.
# A ContextVar, not a thread-local: transfers run on a worker thread and keep
# concurrent jobs isolated, but the chunk writers run on a pool that copies the
# caller's context (ChunkDispatcher) - a thread-local was invisible there, so a
# cancel never reached a writer already past its first chunk (D168).
_probe: contextvars.ContextVar[Callable[[int | None], None] | None] = (
    contextvars.ContextVar("df_cancel_probe", default=None)
)


#: The run attempt this context belongs to. A job keeps one id across resume,
#: so "cancel cleared" cannot tell an old worker still running from the new one
#: (D168): resume cleared the cancel flag and the old worker simply carried on
#: writing. Each execution stamps ``run_attempt`` on the job; a context whose
#: attempt no longer matches is superseded and stops.
_attempt: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "df_run_attempt", default=None
)


def set_cancel_probe(fn: Callable[[int | None], None] | None) -> None:
    _probe.set(fn)


def begin_run_attempt(mongo: Any, job_id: str) -> str | None:
    """Stamp a fresh attempt id on the job and bind it to this context.

    Returns ``None`` (and binds nothing) when the store cannot record it - an
    attempt that cannot be proven current must not be able to find itself
    superseded.
    """
    import uuid

    if mongo is None or not job_id:
        return None
    attempt = uuid.uuid4().hex
    try:
        stamped = mongo.update_job_fields(job_id, {"run_attempt": attempt})
    except Exception as exc:  # noqa: BLE001 - the fence is a guard, not a gate
        logger.debug("run attempt not recorded for %s: %s", job_id, exc)
        return None
    if not stamped:
        return None
    _attempt.set(attempt)
    return attempt


def end_run_attempt() -> None:
    _attempt.set(None)


def get_cancel_probe() -> Callable[[int | None], None] | None:
    return _probe.get()


def cancel_checkpoint(rows_written: int | None = None) -> None:
    """Poll the active probe; raise ``TransferCancelled`` when cancelled.

    Called between batches and inside long writes. A no-op when no engine
    run owns this thread (e.g. tools calling a copy helper directly).
    """
    fn = _probe.get()
    if fn is not None:
        fn(rows_written)
    meter = _meter.get()
    if meter is not None and rows_written is not None:
        meter.observe_cumulative(rows_written)
    elif meter is not None:
        meter.beat()


# --- per-batch progress (D168) ----------------------------------------------
# The engine's on_checkpoint fires once per *chunk* (thousands of rows) and
# COPY paths report nothing until they finish, so a job read 0 rows / 5% with a
# frozen updated_at while the destination grew batch by batch. Every batch
# boundary already calls cancel_checkpoint; the same seam now feeds a throttled
# progress write to the job row.

_meter: contextvars.ContextVar["ProgressMeter | None"] = contextvars.ContextVar(
    "df_progress_meter", default=None
)
#: Rows the current context's writer call had reported (per pool task, since
#: each task runs in its own copied context).
_last_written: contextvars.ContextVar[int] = contextvars.ContextVar(
    "df_last_written", default=0
)


class ProgressMeter:
    """Accumulates committed rows and writes them to the job row, throttled."""

    def __init__(self, mongo: Any, job_id: str, *, interval: float = 1.0) -> None:
        import threading

        self._mongo = mongo
        self._job_id = job_id
        self._interval = interval
        self._lock = threading.Lock()
        self._rows = 0
        self._last_flush = 0.0

    def observe_cumulative(self, written: int) -> None:
        """``written`` is cumulative within one writer call; a smaller value
        than the last one means a new call started in this context."""
        last = _last_written.get()
        delta = written - last if written >= last else written
        _last_written.set(written)
        self.add(delta)

    def add(self, delta: int) -> None:
        with self._lock:
            self._rows += max(0, int(delta or 0))
        self.beat()

    def beat(self) -> None:
        import time

        now = time.monotonic()
        with self._lock:
            if now - self._last_flush < self._interval:
                return
            self._last_flush = now
            rows = self._rows
        self._flush(rows)

    def _flush(self, rows: int) -> None:
        try:
            job = _read_job(self._mongo, self._job_id) or {}
            if str(job.get("status") or "") != "running" or _is_superseded(job):
                return
            fields: dict[str, Any] = {}
            shown = int(job.get("records_processed") or 0)
            if rows > shown:
                from services.batch_progress import compute_transfer_progress_pct

                fields["records_processed"] = rows
                pct = compute_transfer_progress_pct(
                    phase="writing",
                    rows_processed=rows,
                    total_rows=int(job.get("total_rows") or 0),
                )
                if pct is not None and pct > float(job.get("progress_pct") or 0):
                    fields["progress_pct"] = pct
            # Even with nothing new to show, a live batch is a heartbeat: the
            # stall watchdog and the Jobs list read updated_at.
            from datetime import datetime, timezone

            self._mongo.update_job_fields(
                self._job_id,
                fields or {"last_batch_at": datetime.now(timezone.utc).isoformat()},
            )
        except Exception as exc:  # noqa: BLE001 - progress must never fail a write
            logger.debug("progress heartbeat skipped for %s: %s", self._job_id, exc)


def set_progress_meter(meter: "ProgressMeter | None") -> None:
    _meter.set(meter)
    _last_written.set(0)


def report_rows(rows: int) -> None:
    """COPY loops: ``rows`` were just read for (and are about to be) loaded."""
    meter = _meter.get()
    if meter is not None and rows:
        meter.add(rows)


def _read_job(mongo: Any, job_id: str) -> dict | None:
    if mongo is None or not job_id:
        return None
    try:
        return mongo.get_job(job_id)
    except Exception as exc:  # noqa: BLE001 - a failed read must not hide the transfer
        logger.warning("Cancellation check failed: %s", exc, exc_info=exc)
        return None


def _is_superseded(job: dict | None) -> bool:
    bound = _attempt.get()
    current = str((job or {}).get("run_attempt") or "")
    return bool(bound and current and current != bound)


def attempt_superseded(mongo: Any, job_id: str) -> bool:
    """True when a newer attempt of this job has taken over from this context."""
    return _is_superseded(_read_job(mongo, job_id))


def job_cancel_requested(mongo: Any, job_id: str) -> bool:
    """True when the operator (or a lost lease) flagged this job to stop."""
    job = _read_job(mongo, job_id)
    return bool(
        job
        and (
            job.get("cancel_requested")
            or str(job.get("status") or "") == "cancelled"
        )
    )


def raise_if_job_cancelled(
    mongo: Any, job_id: str, *, rows_written: int | None = None
) -> None:
    """Raise ``TransferCancelled`` when the job was cancelled.

    ``rows_written`` is whatever this chunk already committed, so the
    cancelled job does not report 0 rows for a table that now holds them.
    """
    if attempt_superseded(mongo, job_id):
        raise TransferCancelled(
            "Transfer superseded by a newer attempt of this job",
            rows_written=rows_written,
            superseded=True,
        )
    if job_cancel_requested(mongo, job_id):
        raise TransferCancelled(
            "Transfer cancelled by user", rows_written=rows_written
        )


def cancel_check_for(mongo: Any, job_id: str) -> Callable[[int | None], None]:
    """Callable ``check(rows_written)`` for threading into write/copy loops."""

    def _check(rows_written: int | None = None) -> None:
        raise_if_job_cancelled(mongo, job_id, rows_written=rows_written)

    return _check
