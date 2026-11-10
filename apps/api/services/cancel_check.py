"""Shared cancellation probe for transfer write paths (D102).

The engine's ``on_checkpoint`` callback fires only after a batch commits and
is further throttled — a fast copy can finish before the first effective
check runs, so a cancelled job still lands every row. This module is the
single cancel probe both the row writers and the COPY fast paths call
*between* batches (and inside long writes where the engine allows), so a
cancel stops further writes instead of being noticed at the end.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
from typing import Any, Callable, Iterator

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


def poll_cancel(rows_written: int | None = None) -> None:
    """Raise ``TransferCancelled`` when the run was cancelled; count nothing.

    For a loop that writes nothing (the post-write source re-read). Unlike
    :func:`cancel_checkpoint` it never feeds the progress meter: the meter reads
    its argument as cumulative within one writer call, so a job total there
    would be added again as new rows. ``rows_written`` only labels the cancel.
    """
    fn = _probe.get()
    if fn is not None:
        fn(rows_written)


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


class _CancelKeepingFile:
    """File proxy for a COPY callback that remembers a cancel raised inside it."""

    def __init__(self, inner: Any, poll: bool) -> None:
        self._inner = inner
        self._poll = poll
        self.cancelled: TransferCancelled | None = None

    def _call(self, name: str, *args: Any) -> Any:
        try:
            if self._poll:
                cancel_checkpoint()
            return getattr(self._inner, name)(*args)
        except TransferCancelled as exc:
            self.cancelled = exc
            raise

    def read(self, *args: Any) -> Any:
        return self._call("read", *args)

    def readline(self, *args: Any) -> Any:
        return self._call("readline", *args)

    def write(self, *args: Any) -> Any:
        return self._call("write", *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def copy_expert_keeping_cancel(cur: Any, sql: str, file: Any, *, poll: bool = False) -> None:
    """``cursor.copy_expert`` that lets a job cancel out as ``TransferCancelled``.

    A cancel probe that fires inside the COPY's ``read()`` / ``write()``
    callback is caught by psycopg and re-raised as a driver error ("error in
    .read() call: TransferCancelled ..."). The route then saw a COPY failure,
    not a cancel: the job's record kept the progress meter's row count with no
    cancel outcome, and the operator read "1,024 rows retained" for a COPY
    that had rolled back (QA C16). ``poll`` also polls the probe on every
    callback (the reader/writer does not poll itself).
    """
    proxy = _CancelKeepingFile(file, poll)
    try:
        cur.copy_expert(sql, proxy)
    except TransferCancelled:
        raise
    except Exception as exc:
        if proxy.cancelled is not None:
            raise proxy.cancelled from exc
        raise


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
        #: Set by :meth:`rewind`: the next flush may lower the shown count.
        self._rewound = False

    def mark(self) -> int:
        """Rows counted so far, for :meth:`rewind`."""
        with self._lock:
            return self._rows

    def rewind(self, mark: int) -> None:
        """Forget rows counted after ``mark``: that write was rolled back.

        A COPY reports the rows it reads; when it declines to the row path
        (or fails) its transaction is rolled back and the row path re-reads
        the same rows from the start. Without this the job showed both
        passes: a failed 40,000-row attempt reached 62,528.
        """
        with self._lock:
            if self._rows <= mark:
                return
            self._rows = max(0, int(mark))
            self._rewound = True
            self._last_flush = 0.0
        self.beat()

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
            rewound, self._rewound = self._rewound, False
        self._flush(rows, rewound=rewound)

    def _flush(self, rows: int, *, rewound: bool = False) -> None:
        try:
            job = _read_job(self._mongo, self._job_id) or {}
            if str(job.get("status") or "") != "running" or _is_superseded(job):
                return
            fields: dict[str, Any] = {}
            shown = int(job.get("records_processed") or 0)
            # Safety net only: the count itself is rewound when a COPY's rows
            # are rolled back (rewind). A job never processes more rows than
            # its source holds, whatever else fed the meter.
            known_total = int(job.get("total_rows") or 0)
            if known_total > 0:
                rows = min(rows, known_total)
            if rows > shown or (rewound and rows < shown):
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


#: Seconds between pulses while a run waits on one blocking step (D19).
BLOCKING_STEP_PULSE_SECONDS = 15.0


def _blocking_step_bound_seconds() -> float:
    """How long a wait may claim "has not stalled": the existing readiness
    bound for the oldest in-flight connector call (default 300 s)."""
    from services.connector_executor import (
        DEFAULT_READY_MAX_AGE_SECONDS,
        _float_setting,
    )

    return _float_setting(
        "DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS", DEFAULT_READY_MAX_AGE_SECONDS
    )


@contextlib.contextmanager
def blocking_step(message: str, *, phase: str = "reading") -> Iterator[None]:
    """Say what the run is waiting on, and keep the job's heartbeat moving.

    Between the engine's last phase message and the first committed batch the
    run can wait minutes on one call (the source executing its query, a first
    page over a slow link). Nothing reached the job row in that window: it read
    "Connecting to destination and creating table..." with ``updated_at``
    frozen while the wait was the *source* (D19). Inside this scope the message
    names the real wait at once and the existing phase pulse
    (``job_phase_heartbeat``) stamps ``heartbeat_at`` until rows start
    arriving; from then on the per-batch meter is the heartbeat. The pulse
    never moves ``updated_at`` (the operator's stall signal) and past the
    readiness bound it says the source has not answered. A no-op outside a
    run, for a cancelled job (the status fence refuses it) and for a
    superseded attempt; it also stops as soon as the attempt is superseded.
    """
    meter = _meter.get()
    if meter is None:
        yield
        return
    mongo, job_id = meter._mongo, meter._job_id
    rows_at_entry = meter.mark()
    try:
        job = _read_job(mongo, job_id) or {}
        live = str(job.get("status") or "") == "running" and not _is_superseded(job)
        if live:
            mongo.update_job_status(job_id, "running", phase=phase, message=message)
    except Exception as exc:  # noqa: BLE001 - a status line must never fail the read
        logger.debug("blocking-step message skipped for %s: %s", job_id, exc)
        live = False
    if not live:
        yield
        return
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    with job_phase_heartbeat(
        mongo,
        job_id,
        phase=phase,
        message=message,
        interval_s=BLOCKING_STEP_PULSE_SECONDS,
        until=lambda: meter.mark() > rows_at_entry or attempt_superseded(mongo, job_id),
        bound_seconds=_blocking_step_bound_seconds(),
    ):
        yield


@contextlib.contextmanager
def wait_pulse(message: str) -> Iterator[None]:
    """Keep the heartbeat moving while one source call waits (D287).

    ``blocking_step`` names its wait at once, at the cost of a job read and a
    status write: right for a run's first page, too much for every page. This
    pulse writes nothing unless the call outlasts one pulse interval; from
    then on it stamps ``heartbeat_at`` and ``message`` with the elapsed time
    (never ``updated_at``), and past the readiness bound it says the source
    has not answered. A Kafka page read waiting on a broker that stopped
    answering left the job on its last message ("Writing batch 1/2 (10
    rows)...") with no heartbeat at all. The phase is left as it is. A no-op
    outside a run; it stops for a cancelled or superseded attempt.
    """
    store = active_job_store()
    if store is None:
        yield
        return
    mongo, job_id = store
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    def _not_live() -> bool:
        job = _read_job(mongo, job_id) or {}
        return str(job.get("status") or "") != "running" or _is_superseded(job)

    with job_phase_heartbeat(
        mongo,
        job_id,
        phase="",
        message=message,
        interval_s=BLOCKING_STEP_PULSE_SECONDS,
        until=_not_live,
        bound_seconds=_blocking_step_bound_seconds(),
    ):
        yield


def active_job_store() -> tuple[Any, str] | None:
    """``(job store, job id)`` of the run this context belongs to, or ``None``.

    The engine binds one meter per run; a long step deep in a writer (the
    source re-read after the last batch) reports through the same store.
    """
    meter = _meter.get()
    if meter is None or not meter._job_id:
        return None
    return meter._mongo, meter._job_id


def set_progress_meter(meter: "ProgressMeter | None") -> None:
    _meter.set(meter)
    _last_written.set(0)


def progress_mark() -> int | None:
    """The current context's committed-row count, for :func:`progress_rewind`."""
    meter = _meter.get()
    return meter.mark() if meter is not None else None


def progress_rewind(mark: int | None) -> None:
    """Roll the progress count back to ``mark`` (a rolled-back write)."""
    meter = _meter.get()
    if meter is not None and mark is not None:
        meter.rewind(mark)
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
