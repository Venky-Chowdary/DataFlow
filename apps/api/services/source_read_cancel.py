"""End the held source read as soon as a run is cancelled (D302, R10).

A full-refresh PostgreSQL/Redshift read holds one REPEATABLE READ session for
the whole run (``services.source_snapshot``) and ends it when the stream
returns. A cancel is noticed by the writers, so while a destination write is
stalled (a lock wait, a black-holed endpoint, a slow commit) the run cannot
return and the customer's source sat ``idle in transaction`` holding
AccessShareLock on the table and pinning ``backend_xmin`` (QA: 12+ minutes,
a DROP TABLE on the source waited).

:class:`SourceReadCancelWatch` polls the run's cancel probe from a small
daemon thread once per ``interval`` seconds. On a cancel (or a superseded
attempt) it interrupts any running source query and closes the snapshot
connection, which ends the transaction on the server. The read path then
gets ``TransferCancelled`` from :func:`raise_if_aborted` instead of a closed
connection, so the run still ends as cancelled.

Two rules keep that safe:

* The connection is closed only while no other thread is inside libpq on it
  (:func:`services.cancel_check.libpq_gate`): psycopg2 runs a COPY's data
  loop without its own connection lock, so a close there freed the
  connection under the COPY thread.
* Once a route has settled the watch (right before it commits the
  destination), no abort can start any more: a cancel that arrives later is
  a cancel after the write, reported as such by the engine, never a closed
  source under committed rows.
"""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from typing import Any, Callable

from services.error_handling import TransferCancelled

logger = logging.getLogger(__name__)

#: Seconds between cancel polls; the snapshot ends within about this long.
POLL_SECONDS = 1.0

#: How long an abort waits for a COPY thread to leave libpq before giving up
#: on the close (the server-side cancel is resent on every wait).
GATE_WAIT_SECONDS = 30.0


class SourceReadCancelWatch:
    def __init__(
        self,
        conn: Any,
        probe: Callable[[int | None], None],
        *,
        interval: float = POLL_SECONDS,
    ) -> None:
        self._conn = conn
        self._probe = probe
        self._interval = interval
        # The probe reads the run attempt from this context (D168 fence).
        self._ctx = contextvars.copy_context()
        self._stop = threading.Event()
        # Serialises "abort" against "stop": an abort starts only while the
        # watch is live, and stop() returns only once no abort is in flight.
        self._lock = threading.Lock()
        self._stopped = False
        self.aborted: TransferCancelled | None = None
        self._thread = threading.Thread(
            target=self._run, name="df-source-cancel-watch", daemon=True
        )

    def start(self) -> "SourceReadCancelWatch":
        self._thread.start()
        return self

    def stop(self) -> None:
        """No abort starts after this returns; one in flight has finished."""
        with self._lock:
            self._stopped = True
            self._stop.set()

    def settle(self) -> None:
        """:meth:`stop`, then ``TransferCancelled`` if the watch ended the read."""
        self.stop()
        self.raise_if_aborted()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._ctx.run(self._probe, None)
            except TransferCancelled as exc:
                with self._lock:
                    if self._stopped:
                        # Settled while the probe ran: the route is committing
                        # (or has); the cancel is no longer ours to act on.
                        return
                    self.aborted = exc
                    abort_connection(self._conn)
                return

    def raise_if_aborted(self) -> None:
        if self.aborted is not None:
            raise TransferCancelled(
                str(self.aborted) or "Transfer cancelled by user",
                superseded=bool(getattr(self.aborted, "superseded", False)),
            )


def _call(conn: Any, step: str) -> None:
    fn = getattr(conn, step, None)
    if fn is None:
        return
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - best effort on a cancelled run
        logger.debug("source read %s on cancel skipped: %s", step, exc)


def abort_connection(conn: Any, *, gate_wait: float | None = None) -> bool:
    """Interrupt the running statement, then close: the server ends the transaction.

    ``cancel()`` (libpq ``PQcancel``) is thread-safe and stops a running query
    or COPY on the server, which also brings a COPY thread out of libpq. The
    close waits for :func:`~services.cancel_check.libpq_gate`, so it never
    frees the connection under a thread still inside libpq. Returns whether
    the connection was closed.
    """
    from services.cancel_check import libpq_gate

    _call(conn, "cancel")
    gate = libpq_gate(conn)
    if gate is None:
        _call(conn, "close")
        return True
    budget = GATE_WAIT_SECONDS if gate_wait is None else gate_wait
    deadline = time.monotonic() + budget
    while not gate.acquire(timeout=0.25):
        if time.monotonic() >= deadline:
            logger.warning(
                "source read did not leave libpq within %.0fs of the cancel; "
                "the session is left to the server-side cancel",
                budget,
            )
            return False
        _call(conn, "cancel")
    try:
        _call(conn, "close")
    finally:
        gate.release()
    return True


def start_watch(conn: Any) -> SourceReadCancelWatch | None:
    """Watch ``conn`` when a cancel probe owns this run; else ``None``."""
    from services.cancel_check import get_cancel_probe

    probe = get_cancel_probe()
    if probe is None or conn is None:
        return None
    return SourceReadCancelWatch(conn, probe).start()


def settle_watch(watch: SourceReadCancelWatch | None) -> None:
    """Settle ``watch`` right before a destination commit (see the module notes)."""
    if watch is not None:
        watch.settle()


def stop_watch(watch: SourceReadCancelWatch | None) -> None:
    """Stop ``watch`` without raising: the write it guarded has already landed."""
    if watch is not None:
        watch.stop()


def as_cancel(
    watch: SourceReadCancelWatch | None, exc: BaseException
) -> BaseException:
    """What to raise for ``exc``: the cancel, when the watch ended the read.

    Closing the source session under a running read surfaces as a driver
    error ("connection already closed"); the run was cancelled, not failed.
    """
    if (
        watch is not None
        and watch.aborted is not None
        and not isinstance(exc, TransferCancelled)
    ):
        return TransferCancelled(
            str(watch.aborted) or "Transfer cancelled by user",
            superseded=bool(getattr(watch.aborted, "superseded", False)),
        )
    return exc
