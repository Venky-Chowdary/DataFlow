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
import weakref
from typing import Any, Callable

from services.error_handling import TransferCancelled

logger = logging.getLogger(__name__)

#: Seconds between cancel polls; the snapshot ends within about this long.
POLL_SECONDS = 1.0

#: The D302 promise: a cancel ends the source session within this long.
#: Past it, the job record says the session is still open.
RELEASE_BUDGET_SECONDS = 5.0

#: How often the server-side cancel is resent while a COPY thread is still
#: inside libpq (a pooler or proxy may drop a cancel request).
CANCEL_RESEND_SECONDS = 1.0

#: Owner-side close of a session whose COPY thread is still inside libpq
#: (a destination failure during a silent source scan): bounded wait.
OWNER_CLOSE_SECONDS = 10.0


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
        # The probe reads the run attempt from this context (D168 fence), and
        # the job-record notes below find the run's store through it.
        self._ctx = contextvars.copy_context()
        self._stop = threading.Event()
        # "abort starts" and "stop" are serialised: an abort starts only while
        # the watch is live; once stop() returns none can start.
        self._lock = threading.Lock()
        self._stopped = False
        self.aborted: TransferCancelled | None = None
        #: Whether the cancel closed the session (None: no cancel acted).
        self.released: bool | None = None
        self._pid = _backend_pid(conn)
        self._thread = threading.Thread(
            target=self._run, name="df-source-cancel-watch", daemon=True
        )

    def start(self) -> "SourceReadCancelWatch":
        self._thread.start()
        return self

    def stop(self) -> None:
        """No abort starts after this returns; one already started gives up
        waiting (the owner closes the session itself, through the gate)."""
        with self._lock:
            self._stopped = True
            self._stop.set()

    def settle(self) -> None:
        """:meth:`stop`, then ``TransferCancelled`` if the watch acted on a cancel."""
        self.stop()
        self.raise_if_aborted()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

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
                self._ctx.run(self._abort)
                return

    def _abort(self) -> None:
        started = time.monotonic()

        def slow() -> None:
            _note_source_still_open(self._pid)

        self.released = abort_connection(
            self._conn,
            until=self._stop.is_set,
            on_slow=slow,
            slow_after=RELEASE_BUDGET_SECONDS,
        )
        if self.released and time.monotonic() - started > RELEASE_BUDGET_SECONDS:
            from services.cancel_check import note_on_job

            note_on_job({
                "source_session_released_after_s": round(time.monotonic() - started, 1),
            })

    def raise_if_aborted(self) -> None:
        if self.aborted is not None:
            raise TransferCancelled(
                str(self.aborted) or "Transfer cancelled by user",
                superseded=bool(getattr(self.aborted, "superseded", False)),
            )


def _backend_pid(conn: Any) -> int | None:
    fn = getattr(conn, "get_backend_pid", None)
    if fn is None:
        return None
    try:
        return int(fn())
    except Exception:  # noqa: BLE001 - only used to name the session
        return None


def _note_source_still_open(pid: int | None) -> None:
    from services.cancel_check import note_on_job

    who = f" (server process {pid})" if pid else ""
    message = (
        f"The cancel could not close the source database session{who} within "
        f"{RELEASE_BUDGET_SECONDS:.0f} seconds. It is still open: it keeps its "
        "read lock on the source table and holds back vacuum until the database "
        "acts on the cancel or the read in progress returns. If it stays open, "
        "end it on the source database"
        + (f" (SELECT pg_terminate_backend({pid}))." if pid else ".")
    )
    logger.warning(message)
    note_on_job({"source_session_warning": message})


#: Per-connection lock around ``cancel()`` / ``close()`` from any thread.
_CANCEL_CLOSE_LOCKS: "weakref.WeakKeyDictionary[Any, threading.Lock]" = weakref.WeakKeyDictionary()
_CANCEL_CLOSE_GUARD = threading.Lock()


def _cancel_close_lock(conn: Any) -> threading.Lock | None:
    try:
        with _CANCEL_CLOSE_GUARD:
            lock = _CANCEL_CLOSE_LOCKS.get(conn)
            if lock is None:
                lock = _CANCEL_CLOSE_LOCKS[conn] = threading.Lock()
            return lock
    except TypeError:
        return None


def _call(conn: Any, step: str) -> bool:
    """``conn.cancel()`` / ``conn.close()``, never both at once.

    psycopg2's close frees the connection's cancel handle (``PGcancel``); a
    ``cancel()`` from another thread at that moment used it after the free.
    Both run under one per-connection lock, and a cancel of a closed
    connection is skipped.
    """
    fn = getattr(conn, step, None)
    if fn is None:
        return True
    lock = _cancel_close_lock(conn)
    try:
        if lock is not None:
            lock.acquire()
        try:
            if step == "cancel" and getattr(conn, "closed", 0):
                return True
            fn()
            return True
        finally:
            if lock is not None:
                lock.release()
    except Exception as exc:  # noqa: BLE001 - best effort on a cancelled run
        # A cancel that fails leaves the session open: say so. A failing
        # close of an already broken session is routine.
        log = logger.warning if step == "cancel" else logger.debug
        log("source read %s on cancel failed: %s", step, exc)
        return False


def cancel_connection(conn: Any) -> bool:
    """Thread-safe ``PQcancel`` of ``conn``'s running statement (see :func:`_call`)."""
    return _call(conn, "cancel")


def _detach_from_pool(conn: Any) -> None:
    """Make ``close()`` end the session even if an endpoint scope lent it.

    A pooled PostgreSQL session's ``close()`` rolls back and hands it back to
    its scope (connectors.postgresql_conn.ScopedConnection). A cancelled
    source session must really end, never be lent to the next consumer.
    """
    if getattr(conn, "_df_release", None) is not None:
        try:
            conn._df_release = None
        except Exception:  # noqa: BLE001 - not a scoped connection
            logger.debug("pool detach skipped", exc_info=True)


def abort_connection(
    conn: Any,
    *,
    until: Callable[[], bool] | None = None,
    gate_wait: float | None = None,
    on_slow: Callable[[], None] | None = None,
    slow_after: float | None = None,
) -> bool:
    """Interrupt the running statement, then close: the server ends the session.

    ``cancel()`` (libpq ``PQcancel``) is thread-safe and stops a running query
    or COPY on the server, which also brings a COPY thread out of libpq; it is
    resent every second while waiting. The close waits for
    :func:`~services.cancel_check.libpq_gate`, so it never frees the
    connection under a thread still inside libpq. The wait ends when the gate
    is free, ``until()`` is true, or ``gate_wait`` seconds pass; ``on_slow``
    runs once if it lasts past ``slow_after``. Returns whether it closed.
    """
    from services.cancel_check import libpq_gate

    _call(conn, "cancel")
    _detach_from_pool(conn)
    gate = libpq_gate(conn)
    if gate is None:
        _call(conn, "close")
        return True
    if slow_after is None:
        slow_after = RELEASE_BUDGET_SECONDS
    started = time.monotonic()
    last_cancel = started
    slow_reported = False
    while not gate.acquire(timeout=0.25):
        now = time.monotonic()
        if until is not None and until():
            return False
        if gate_wait is not None and now - started >= gate_wait:
            logger.warning(
                "source session still inside a read %.0fs after the cancel; "
                "not closing it under that read",
                now - started,
            )
            return False
        if on_slow is not None and not slow_reported and now - started >= slow_after:
            slow_reported = True
            on_slow()
        if now - last_cancel >= CANCEL_RESEND_SECONDS:
            last_cancel = now
            _call(conn, "cancel")
    try:
        _call(conn, "close")
    finally:
        gate.release()
    return True


def close_connection(conn: Any, *, budget: float = OWNER_CLOSE_SECONDS) -> bool:
    """The owner's close of a source session, safe against its own COPY thread.

    After a normal end nothing is inside libpq and this is a plain close. When
    the destination failed while the COPY thread was still waiting inside
    libpq for the next rows (a silent source scan), a bare ``close()`` freed
    the connection under it: cancel the COPY first and close once it is out,
    within ``budget`` seconds. Returns whether it closed.
    """
    from services.cancel_check import libpq_gate

    if conn is None or getattr(conn, "closed", 0):
        return True
    gate = libpq_gate(conn)
    if gate is None:
        _call(conn, "close")
        return True
    if gate.acquire(blocking=False):
        try:
            _call(conn, "close")
        finally:
            gate.release()
        return True
    return abort_connection(conn, gate_wait=budget)


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
