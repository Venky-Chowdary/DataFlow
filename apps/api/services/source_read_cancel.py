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
"""

from __future__ import annotations

import contextvars
import logging
import threading
from typing import Any, Callable

from services.error_handling import TransferCancelled

logger = logging.getLogger(__name__)

#: Seconds between cancel polls; the snapshot ends within about this long.
POLL_SECONDS = 1.0


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
        self.aborted: TransferCancelled | None = None
        self._thread = threading.Thread(
            target=self._run, name="df-source-cancel-watch", daemon=True
        )

    def start(self) -> "SourceReadCancelWatch":
        self._thread.start()
        return self

    def stop(self, *, wait: bool = False) -> None:
        self._stop.set()
        if wait and self._thread.is_alive() and self._thread is not threading.current_thread():
            # The poll wakes on the event at once; an abort in flight finishes.
            self._thread.join(timeout=max(self._interval, 1.0) * 5)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._ctx.run(self._probe, None)
            except TransferCancelled as exc:
                self.aborted = exc
                abort_connection(self._conn)
                return

    def raise_if_aborted(self) -> None:
        if self.aborted is not None:
            raise TransferCancelled(
                str(self.aborted) or "Transfer cancelled by user",
                superseded=bool(getattr(self.aborted, "superseded", False)),
            )


def abort_connection(conn: Any) -> None:
    """Interrupt a running statement, then close: the server ends the transaction."""
    for step in ("cancel", "close"):
        fn = getattr(conn, step, None)
        if fn is None:
            continue
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - best effort on a cancelled run
            logger.debug("source read %s on cancel skipped: %s", step, exc)


def start_watch(conn: Any) -> SourceReadCancelWatch | None:
    """Watch ``conn`` when a cancel probe owns this run; else ``None``."""
    from services.cancel_check import get_cancel_probe

    probe = get_cancel_probe()
    if probe is None or conn is None:
        return None
    return SourceReadCancelWatch(conn, probe).start()


def settle_watch(watch: SourceReadCancelWatch | None) -> None:
    """Stop ``watch`` before a commit; ``TransferCancelled`` if it ended the read.

    A route that commits its destination after the read must not race the
    watch: once this returns, no cancel can close the source under the
    commit, and a cancel that already did is raised instead of committing.
    """
    if watch is None:
        return
    watch.stop(wait=True)
    watch.raise_if_aborted()


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
