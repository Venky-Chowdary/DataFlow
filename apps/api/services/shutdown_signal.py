"""A process-wide "shutting down" flag, set the moment SIGTERM arrives (ops review High 4).

uvicorn runs the lifespan teardown (where the API stops its background work)
only once every open connection has closed. A job's live-progress stream
stays open for up to six hours, so one viewer kept the pod from shutting down
until Kubernetes killed it. Long-lived responses check :func:`requested` and
end at once, telling the client to reconnect (to another replica).

:func:`install` chains a handler in front of the server's own SIGTERM/SIGINT
handlers: it sets the flag, then calls the handler that was there. Call it
from the lifespan startup, after uvicorn has installed its handlers.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
import time

logger = logging.getLogger(__name__)

_event = threading.Event()
#: Set only by a real SIGTERM/SIGINT (not by :func:`request`).
_signalled = threading.Event()
_SIGNALS = (signal.SIGTERM, signal.SIGINT)
_POLL_SECONDS = 0.2


def requested() -> bool:
    """True once the process has been asked to stop."""
    return _event.is_set()


def request() -> None:
    _event.set()


def reset() -> None:
    """Forget a shutdown request (tests and in-process restarts only)."""
    _event.clear()
    _signalled.clear()


def signalled() -> bool:
    """True once this process received SIGTERM or SIGINT (since the last :func:`reset`).

    Unlike :func:`requested`, an in-process lifespan teardown (tests) does not
    set it: what it closes stays closed until the process exits.
    """
    return _signalled.is_set()


def install() -> bool:
    """Chain the flag in front of the current SIGTERM/SIGINT handlers. False off the main thread."""
    if threading.current_thread() is not threading.main_thread():
        return False
    for sig in _SIGNALS:
        previous = signal.getsignal(sig)
        if getattr(previous, "_datawrap_shutdown_flag", False):
            continue

        def handler(signum, frame, _previous=previous):
            _signalled.set()
            _event.set()
            if callable(_previous):
                _previous(signum, frame)
            elif _previous == signal.SIG_DFL:
                signal.signal(signum, signal.SIG_DFL)
                signal.raise_signal(signum)

        handler._datawrap_shutdown_flag = True  # type: ignore[attr-defined]
        signal.signal(sig, handler)
    return True


async def sleep_unless_shutdown(seconds: float) -> bool:
    """Sleep ``seconds``; return True early (and at once) when shutdown is requested."""
    deadline = time.monotonic() + max(0.0, seconds)
    while not _event.is_set():
        left = deadline - time.monotonic()
        if left <= 0:
            return False
        await asyncio.sleep(min(_POLL_SECONDS, left))
    return True
