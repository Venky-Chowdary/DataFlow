"""Short, bounded calls to the shared control-plane database (ops review High 1).

Tenant settings, ACL grants and BYOK key records are read on the request
path. The application's MongoDB client waits up to 5 s for server selection
and 20 s on a socket, which is right for a transfer and wrong for a lookup
that every request makes: during an outage each request would wait the full
timeout, and the probes that share the process would time out with it.

``bounded(collection)`` wraps a pymongo collection so that every call on it
runs under :func:`pymongo.timeout` (server selection, the network round trip
and the server's ``maxTimeMS`` together), about one second by default. A call
that fails because the database cannot be reached raises
:class:`ControlPlaneUnavailable`, and the failure is remembered for a short,
bounded window: calls in that window fail at once instead of each paying the
timeout again. Callers fail closed on it (a 503, a refused ACL check), never
by reading a local file.

Settings (``DATAFLOW_`` or ``DATAWRAP_`` prefix):

``CONTROL_STORE_TIMEOUT_SECONDS``  per-call bound for reads, default 1, at most 5.
``CONTROL_STORE_WRITE_TIMEOUT_SECONDS``  per-call bound for writes (updates,
                                   inserts, deletes, transaction commits) and
                                   for every call inside a read-modify-write
                                   (:func:`write_scope`), default 3, at most
                                   10: rare admin writes under compare-and-swap
                                   contention.
``CONTROL_STORE_OUTAGE_SECONDS``   how long an outage is remembered, default 2,
                                   at most 10 (0 turns the memory off).

What is remembered as an outage: server selection timing out or a connection
refused/lost at once, and socket timeouts only when ``SOCKET_TIMEOUTS_FOR_OUTAGE``
(3) of them come in a row with no call succeeding between them (a database
that stopped answering on open connections). One slow call on a working
database (a socket timeout among successes, ``maxTimeMS``, a write-concern
timeout) fails closed on its own: remembering it failed every other call of
the process for the outage window (PR 406 CI: 2 slow calls became 123
failures).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from services.brand_env import getenv_brand
from services.control_plane_store import ControlPlaneUnavailable

DEFAULT_TIMEOUT_SECONDS = 1.0
MAX_TIMEOUT_SECONDS = 5.0
DEFAULT_WRITE_TIMEOUT_SECONDS = 3.0
MAX_WRITE_TIMEOUT_SECONDS = 10.0
#: Collection methods that write; they run under the write bound.
WRITE_METHODS = frozenset({
    "insert_one", "insert_many", "update_one", "update_many", "replace_one",
    "delete_one", "delete_many", "find_one_and_update", "find_one_and_replace",
    "find_one_and_delete", "bulk_write",
})
DEFAULT_OUTAGE_SECONDS = 2.0
MAX_OUTAGE_SECONDS = 10.0

#: Socket timeouts in a row, with no success between them, that mean the
#: database stopped answering (not one slow call).
SOCKET_TIMEOUTS_FOR_OUTAGE = 3

_lock = threading.Lock()
_outage: dict[str, Any] = {"until": 0.0, "reason": "", "timeouts_in_a_row": 0}


def _setting(name: str, default: float, low: float, high: float) -> float:
    try:
        value = float(getenv_brand(name, "") or default)
    except ValueError:
        value = default
    return min(high, max(low, value))


def operation_timeout_seconds() -> float:
    return _setting("CONTROL_STORE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS, 0.05, MAX_TIMEOUT_SECONDS)


def write_timeout_seconds() -> float:
    return _setting(
        "CONTROL_STORE_WRITE_TIMEOUT_SECONDS", DEFAULT_WRITE_TIMEOUT_SECONDS, 0.05, MAX_WRITE_TIMEOUT_SECONDS
    )


def outage_seconds() -> float:
    return _setting("CONTROL_STORE_OUTAGE_SECONDS", DEFAULT_OUTAGE_SECONDS, 0.0, MAX_OUTAGE_SECONDS)


def outage_retry_at() -> float:
    """``time.monotonic()`` value until which calls fail at once (0: no outage remembered)."""
    with _lock:
        return float(_outage["until"])


def reset_outage_state() -> None:
    with _lock:
        _outage["until"] = 0.0
        _outage["reason"] = ""
        _outage["timeouts_in_a_row"] = 0


def _note_success() -> None:
    with _lock:
        _outage["timeouts_in_a_row"] = 0


def _socket_timeouts_mean_outage() -> bool:
    """Count one more socket timeout; True once enough came with no success between."""
    with _lock:
        _outage["timeouts_in_a_row"] = int(_outage["timeouts_in_a_row"]) + 1
        return _outage["timeouts_in_a_row"] >= SOCKET_TIMEOUTS_FOR_OUTAGE


def note_outage(exc: BaseException) -> None:
    window = outage_seconds()
    if window <= 0:
        return
    with _lock:
        _outage["until"] = time.monotonic() + window
        _outage["reason"] = type(exc).__name__


def raise_if_recent_outage() -> None:
    with _lock:
        until, reason = float(_outage["until"]), str(_outage["reason"])
    left = until - time.monotonic()
    if left > 0:
        raise ControlPlaneUnavailable(
            f"the control-plane database is unavailable ({reason}); the next attempt is in {left:.1f}s"
        )


def is_unreachable(exc: BaseException) -> bool:
    """A failure to reach the database (or a timeout), as opposed to a refused write."""
    try:
        from pymongo.errors import ConnectionFailure, PyMongoError
    except ImportError:  # pragma: no cover - pymongo is a dependency of the API
        return False
    if isinstance(exc, ConnectionFailure):
        return True
    return isinstance(exc, PyMongoError) and bool(getattr(exc, "timeout", False))


def is_outage(exc: BaseException) -> bool:
    """The database could not be reached at all, as opposed to one slow call.

    Server selection timing out, or a connection refused or lost, means every
    other call would wait too: that is remembered. A socket timeout on a
    working connection (``NetworkTimeout``), the server's ``maxTimeMS``
    (``ExecutionTimeout``) or a write-concern timeout is this call only.
    """
    try:
        from pymongo.errors import (
            ConnectionFailure,
            ExecutionTimeout,
            NetworkTimeout,
            ServerSelectionTimeoutError,
            WTimeoutError,
        )
    except ImportError:  # pragma: no cover - pymongo is a dependency of the API
        return False
    if isinstance(exc, ServerSelectionTimeoutError):
        return True
    if isinstance(exc, (NetworkTimeout, ExecutionTimeout, WTimeoutError)):
        return False
    return isinstance(exc, ConnectionFailure)


def _is_socket_timeout(exc: BaseException) -> bool:
    try:
        from pymongo.errors import NetworkTimeout
    except ImportError:  # pragma: no cover
        return False
    return isinstance(exc, NetworkTimeout)


def is_slow_query(exc: BaseException) -> bool:
    """The server answered that this one operation ran out of time (``maxTimeMS``).

    That call fails (closed), but the database is reachable: it is not
    remembered as an outage, so one slow query does not fail every store of
    the process for the outage window (ops review round 2, High 3).
    """
    try:
        from pymongo.errors import ExecutionTimeout
    except ImportError:  # pragma: no cover - pymongo is a dependency of the API
        return False
    return isinstance(exc, ExecutionTimeout)


_write_scope = threading.local()


@contextmanager
def write_scope() -> Iterator[None]:
    """Every call in this block runs under the write bound.

    The read of a read-modify-write is part of the write: under the short read
    bound, a shared store that was slow but healthy made an admin's grant fail
    while its write would have had the write bound (PR 406 CI, 16 writers on
    one tenant). Request-path reads outside a write keep the short bound.
    """
    _write_scope.depth = getattr(_write_scope, "depth", 0) + 1
    try:
        yield
    finally:
        _write_scope.depth -= 1


class _Deadline:
    """``pymongo.timeout`` around one call, with outage bookkeeping."""

    def __init__(self, *, write: bool = False) -> None:
        write = write or getattr(_write_scope, "depth", 0) > 0
        self._seconds = write_timeout_seconds() if write else operation_timeout_seconds()

    def __enter__(self) -> "_Deadline":
        import pymongo

        raise_if_recent_outage()
        self._ctx = pymongo.timeout(self._seconds)
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> bool:
        self._ctx.__exit__(exc_type, exc, tb)
        if exc is None:
            _note_success()
            return False
        if is_unreachable(exc) or is_outage(exc):
            if is_outage(exc) or (_is_socket_timeout(exc) and _socket_timeouts_mean_outage()):
                note_outage(exc)
            raise ControlPlaneUnavailable(
                f"the control-plane database did not answer within {self._seconds:.1f}s"
            ) from exc
        return False


def deadline(*, write: bool = False) -> _Deadline:
    return _Deadline(write=write)


class _BoundedCursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def sort(self, *args: Any, **kwargs: Any) -> "_BoundedCursor":
        self._cursor = self._cursor.sort(*args, **kwargs)
        return self

    def skip(self, n: int) -> "_BoundedCursor":
        self._cursor = self._cursor.skip(n)
        return self

    def limit(self, n: int) -> "_BoundedCursor":
        self._cursor = self._cursor.limit(n)
        return self

    def __iter__(self):
        with deadline():
            rows = list(self._cursor)
        return iter(rows)


class _BoundedDatabase:
    def __init__(self, database: Any) -> None:
        self._database = database

    def __getitem__(self, name: str) -> "BoundedCollection":
        return BoundedCollection(self._database[name])

    def command(self, *args: Any, **kwargs: Any) -> Any:
        with deadline():
            return self._database.command(*args, **kwargs)


class BoundedCollection:
    """A pymongo collection whose every call is short and bounded (see module docstring)."""

    def __init__(self, collection: Any) -> None:
        self._coll = collection

    @property
    def name(self) -> str:
        return str(getattr(self._coll, "name", ""))

    @property
    def database(self) -> _BoundedDatabase:
        return _BoundedDatabase(self._coll.database)

    def find(self, *args: Any, **kwargs: Any) -> _BoundedCursor:
        return _BoundedCursor(self._coll.find(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._coll, name)
        if not callable(target):
            return target

        write = name in WRITE_METHODS

        def call(*args: Any, **kwargs: Any) -> Any:
            with deadline(write=write):
                return target(*args, **kwargs)

        return call


def bounded(collection: Any) -> Any:
    """``collection`` with bounded calls; None stays None."""
    return None if collection is None else BoundedCollection(collection)
