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

``CONTROL_STORE_TIMEOUT_SECONDS``  per-call bound, default 1, at most 5.
``CONTROL_STORE_OUTAGE_SECONDS``   how long a failure is remembered, default 2,
                                   at most 10 (0 turns the memory off).
"""

from __future__ import annotations

import threading
import time
from typing import Any

from services.brand_env import getenv_brand
from services.control_plane_store import ControlPlaneUnavailable

DEFAULT_TIMEOUT_SECONDS = 1.0
MAX_TIMEOUT_SECONDS = 5.0
DEFAULT_OUTAGE_SECONDS = 2.0
MAX_OUTAGE_SECONDS = 10.0

_lock = threading.Lock()
_outage: dict[str, Any] = {"until": 0.0, "reason": ""}


def _setting(name: str, default: float, low: float, high: float) -> float:
    try:
        value = float(getenv_brand(name, "") or default)
    except ValueError:
        value = default
    return min(high, max(low, value))


def operation_timeout_seconds() -> float:
    return _setting("CONTROL_STORE_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS, 0.05, MAX_TIMEOUT_SECONDS)


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


class _Deadline:
    """``pymongo.timeout`` around one call, with outage bookkeeping."""

    def __enter__(self) -> "_Deadline":
        import pymongo

        raise_if_recent_outage()
        self._ctx = pymongo.timeout(operation_timeout_seconds())
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> bool:
        self._ctx.__exit__(exc_type, exc, tb)
        if exc is not None and is_unreachable(exc):
            note_outage(exc)
            raise ControlPlaneUnavailable(
                f"the control-plane database did not answer within {operation_timeout_seconds():.1f}s"
            ) from exc
        return False


def deadline() -> _Deadline:
    return _Deadline()


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

        def call(*args: Any, **kwargs: Any) -> Any:
            with deadline():
                return target(*args, **kwargs)

        return call


def bounded(collection: Any) -> Any:
    """``collection`` with bounded calls; None stays None."""
    return None if collection is None else BoundedCollection(collection)
