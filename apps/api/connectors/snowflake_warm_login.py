"""A Snowflake plan login outlives the tool call that opened it (QA D187).

``plan_transfer`` and ``start_transfer`` each opened one Snowflake login inside
their 45s deadline, and run 2's plan opened another. A real account's login is
the slowest single step of a plan (TLS, OCSP, authentication, session setup:
seconds, more across regions or behind a proxy), and every call paid it again
for an endpoint the previous call had just logged in to.

When a tool call's endpoint scope ends, a plan login whose session is still the
one the login set up (same session parameters, same warehouse / database /
schema / role) is parked here for a short while instead of being closed. The
next tool call for the same endpoint and credentials takes it instead of
logging in. Catalog answers are not kept: a job may have created the table in
between. A parked login past its idle time, or closed by the server, is closed
and replaced by a fresh login.

``DATAFLOW_SNOWFLAKE_WARM_LOGIN_SECONDS`` (default 240; 0 turns it off) bounds
the idle time, well under Snowflake's own session idle timeout. The local
emulator (fakesnow) only parks when ``DATAFLOW_SNOWFLAKE_WARM_LOGIN_EMULATOR=1``:
its catalog is a per-test file.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Hashable
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_IDLE_SECONDS = 240.0

_lock = threading.Lock()
#: endpoint key -> [(raw connection, session context, parked at)]
_parked: dict[Hashable, list[tuple[Any, dict[str, str], float]]] = {}
_stats = {"parked": 0, "taken": 0, "expired": 0}


def idle_seconds() -> float:
    raw = os.environ.get("DATAFLOW_SNOWFLAKE_WARM_LOGIN_SECONDS", "")
    try:
        value = float(raw) if raw.strip() else DEFAULT_IDLE_SECONDS
    except ValueError:
        value = DEFAULT_IDLE_SECONDS
    return max(0.0, value)


def enabled(local_emulator: bool) -> bool:
    if idle_seconds() <= 0:
        return False
    if local_emulator:
        return os.environ.get("DATAFLOW_SNOWFLAKE_WARM_LOGIN_EMULATOR", "") == "1"
    return True


def _close(conn: Any) -> None:
    try:
        conn.close()
    except Exception as exc:  # noqa: BLE001 - closing must never raise
        logger.debug("parked Snowflake login close failed: %s", exc)


def _is_closed(conn: Any) -> bool:
    try:
        return bool(conn.is_closed())
    except Exception:  # noqa: BLE001 - an unreadable login is not reused
        return True


def take(key: Hashable) -> tuple[Any, dict[str, str]] | None:
    """A parked login for ``key`` and the session context it holds, or ``None``."""
    limit = idle_seconds()
    now = time.monotonic()
    stale: list[Any] = []
    found: tuple[Any, dict[str, str]] | None = None
    with _lock:
        entries = _parked.get(key) or []
        while entries:
            conn, context, at = entries.pop()
            if now - at > limit or _is_closed(conn):
                stale.append(conn)
                continue
            found = (conn, context)
            break
        if not entries:
            _parked.pop(key, None)
        _stats["expired"] += len(stale)
        if found is not None:
            _stats["taken"] += 1
    for conn in stale:
        _close(conn)
    return found


def park(key: Hashable, conn: Any, context: dict[str, str]) -> bool:
    """Keep ``conn`` for the next call to ``key``; ``False`` means close it."""
    if _is_closed(conn):
        return False
    limit = idle_seconds()
    now = time.monotonic()
    stale: list[Any] = []
    with _lock:
        for other_key in list(_parked):
            keep = []
            for entry in _parked[other_key]:
                (stale if now - entry[2] > limit else keep).append(entry)
            if keep:
                _parked[other_key] = keep
            else:
                _parked.pop(other_key, None)
        _parked.setdefault(key, []).append((conn, dict(context), now))
        _stats["parked"] += 1
        _stats["expired"] += len(stale)
    for entry in stale:
        _close(entry[0])
    return True


def open_or_take(key: Hashable, open_fn: Callable[[], Any]) -> tuple[Any, dict[str, str] | None]:
    """``(connection, context)``: a parked login when there is one, else a new one."""
    got = take(key)
    if got is not None:
        return got
    return open_fn(), None


def stats() -> dict[str, int]:
    with _lock:
        return {**_stats, "idle": sum(len(v) for v in _parked.values())}


def reset_for_tests() -> None:
    with _lock:
        conns = [entry[0] for entries in _parked.values() for entry in entries]
        _parked.clear()
        for name in _stats:
            _stats[name] = 0
    for conn in conns:
        _close(conn)
