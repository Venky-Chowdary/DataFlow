"""One session per endpoint for the length of one tool call, for every engine.

A planning tool reads the same customer system from many steps: catalog
reads, the connection probe, the privilege probe, the sample, the row count,
the fit scan and the uniqueness probe. Each opened its own session. On a
remote PostgreSQL a connect with TLS costs a few hundred milliseconds; a
Snowflake login costs seconds; an SFTP login costs a TCP handshake, a key
exchange and an authentication round trip, and a single ``start_transfer``
opened more than a hundred of them (D187, D189).

Inside :func:`endpoint_session_scope` a connector's factory calls
:func:`borrow` instead of dialling: a consumer that finished hands its
session back, and the next consumer of the same endpoint gets it instead of
a new login. Two modes:

* ``shared=True``: one session serves every consumer at once (Snowflake: each
  consumer takes its own cursor; ``close`` by a consumer is a no-op).
* exclusive (default): a session serves one consumer at a time. A consumer
  that is still holding its session while another asks gets a second one;
  serial consumers reuse one. ``reset`` runs before a session is handed out
  again (PostgreSQL: roll back, restore the session settings).

The scope closes every session when it ends. Outside a scope ``borrow`` is
the factory itself: Execute and writers open and close their own sessions.

``snowflake_plan_scope`` (D187) is this scope.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
from collections.abc import Callable, Hashable, Iterable, Iterator
from typing import Any

logger = logging.getLogger(__name__)


class _Pool:
    def __init__(self, engine: str) -> None:
        self.engine = engine
        self.idle: list[Any] = []
        self.shared: Any = None
        self.opened = 0
        self.reused = 0
        self.closers: list[tuple[Any, Callable[[Any], None]]] = []


class _Scope:
    def __init__(self, engines: frozenset[str] | None = None) -> None:
        #: Engines this scope shares; ``None`` means every engine.
        self.engines = engines
        self.lock = threading.Lock()
        self.pools: dict[tuple[str, Hashable], _Pool] = {}
        self.closed = False

    def stats(self) -> dict[str, dict[str, int]]:
        with self.lock:
            out: dict[str, dict[str, int]] = {}
            for (engine, _key), pool in self.pools.items():
                agg = out.setdefault(engine, {"endpoints": 0, "opened": 0, "reused": 0})
                agg["endpoints"] += 1
                agg["opened"] += pool.opened
                agg["reused"] += pool.reused
            return out


_scope: contextvars.ContextVar[_Scope | None] = contextvars.ContextVar(
    "df_endpoint_session_scope", default=None
)


def _close_quietly(conn: Any, close: Callable[[Any], None]) -> None:
    try:
        close(conn)
    except Exception as exc:  # noqa: BLE001 - closing must never raise
        logger.debug("endpoint session close failed: %s", exc)


@contextlib.contextmanager
def endpoint_session_scope(engines: Iterable[str] | None = None) -> Iterator[None]:
    """Share one session per endpoint until the block ends (re-entrant).

    ``engines`` limits sharing to those engines (a transfer job shares its
    SFTP login; its database sessions stay its own).
    """
    if _scope.get() is not None:
        yield
        return
    scope = _Scope(frozenset(engines) if engines is not None else None)
    token = _scope.set(scope)
    try:
        yield
    finally:
        _scope.reset(token)
        with scope.lock:
            scope.closed = True
            closers = [c for pool in scope.pools.values() for c in pool.closers]
            scope.pools.clear()
        for conn, close in closers:
            _close_quietly(conn, close)


def in_scope(engine: str = "") -> bool:
    scope = _scope.get()
    if scope is None or scope.closed:
        # A step still running after its tool call ended (abandoned at its
        # budget) holds a copy of a scope that is gone: it owns its sessions.
        return False
    return not engine or scope.engines is None or engine in scope.engines


def scope_stats() -> dict[str, dict[str, int]]:
    """Sessions opened and reused per engine in the current scope (for tests)."""
    scope = _scope.get()
    return scope.stats() if scope is not None else {}


def borrow(
    engine: str,
    key: Hashable,
    open_fn: Callable[[], Any],
    *,
    close: Callable[[Any], None] = lambda c: c.close(),
    alive: Callable[[Any], bool] | None = None,
    reset: Callable[[Any], None] | None = None,
    shared: bool = False,
) -> tuple[Any, Callable[[], None] | None]:
    """A session for ``(engine, key)`` and the function that hands it back.

    Returns ``(conn, release)``. Outside a scope ``release`` is ``None`` and
    the caller owns ``conn`` as before. Inside a scope the caller must call
    ``release()`` instead of closing (shared sessions: ``release`` is a no-op).
    ``key`` must hold everything that makes two logins different (host, port,
    database, user, credential, role, warehouse...).
    """
    scope = _scope.get()
    if (
        scope is None
        or scope.closed
        or (scope.engines is not None and engine not in scope.engines)
    ):
        # Closed: nothing would ever close a session opened into it (D187).
        return open_fn(), None
    pool_key = (engine, key)
    with scope.lock:
        pool = scope.pools.get(pool_key)
        if pool is None:
            pool = scope.pools[pool_key] = _Pool(engine)
        if shared and pool.shared is not None:
            pool.reused += 1
            return pool.shared, (lambda: None)
        candidate = pool.idle.pop() if (not shared and pool.idle) else None
    if candidate is not None:
        usable = True
        try:
            if alive is not None and not alive(candidate):
                usable = False
            elif reset is not None:
                reset(candidate)
        except Exception as exc:  # noqa: BLE001 - a broken session is replaced
            logger.debug("endpoint session for %s not reusable: %s", engine, exc)
            usable = False
        if usable:
            with scope.lock:
                pool.reused += 1
            return candidate, _releaser(scope, pool, candidate, close)
        _close_quietly(candidate, close)
        with scope.lock:
            pool.closers = [(c, f) for c, f in pool.closers if c is not candidate]
    conn = open_fn()
    with scope.lock:
        pool.opened += 1
        pool.closers.append((conn, close))
        if shared:
            if pool.shared is None:
                pool.shared = conn
            else:  # another thread logged in first: keep one, close ours
                pool.closers.pop()
                winner = pool.shared
                pool.reused += 1
            if pool.shared is conn:
                return conn, (lambda: None)
    if shared:
        _close_quietly(conn, close)
        return winner, (lambda: None)
    return conn, _releaser(scope, pool, conn, close)


def _releaser(scope: _Scope, pool: _Pool, conn: Any, close: Callable[[Any], None]):
    done = threading.Event()

    def release() -> None:
        if done.is_set():
            return
        done.set()
        with scope.lock:
            if not scope.closed:
                pool.idle.append(conn)
                return
        _close_quietly(conn, close)

    return release
