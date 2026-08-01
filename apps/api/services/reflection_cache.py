"""Per-table metadata cache for the transfer hot path.

Reading a table in chunks used to re-reflect its full definition on every
chunk: a fresh ``MetaData()`` plus ``autoload_with=engine``, which round-trips
for columns, primary keys and constraints. Writing was no better — every write
chunk called ``inspect(engine).has_table(...)``, and MySQL additionally queried
``INFORMATION_SCHEMA.COLUMNS`` per chunk. On a 500-chunk transfer that is
1,500+ metadata round-trips describing a table whose shape does not change.

The correctness question is not "is caching faster" but "when is the cached
answer wrong". A table's shape changes exactly when *this process* runs DDL
against it — creating it, adding a drift column, widening a type, or dropping
it. Every one of those paths calls :func:`invalidate_table` here, so the cache
is coherent with our own writes.

It is deliberately **not** coherent with DDL run by someone else mid-transfer.
That is the right trade: a concurrent external ``ALTER`` during a running load
already breaks the write in ways a metadata cache cannot rescue, and the
alternative — re-reflecting every chunk — is the cost this module exists to
remove. Entries also expire, so a long-lived process eventually re-reads.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_TTL_SECONDS = 300
DEFAULT_MAX_ENTRIES = 512


def cache_enabled() -> bool:
    return os.getenv("DATAFLOW_SCHEMA_CACHE", "1").lower() not in ("0", "false", "off", "no")


def _ttl() -> float:
    try:
        return max(1.0, float(os.getenv("DATAFLOW_SCHEMA_CACHE_TTL", str(DEFAULT_TTL_SECONDS))))
    except ValueError:
        return float(DEFAULT_TTL_SECONDS)


def _max_entries() -> int:
    try:
        return max(1, int(os.getenv("DATAFLOW_SCHEMA_CACHE_SIZE", str(DEFAULT_MAX_ENTRIES))))
    except ValueError:
        return DEFAULT_MAX_ENTRIES


@dataclass
class _Entry:
    value: Any
    expires_at: float


@dataclass
class ReflectionCacheStats:
    hits: int = 0
    misses: int = 0
    invalidations: int = 0
    expirations: int = 0
    live: int = 0

    @property
    def reuse_ratio(self) -> float:
        total = self.hits + self.misses
        return round(self.hits / total, 4) if total else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "invalidations": self.invalidations,
            "expirations": self.expirations,
            "live": self.live,
            "reuse_ratio": self.reuse_ratio,
            "metadata_queries_saved": self.hits,
        }


_CACHE: dict[tuple[str, str, str, str], _Entry] = {}
_LOCK = threading.RLock()
_STATS = ReflectionCacheStats()


def engine_identity(engine: Any) -> str:
    """Stable, credential-free identity for the engine a table lives behind.

    ``str(url)`` masks the password but keeps driver, user, host, port and
    database, which is exactly the identity that decides whether two callers
    are looking at the same physical table. ``id(engine)`` would be wrong:
    after an eviction a new object can land on a recycled address and silently
    inherit another target's cached schema.
    """
    try:
        return str(getattr(engine, "url", "")) or repr(engine)
    except Exception:
        return repr(engine)


def dsn_identity(
    *,
    driver: str,
    host: str = "",
    port: Any = "",
    database: str = "",
    username: str = "",
    connection_string: str = "",
) -> str:
    """Identity for the raw-DBAPI paths that never build an Engine.

    The native psycopg2 / PyMySQL readers and writers ask the same
    ``information_schema`` questions per chunk but have no Engine to key on.
    Passwords are deliberately excluded — they do not change which table you
    are looking at, and keeping them out means a cache key is never a secret.
    """
    return "|".join(
        str(part or "")
        for part in (driver, host, port, database, username, connection_string)
    )


def get_or_load(
    engine: Any,
    schema: str | None,
    table: str,
    kind: str,
    loader: Callable[[], T],
) -> T:
    """Return a cached metadata value, loading it once per table per TTL.

    ``kind`` separates the different questions asked about one table
    (``reflect``, ``has_table``, ``columns``) so invalidation can drop all of
    them together without them colliding with each other.
    """
    return get_or_load_by_identity(engine_identity(engine), schema, table, kind, loader)


def get_or_load_by_identity(
    identity: str,
    schema: str | None,
    table: str,
    kind: str,
    loader: Callable[[], T],
) -> T:
    """:func:`get_or_load` for callers holding a DSN string instead of an Engine."""
    if not cache_enabled():
        return loader()

    key = (str(identity), str(schema or ""), str(table or ""), kind)
    now = time.monotonic()

    with _LOCK:
        entry = _CACHE.get(key)
        if entry is not None:
            if entry.expires_at > now:
                _STATS.hits += 1
                return entry.value
            # Expired: drop it and fall through to reload.
            _CACHE.pop(key, None)
            _STATS.expirations += 1
        _STATS.misses += 1

    # Load outside the lock. A slow reflection against one database must not
    # stall lookups for every other table. Two threads racing on a cold key
    # both load; they compute the same answer, so the duplicate work is bounded
    # and the result is identical.
    value = loader()

    with _LOCK:
        _CACHE[key] = _Entry(value=value, expires_at=time.monotonic() + _ttl())
        _evict_if_needed_locked()
        _STATS.live = len(_CACHE)
    return value


def peek_by_identity(identity: str, schema: str | None, table: str, kind: str) -> Any:
    """Return a cached value without loading, or ``None`` when absent/expired.

    For callers whose loader is only correct to cache conditionally — the MySQL
    writer must not cache an "empty" column list, because empty means the table
    does not exist yet and that is exactly the answer its own DDL invalidates.
    """
    if not cache_enabled():
        return None
    key = (str(identity), str(schema or ""), str(table or ""), kind)
    with _LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            _STATS.misses += 1
            return None
        if entry.expires_at <= time.monotonic():
            _CACHE.pop(key, None)
            _STATS.expirations += 1
            _STATS.misses += 1
            return None
        _STATS.hits += 1
        return entry.value


def put_by_identity(
    identity: str, schema: str | None, table: str, kind: str, value: Any
) -> None:
    """Store a value the caller loaded itself. Pairs with :func:`peek_by_identity`."""
    if not cache_enabled():
        return
    key = (str(identity), str(schema or ""), str(table or ""), kind)
    with _LOCK:
        _CACHE[key] = _Entry(value=value, expires_at=time.monotonic() + _ttl())
        _evict_if_needed_locked()
        _STATS.live = len(_CACHE)


def invalidate_table(engine: Any, schema: str | None, table: str) -> int:
    """Drop every cached answer for one table. Call after any DDL on it."""
    return invalidate_by_identity(engine_identity(engine), schema, table)


def invalidate_by_identity(identity: str, schema: str | None, table: str) -> int:
    """:func:`invalidate_table` for DSN-keyed entries."""
    identity = str(identity)
    schema_s = str(schema or "")
    table_s = str(table or "")
    with _LOCK:
        doomed = [
            key
            for key in _CACHE
            if key[0] == identity and key[1] == schema_s and key[2] == table_s
        ]
        for key in doomed:
            _CACHE.pop(key, None)
        if doomed:
            _STATS.invalidations += len(doomed)
        _STATS.live = len(_CACHE)
    return len(doomed)


def clear() -> None:
    with _LOCK:
        _CACHE.clear()
        _STATS.live = 0


def reset_for_tests() -> None:
    with _LOCK:
        _CACHE.clear()
        _STATS.hits = 0
        _STATS.misses = 0
        _STATS.invalidations = 0
        _STATS.expirations = 0
        _STATS.live = 0


def stats() -> ReflectionCacheStats:
    with _LOCK:
        return ReflectionCacheStats(
            hits=_STATS.hits,
            misses=_STATS.misses,
            invalidations=_STATS.invalidations,
            expirations=_STATS.expirations,
            live=len(_CACHE),
        )


def _evict_if_needed_locked() -> None:
    """Trim to the size bound, soonest-to-expire first."""
    limit = _max_entries()
    if len(_CACHE) <= limit:
        return
    ordered = sorted(_CACHE.items(), key=lambda kv: kv[1].expires_at)
    for key, _ in ordered[: len(_CACHE) - limit]:
        _CACHE.pop(key, None)
