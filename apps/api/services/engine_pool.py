"""Process-wide SQLAlchemy Engine cache.

Before this module every read chunk, every write chunk, and every checksum
re-read called ``create_engine`` and then ``engine.dispose()`` in a ``finally``.
A transfer of N chunks therefore built and tore down roughly ``3N`` connection
pools: build a pool, check out exactly one connection, throw the pool away.
On a network database that is a full TCP connect plus TLS handshake plus auth
round-trip per chunk, and it also meant SQLAlchemy's per-dialect reflection
cache was cold every single chunk.

An Engine is designed to be a long-lived, thread-safe object that *owns* a
connection pool. Creating one per operation is the documented anti-pattern.
This module makes the Engine what it was meant to be while keeping the call
sites unchanged: they still ask for an engine and still call a release helper
in ``finally``; the release is now a no-op for pooled engines.

Design notes worth stating because a reviewer will ask:

* **The cache key is derived, not the cfg dict.** Config dicts carry
  ``endpoint.extra`` — arbitrary operator JSON that can contain lists — so
  ``frozenset(cfg.items())`` raises ``TypeError``. The key is built from the
  closed set of fields that actually change the connection URL, each coerced to
  ``str`` so ``port=3306`` and ``port="3306"`` collapse to one entry.
* **Bounded with LRU eviction.** A workspace with hundreds of connectors must
  not accumulate hundreds of live pools. Evicted engines are disposed.
* **Passwords never appear in a log line.** The key is hashed for display; the
  raw key stays in memory only.
* **Opt-out exists.** ``DATAFLOW_ENGINE_CACHE=0`` restores the old
  build-per-call behaviour, so a deployment that hits an unforeseen pooling
  problem has a switch rather than a rollback.
"""

from __future__ import annotations

import hashlib
import logging
import os
from services.brand_env import getenv_brand
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: Maximum number of distinct engines held at once. Each engine owns a pool, so
#: this bounds real file descriptors, not just Python objects.
DEFAULT_MAX_ENGINES = 32

#: Fields that can change the resulting connection URL. Anything outside this
#: set (``table``, ``metadata_columns``, ``connector_id``, …) must not affect
#: engine identity, or every table in a database would get its own pool.
#: TLS / SID / driver keywords belong here: ODBC Driver 18 encrypts and
#: verifies by default, so ``trust_server_certificate`` changes which
#: handshake the pooled engine will attempt. Reusing a verify-or-fail engine
#: after the operator declared trust (or the reverse) is a different server.
_KEY_FIELDS = (
    "type",
    "host",
    "port",
    "database",
    "username",
    "password",
    "schema",
    "connection_string",
    "warehouse",
    "role",
    "account",
    "auth_mode",
    "auth_role",
    "private_key",
    "service_account",
    "ssl",
    "server_certificate",
    "hostname_in_certificate",
    "trust_server_certificate",
    "encrypt",
    "sslmode",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "ssl_ca",
    "ssl_cert",
    "ssl_key",
    "ssl_verify_cert",
    "ssl_disabled",
    "service_name",
    "sid",
    "driver",
    "odbc_driver",
    "connect_timeout",
    "multi_subnet_failover",
    "MultiSubnetFailover",
    "application_intent",
    "ApplicationIntent",
)


def cache_enabled() -> bool:
    """Whether engines are reused. Off restores per-call construction."""
    return getenv_brand("ENGINE_CACHE", "1").lower() not in ("0", "false", "off", "no")


def max_engines() -> int:
    try:
        return max(1, int(getenv_brand("ENGINE_CACHE_SIZE", str(DEFAULT_MAX_ENGINES))))
    except ValueError:
        return DEFAULT_MAX_ENGINES


def pool_settings() -> dict[str, int]:
    """Pool geometry for networked databases.

    A single transfer checks out ``1 + max_workers`` connections concurrently
    (one reader on the main thread, one per chunk writer). Several transfers
    can run in one process, so the default leaves real headroom above
    SQLAlchemy's ``pool_size=5``. ``pool_timeout`` matters more than the size:
    without it a starved pool blocks forever and the job looks hung rather than
    reporting that it ran out of connections.
    """

    def _int(name: str, default: int) -> int:
        try:
            return max(1, int(os.getenv(name, str(default))))
        except ValueError:
            return default

    return {
        "pool_size": _int("DATAFLOW_DB_POOL_SIZE", 8),
        "max_overflow": _int("DATAFLOW_DB_POOL_OVERFLOW", 12),
        "pool_timeout": _int("DATAFLOW_DB_POOL_TIMEOUT", 30),
        "pool_recycle": _int("DATAFLOW_DB_POOL_RECYCLE", 600),
    }


@dataclass
class EngineCacheStats:
    """Observable counters — proof that reuse is happening, not just claimed."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    disposals: int = 0
    live: int = 0
    #: Per-key hit counts, for the run summary. Keys are already hashed.
    by_key: dict[str, int] = field(default_factory=dict)

    @property
    def reuse_ratio(self) -> float:
        total = self.hits + self.misses
        return round(self.hits / total, 4) if total else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "disposals": self.disposals,
            "live": self.live,
            "reuse_ratio": self.reuse_ratio,
            "connections_saved": self.hits,
        }


_ENGINES: "OrderedDict[str, Any]" = OrderedDict()
_LOCK = threading.RLock()
_STATS = EngineCacheStats()
#: Identity set of engines this module owns, so `release_engine` can tell a
#: pooled engine (never dispose) from a caller-owned one (dispose normally).
_OWNED: set[int] = set()


def engine_cache_key(cfg: dict[str, Any]) -> str:
    """Stable identity for a connection config.

    Built field by field rather than from the whole dict: config dicts carry
    arbitrary operator JSON under ``extra`` which may hold lists, and iterating
    them wholesale raises ``TypeError: unhashable``. Values are coerced to
    ``str`` so equivalent configs that differ only in int-vs-str typing share
    one engine.
    """
    parts = []
    for name in _KEY_FIELDS:
        value = cfg.get(name)
        if value is None or value == "":
            parts.append("")
            continue
        if isinstance(value, bool):
            parts.append("1" if value else "0")
        elif isinstance(value, (str, int, float)):
            parts.append(str(value))
        else:
            # Never let an unhashable or huge nested value into the key; its
            # presence is recorded but its content cannot change connectivity.
            parts.append(f"<{type(value).__name__}>")
    return "\x1f".join(parts)


def redact_key(key: str) -> str:
    """Short, stable, credential-free label for logs and metrics."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


def get_pooled_engine(cfg: dict[str, Any], factory: Callable[[dict[str, Any]], Any]) -> Any:
    """Return a cached engine for ``cfg``, creating it via ``factory`` once.

    ``factory`` stays injectable so this module never imports the connector
    layer — that direction of dependency would be a cycle, and it also lets the
    tests count construction precisely.
    """
    if not cache_enabled():
        return factory(cfg)

    key = engine_cache_key(cfg)

    with _LOCK:
        existing = _ENGINES.get(key)
        if existing is not None:
            _ENGINES.move_to_end(key)
            _STATS.hits += 1
            _STATS.by_key[redact_key(key)] = _STATS.by_key.get(redact_key(key), 0) + 1
            return existing

    # Build outside the lock: connecting to a slow or unreachable host must not
    # block every other thread's cache lookup. Two threads racing on the same
    # cold key can both build; the loser's engine is disposed below rather than
    # leaked.
    engine = factory(cfg)

    with _LOCK:
        winner = _ENGINES.get(key)
        if winner is not None:
            _STATS.hits += 1
            _dispose_quietly(engine)
            _ENGINES.move_to_end(key)
            return winner
        _ENGINES[key] = engine
        _OWNED.add(id(engine))
        _STATS.misses += 1
        _STATS.live = len(_ENGINES)
        _evict_if_needed_locked()
        return engine


def release_engine(engine: Any) -> None:
    """Return an engine after use.

    A no-op for cached engines — that is the entire point. Call sites keep
    their ``finally`` block, so a future non-pooled engine is still disposed
    correctly and nothing leaks.
    """
    if engine is None:
        return
    with _LOCK:
        owned = id(engine) in _OWNED
    if owned:
        return
    _dispose_quietly(engine)


def invalidate(cfg: dict[str, Any]) -> bool:
    """Drop the engine for one config (e.g. credentials rotated)."""
    key = engine_cache_key(cfg)
    with _LOCK:
        engine = _ENGINES.pop(key, None)
        if engine is None:
            return False
        _OWNED.discard(id(engine))
        _STATS.live = len(_ENGINES)
    _dispose_quietly(engine)
    with _LOCK:
        _STATS.disposals += 1
    return True


def dispose_all() -> int:
    """Dispose every cached engine. For shutdown and for test isolation."""
    with _LOCK:
        engines = list(_ENGINES.values())
        _ENGINES.clear()
        _OWNED.clear()
        _STATS.live = 0
    for engine in engines:
        _dispose_quietly(engine)
    with _LOCK:
        _STATS.disposals += len(engines)
    return len(engines)


def reset_for_tests() -> None:
    """Dispose everything and zero the counters."""
    dispose_all()
    with _LOCK:
        _STATS.hits = 0
        _STATS.misses = 0
        _STATS.evictions = 0
        _STATS.disposals = 0
        _STATS.live = 0
        _STATS.by_key.clear()


def stats() -> EngineCacheStats:
    with _LOCK:
        return EngineCacheStats(
            hits=_STATS.hits,
            misses=_STATS.misses,
            evictions=_STATS.evictions,
            disposals=_STATS.disposals,
            live=len(_ENGINES),
            by_key=dict(_STATS.by_key),
        )


def _evict_if_needed_locked() -> None:
    """Trim to the size bound, oldest first. Caller holds the lock."""
    limit = max_engines()
    while len(_ENGINES) > limit:
        _, evicted = _ENGINES.popitem(last=False)
        _OWNED.discard(id(evicted))
        _STATS.evictions += 1
        # Dispose closes idle pooled connections and swaps in a fresh pool.
        # Connections already checked out by another thread keep working and
        # simply are not returned to the retired pool, so eviction cannot break
        # an in-flight chunk.
        _dispose_quietly(evicted)
    _STATS.live = len(_ENGINES)


def _dispose_quietly(engine: Any) -> None:
    try:
        engine.dispose()
    except Exception as exc:
        logger.debug("engine dispose failed: %s", exc, exc_info=exc)
