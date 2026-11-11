"""Parity Studio: a short-lived, per-process cache of the live reads a report makes.

A report re-reads the keys the evidence names on both sides, introspects the
narrow columns' catalog types and prepares the widen DDL (a dry run). The
Trust Report page polls while a fix is applying, and the Markdown export and
the Pilot tool ask for the same report, so every one of those reads is cached
per job, parity-state version and input for :func:`ttl_seconds` (default 30 s,
``DATAFLOW_PARITY_READ_CACHE_SECONDS``; 0 turns the cache off). A decision
bumps the version, so it is never answered from a read taken before it, and
the decision paths themselves always read fresh (``mode="refresh"``).

Modes:

* ``refresh`` — read now and remember the answer (decisions, fix re-checks);
* ``use`` — answer from a fresh-enough entry, else read and remember (the page,
  the Markdown export);
* ``only`` — answer from a fresh-enough entry, never read (the Pilot tool:
  the same basis as the page when the page was read recently, the stored
  evidence otherwise).

A read that failed is remembered too, so a dead source is not hammered by
every poll; the failure is re-raised to the caller.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable, Hashable, TypeVar

T = TypeVar("T")

MODES = frozenset({"refresh", "use", "only"})
_MAX_ENTRIES = 256
_LOCK = threading.Lock()
_ENTRIES: OrderedDict[Hashable, tuple[float, str, Any, BaseException | None]] = OrderedDict()


class NotCached(LookupError):
    """``mode="only"`` and nothing fresh is remembered for this read."""


def ttl_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(0.0, min(600.0, float(getenv_brand("PARITY_READ_CACHE_SECONDS", "30") or 30)))
    except ValueError:
        return 30.0


def clear() -> None:
    with _LOCK:
        _ENTRIES.clear()


def cached_read(key: Hashable, read: Callable[[], T], *, mode: str = "refresh") -> tuple[T, str | None]:
    """``(value, cached_at)`` — ``cached_at`` is the ISO time of a remembered read, else None."""
    if mode not in MODES:
        raise ValueError(f"unknown parity cache mode {mode!r}")
    ttl = ttl_seconds()
    if mode in {"use", "only"} and ttl > 0:
        with _LOCK:
            hit = _ENTRIES.get(key)
            if hit is not None and time.monotonic() - hit[0] <= ttl:
                _ENTRIES.move_to_end(key)
                _stamp, at, value, error = hit
                if error is not None:
                    raise error
                return value, at
    if mode == "only":
        raise NotCached("no recent live read of this report")
    at = datetime.now(timezone.utc).isoformat()
    try:
        value = read()
    except Exception as exc:
        _remember(key, at, None, exc, ttl)
        raise
    _remember(key, at, value, None, ttl)
    return value, None


def _remember(key: Hashable, at: str, value: Any, error: BaseException | None, ttl: float) -> None:
    if ttl <= 0:
        return
    with _LOCK:
        _ENTRIES[key] = (time.monotonic(), at, value, error)
        _ENTRIES.move_to_end(key)
        while len(_ENTRIES) > _MAX_ENTRIES:
            _ENTRIES.popitem(last=False)
