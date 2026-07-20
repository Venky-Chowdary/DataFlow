"""Durable embedding cache — SQLite persistence across process restarts.

Honesty
-------
Caches real model outputs keyed by ``sha256(model:text)``. Does not invent
vectors. Process memory remains an L1 hot cache; SQLite is the durable L2.
Operators can disable durable writes per transfer or clear the store from Studio.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

_LOCK = threading.RLock()
_CONN: sqlite3.Connection | None = None
_DB_PATH: Path | None = None

# Session counters (process lifetime).
_SESSION_HITS = 0
_SESSION_MISSES = 0
_SESSION_WRITES = 0


def _resolve_db_path() -> Path:
    override = os.getenv("DATAFLOW_EMBEDDING_CACHE_PATH", "").strip()
    if override:
        path = Path(override)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    return data_dir() / "embedding_cache.sqlite3"


def durable_cache_enabled_by_default() -> bool:
    raw = os.getenv("DATAFLOW_EMBEDDING_DURABLE_CACHE", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _connect() -> sqlite3.Connection:
    global _CONN, _DB_PATH
    with _LOCK:
        path = _resolve_db_path()
        if _CONN is not None and _DB_PATH == path:
            return _CONN
        if _CONN is not None:
            try:
                _CONN.close()
            except Exception:
                pass
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                cache_key TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                dimension INTEGER NOT NULL,
                vector_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_hit_at REAL NOT NULL,
                hit_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_embeddings_model ON embeddings(model)"
        )
        conn.commit()
        _CONN = conn
        _DB_PATH = path
        return conn


def reset_connection_for_tests() -> None:
    """Close cached connection so tests can point at a temp DB path."""
    global _CONN, _DB_PATH, _SESSION_HITS, _SESSION_MISSES, _SESSION_WRITES
    with _LOCK:
        if _CONN is not None:
            try:
                _CONN.close()
            except Exception:
                pass
        _CONN = None
        _DB_PATH = None
        _SESSION_HITS = 0
        _SESSION_MISSES = 0
        _SESSION_WRITES = 0


@dataclass
class CacheStats:
    path: str
    entries: int
    models: int
    approx_bytes: int
    session_hits: int
    session_misses: int
    session_writes: int
    durable_default: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "entries": self.entries,
            "models": self.models,
            "approx_bytes": self.approx_bytes,
            "session_hits": self.session_hits,
            "session_misses": self.session_misses,
            "session_writes": self.session_writes,
            "durable_default": self.durable_default,
            "hit_rate": (
                round(self.session_hits / (self.session_hits + self.session_misses), 4)
                if (self.session_hits + self.session_misses) > 0
                else None
            ),
        }


def cache_stats() -> CacheStats:
    global _SESSION_HITS, _SESSION_MISSES, _SESSION_WRITES
    path = _resolve_db_path()
    entries = 0
    models = 0
    approx_bytes = path.stat().st_size if path.exists() else 0
    try:
        conn = _connect()
        with _LOCK:
            row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT model) FROM embeddings").fetchone()
            entries = int(row[0] or 0)
            models = int(row[1] or 0)
    except Exception:
        pass
    return CacheStats(
        path=str(path),
        entries=entries,
        models=models,
        approx_bytes=approx_bytes,
        session_hits=_SESSION_HITS,
        session_misses=_SESSION_MISSES,
        session_writes=_SESSION_WRITES,
        durable_default=durable_cache_enabled_by_default(),
    )


def get_cached(keys: list[str]) -> dict[str, list[float]]:
    """Return cached vectors keyed by cache_key (missing keys omitted)."""
    global _SESSION_HITS, _SESSION_MISSES
    if not keys:
        return {}
    conn = _connect()
    out: dict[str, list[float]] = {}
    now = time.time()
    with _LOCK:
        for key in keys:
            row = conn.execute(
                "SELECT vector_json FROM embeddings WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if not row:
                _SESSION_MISSES += 1
                continue
            try:
                vector = [float(x) for x in json.loads(row[0])]
            except Exception:
                _SESSION_MISSES += 1
                continue
            conn.execute(
                "UPDATE embeddings SET last_hit_at = ?, hit_count = hit_count + 1 WHERE cache_key = ?",
                (now, key),
            )
            out[key] = vector
            _SESSION_HITS += 1
        conn.commit()
    return out


def put_cached(
    items: list[tuple[str, str, list[float]]],
) -> int:
    """Persist ``(cache_key, model, vector)`` rows. Returns write count."""
    global _SESSION_WRITES
    if not items:
        return 0
    conn = _connect()
    now = time.time()
    written = 0
    with _LOCK:
        for key, model, vector in items:
            if not vector:
                continue
            conn.execute(
                """
                INSERT INTO embeddings (cache_key, model, dimension, vector_json, created_at, last_hit_at, hit_count)
                VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(cache_key) DO UPDATE SET
                    vector_json = excluded.vector_json,
                    dimension = excluded.dimension,
                    model = excluded.model,
                    last_hit_at = excluded.last_hit_at
                """,
                (
                    key,
                    model or "default",
                    len(vector),
                    json.dumps(vector),
                    now,
                    now,
                ),
            )
            written += 1
            _SESSION_WRITES += 1
        conn.commit()
    return written


def clear_cache(*, model: str | None = None) -> dict[str, Any]:
    """Delete all entries, or only those for ``model``."""
    conn = _connect()
    with _LOCK:
        if model:
            cur = conn.execute("DELETE FROM embeddings WHERE model = ?", (model,))
        else:
            cur = conn.execute("DELETE FROM embeddings")
        deleted = int(cur.rowcount or 0)
        conn.commit()
        # Reclaim space opportunistically.
        try:
            conn.execute("VACUUM")
        except Exception:
            pass
    return {"deleted": deleted, "model": model or "*"}
