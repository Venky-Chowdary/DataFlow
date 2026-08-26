"""Embedding cache read uses coerce_embedding, not float(x).

A corrupt Auto 1.234 / 2**53+1 cache row must miss (re-embed), not invent.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def isolated_embedding_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db = tmp_path / "embedding_cache.sqlite3"
    monkeypatch.setenv("DATAFLOW_EMBEDDING_CACHE_PATH", str(db))
    monkeypatch.setenv("DATAFLOW_EMBEDDING_DURABLE_CACHE", "true")
    from services.embedding_cache import reset_connection_for_tests
    from services.vectorization import clear_memory_cache

    reset_connection_for_tests()
    clear_memory_cache()
    yield db
    reset_connection_for_tests()
    clear_memory_cache()


def _insert_raw(db: Path, key: str, vector_json: str) -> None:
    from services.embedding_cache import put_cached

    # Create schema via the cache module, then overwrite the JSON payload.
    put_cached([(key, "test-model", [0.0, 0.0])])
    conn = sqlite3.connect(str(db))
    now = time.time()
    conn.execute(
        "UPDATE embeddings SET vector_json = ?, last_hit_at = ? WHERE cache_key = ?",
        (vector_json, now, key),
    )
    conn.commit()
    conn.close()


def test_plain_ieee_cache_still_hits(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached, put_cached

    assert put_cached([("ok", "test-model", [0.1, 0.2])]) == 1
    assert get_cached(["ok"])["ok"] == [0.1, 0.2]


def test_locale_money_cache_hits(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached

    _insert_raw(isolated_embedding_cache, "money", '["$1.50", 2]')
    assert get_cached(["money"])["money"] == [1.5, 2.0]


def test_auto_grouping_cache_misses(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached

    _insert_raw(isolated_embedding_cache, "auto", '["1.234", 2]')
    assert get_cached(["auto"]) == {}


def test_ieee_lossy_mantissa_cache_misses(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached

    _insert_raw(isolated_embedding_cache, "ieee", "[9007199254740993, 1]")
    assert get_cached(["ieee"]) == {}


def test_put_cached_refuses_auto_and_lossy_mantissa(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached, put_cached

    assert put_cached([("auto", "test-model", ["1.234", 2])]) == 0
    assert put_cached([("ieee", "test-model", [9007199254740993, 1])]) == 0
    assert get_cached(["auto", "ieee"]) == {}
    assert put_cached([("ok", "test-model", [0.1, 0.2])]) == 1
    assert get_cached(["ok"])["ok"] == [0.1, 0.2]
