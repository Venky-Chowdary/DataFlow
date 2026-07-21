"""Durable embedding cache — real SQLite persistence (no mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


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


def test_put_get_survives_reconnect(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached, put_cached, reset_connection_for_tests

    key = "abc123"
    vector = [0.1, 0.2, 0.3, 0.4]
    assert put_cached([(key, "test-model", vector)]) == 1
    assert get_cached([key])[key] == vector

    # Simulate process restart: drop connection, keep same DB file.
    reset_connection_for_tests()
    hit = get_cached([key])
    assert key in hit
    assert hit[key] == vector
    assert isolated_embedding_cache.exists()


def test_clear_cache_by_model(isolated_embedding_cache: Path):
    from services.embedding_cache import clear_cache, get_cached, put_cached

    put_cached([
        ("k1", "model-a", [1.0, 2.0]),
        ("k2", "model-b", [3.0, 4.0]),
    ])
    result = clear_cache(model="model-a")
    assert result["deleted"] == 1
    assert get_cached(["k1"]) == {}
    assert get_cached(["k2"])["k2"] == [3.0, 4.0]


def test_embed_uses_durable_layer(isolated_embedding_cache: Path):
    """Second embed after memory clear must hit SQLite, not recompute from scratch."""
    from services.embedding_cache import cache_stats, reset_connection_for_tests
    from services.vectorization import _get_embedder, clear_memory_cache, embed

    _get_embedder.cache_clear()
    texts = ["DataFlow durable embedding cache proof"]
    model = "hash/16"
    first = embed(texts, model=model, durable=True)
    assert len(first) == 1
    assert len(first[0]) == 16

    clear_memory_cache()
    reset_connection_for_tests()

    before = cache_stats()
    second = embed(texts, model=model, durable=True)
    after = cache_stats()

    assert second == first
    assert after.session_hits >= before.session_hits + 1
    assert after.entries >= 1


def test_embed_respects_durable_off(isolated_embedding_cache: Path):
    from services.embedding_cache import cache_stats, get_cached
    from services.vectorization import _cache_key, _get_embedder, clear_memory_cache, embed

    _get_embedder.cache_clear()
    texts = ["opt-out durable write"]
    model = "hash/16"
    embed(texts, model=model, durable=False)
    clear_memory_cache()
    key = _cache_key(texts[0], model)
    assert get_cached([key]) == {}
    assert cache_stats().entries == 0


def test_adapters_forward_durable_flag():
    from src.transfer.adapters import _apply_vector_extra
    from src.transfer.models import EndpointConfig

    common: dict = {}
    endpoint = EndpointConfig(
        kind="database",
        format="pgvector",
        extra={"durable_embedding_cache": False, "content_column": "body"},
    )
    _apply_vector_extra(common, endpoint)
    assert common["durable_embedding_cache"] is False
    assert common["content_column"] == "body"


def test_capabilities_include_embedding_cache(isolated_embedding_cache: Path):
    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    assert "embedding_cache" in caps
    assert "entries" in caps["embedding_cache"]
    assert "durable_default" in caps["embedding_cache"]
