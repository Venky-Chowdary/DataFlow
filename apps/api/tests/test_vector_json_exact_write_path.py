"""VECTOR / embedding JSON arrays use json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 to IEEE, then the native
float binder accepted the invented component. Long fractions now stay
Decimal and refuse. IEEE-exact 1.5 still binds. Cache rows miss.
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

from services.schema_inference import infer_type  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402
from services.vector_embedding import coerce_embedding  # noqa: E402

LONG = "1.234567890123456789"


def test_coerce_embedding_json_long_fraction_refuses():
    vals, err = coerce_embedding(f"[{LONG}, 1.5]")
    assert vals is None and err and "refuse invent" in err
    vals, err = coerce_embedding("[1.5, 2.0]")
    assert err is None
    assert vals == [1.5, 2.0]


def test_apply_transform_vector_long_fraction_refuses():
    parsed, err = apply_transform(f"[{LONG}, 1.5]", "vector")
    assert parsed is None and err and "Invalid vector" in err
    parsed, err = apply_transform("[1.5, 2.0]", "vector")
    assert err is None
    assert parsed == [1.5, 2.0]


def test_infer_type_does_not_invent_vector_from_long_fraction():
    long_arr = "[" + ",".join([LONG] * 8) + "]"
    ieee_arr = "[" + ",".join(["1.5"] * 8) + "]"
    assert infer_type([long_arr, long_arr]) != "VECTOR(8)"
    assert infer_type([ieee_arr, ieee_arr]) == "VECTOR(8)"


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


def test_cache_json_number_long_fraction_misses(isolated_embedding_cache: Path):
    from services.embedding_cache import get_cached, put_cached

    put_cached([("long", "test-model", [0.0, 0.0])])
    conn = sqlite3.connect(str(isolated_embedding_cache))
    conn.execute(
        "UPDATE embeddings SET vector_json = ?, last_hit_at = ? WHERE cache_key = ?",
        (f"[{LONG}, 1.5]", time.time(), "long"),
    )
    conn.commit()
    conn.close()
    assert get_cached(["long"]) == {}
