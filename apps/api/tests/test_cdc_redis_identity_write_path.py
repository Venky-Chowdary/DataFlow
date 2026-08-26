"""CDC and Redis identity use present_cell_text, not str(value).

DuckDB ``__df_ddb_null__`` used to pass as a present CDC key. True became
``True`` so dest ``true`` missed the Redis key and Gate-8 rebuild.
Missing / SQL NULL stay absent. 0 stays present.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.redis_reader import redis_key_for  # noqa: E402
from connectors.redis_writer import (  # noqa: E402
    _redis_row_to_doc,
    _resolve_redis_key_id,
)
from services.cdc_identity import is_present_cdc_row_key  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_cdc_row_key_reader_null_is_absent():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "   ", Missing, DF_MISSING_SENTINEL):
        assert is_present_cdc_row_key(wire) is False, wire
    assert is_present_cdc_row_key(0) is True
    assert is_present_cdc_row_key(False) is True
    assert is_present_cdc_row_key(True) is True
    assert is_present_cdc_row_key("true") is True


def test_redis_key_true_shares_dest_true():
    assert redis_key_for("jobs", True) == redis_key_for("jobs", "true")
    assert redis_key_for("jobs", True) != redis_key_for("jobs", "True")
    assert redis_key_for("jobs", 0) == redis_key_for("jobs", "0")


def test_redis_resolve_true_is_dest_true():
    key, col = _resolve_redis_key_id(
        {"id": True, "note": "kept"},
        ["id", "note"],
        conflict_columns=["id"],
        row_index=0,
    )
    assert col == "id"
    assert key == "true"
    missing, _ = _resolve_redis_key_id(
        {"id": SQL_NULL_SENTINEL, "note": "kept"},
        ["id", "note"],
        conflict_columns=["id"],
        row_index=0,
    )
    assert missing is None
    ddb_missing, _ = _resolve_redis_key_id(
        {"id": "__df_ddb_null__", "note": "kept"},
        ["id", "note"],
        conflict_columns=["id"],
        row_index=0,
    )
    assert ddb_missing is None


def test_redis_doc_ddb_null_is_json_null():
    doc = _redis_row_to_doc(
        ["id", "amt", "note"],
        ("1", "__df_ddb_null__", Missing),
    )
    assert doc == {"id": "1", "amt": None}
    assert "note" not in doc
