"""Wave F accuracy: wide DECIMAL(p,0), Iceberg schema lock, Dynamo/Mongo/Redis identity."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_wide_zero_scale_decimal_not_collapsed_to_integer():
    from services.type_system import normalize_logical_type, zero_scale_numeric_carrier

    assert zero_scale_numeric_carrier(18) == "INTEGER"
    assert zero_scale_numeric_carrier(19) == "DECIMAL(19,0)"
    assert zero_scale_numeric_carrier(38) == "DECIMAL(38,0)"
    assert normalize_logical_type("DECIMAL(18,0)") == "integer"
    assert normalize_logical_type("DECIMAL(38,0)") == "decimal"
    assert normalize_logical_type("NUMBER(38,0)") == "decimal"
    assert normalize_logical_type("NUMERIC(10,0)") == "integer"


def test_sqlserver_oracle_snowflake_introspect_preserve_wide_zero_scale():
    from services.schema_introspect import (
        _oracle_to_logical,
        _sf_to_logical,
        _sqlserver_to_logical,
    )

    assert _sqlserver_to_logical("numeric(38,0)") == "DECIMAL(38,0)"
    assert _sqlserver_to_logical("decimal(12,0)") == "INTEGER"
    assert _oracle_to_logical("NUMBER(38,0)") == "DECIMAL(38,0)"
    assert _oracle_to_logical("NUMBER(10,0)") == "INTEGER"
    assert _sf_to_logical("NUMBER(38,0)") == "DECIMAL(38,0)"


def test_iceberg_write_types_honor_locked_schema():
    from connectors.iceberg_writer import _write_types_from_schema

    schema = {
        "fields": [
            {"id": 1, "name": "amt", "type": {"type": "decimal", "precision": 12, "scale": 2}},
            {"id": 2, "name": "ts", "type": "timestamptz"},
        ]
    }
    dest = {"amt": "float", "ts": "string"}
    out = _write_types_from_schema(schema, dest)
    assert out["amt"] == "DECIMAL(12,2)"
    assert out["ts"] == "timestamptz"


def test_dynamodb_create_requires_identity():
    from connectors.dynamodb_writer import _resolve_key_schema
    import pytest

    with pytest.raises(ValueError, match="conflict_columns"):
        _resolve_key_schema(
            ["note", "payload"],
            [{"source": "note", "target": "note"}],
            conflict_columns=None,
            source_types=["TEXT", "TEXT"],
        )

    keys = _resolve_key_schema(
        ["order_id", "sku", "qty"],
        [],
        conflict_columns=["order_id", "sku"],
        source_types=["TEXT", "TEXT", "INTEGER"],
    )
    assert keys[0][0] == "order_id" and keys[0][1] == "HASH"
    assert keys[1][0] == "sku" and keys[1][1] == "RANGE"


def test_mongo_missing_vs_null_projection():
    from connectors.mongodb_reader import _project_doc_row
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    row = _project_doc_row({"a": 1, "b": None}, ["a", "b", "c"])
    assert row[0] == "1" or row[0] == "1.0" or row[0] == "1"
    assert row[1] == SQL_NULL_SENTINEL
    assert row[2] == DF_MISSING_SENTINEL


def test_redis_binary_decode_envelope():
    from connectors.redis_reader import _decode
    import json

    text = _decode(b"hello")
    assert text == "hello"
    raw = bytes([0xFF, 0x00, 0xFE])
    env = json.loads(_decode(raw))
    assert env["_df_redis_binary"] is True
    assert env["encoding"] == "base64"


def test_redis_conflict_key_resolution():
    from connectors.redis_writer import _resolve_redis_key_id

    key, col = _resolve_redis_key_id(
        {"order_id": "o1", "sku": "s1"},
        ["order_id", "sku"],
        conflict_columns=["order_id", "sku"],
        row_index=0,
    )
    assert key == "o1|s1"
    assert col == "order_id"
    missing, _ = _resolve_redis_key_id(
        {"order_id": "o1", "sku": ""},
        ["order_id", "sku"],
        conflict_columns=["order_id", "sku"],
        row_index=0,
    )
    assert missing is None
