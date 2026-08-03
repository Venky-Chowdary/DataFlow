"""Wave H accuracy: Parquet Arrow schema, wide NUMERIC via generic_sql, nested DDL, Iceberg fail-closed, object-store union."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_generic_sql_wide_numeric_stays_decimal():
    import sqlalchemy as sa

    from connectors.generic_sql import _logical_type_from_sa

    assert _logical_type_from_sa(sa.Numeric(18, 0)) == "integer"
    assert _logical_type_from_sa(sa.Numeric(38, 0)) == "DECIMAL(38,0)"
    assert _logical_type_from_sa(sa.Numeric(12, 2)) == "DECIMAL(12,2)"


def test_arrow_schema_preserves_decimal_tz_nested():
    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from services.arrow_schema import columns_from_arrow_schema, schema_from_arrow

    schema = pa.schema([
        ("amt", pa.decimal128(12, 2)),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("wall", pa.timestamp("us")),
        ("tags", pa.list_(pa.string())),
        ("loc", pa.struct([("lat", pa.float64()), ("lon", pa.float64())])),
    ])
    mapped = schema_from_arrow(schema)
    assert mapped["amt"] == "DECIMAL(12,2)"
    assert mapped["ts"] == "TIMESTAMPTZ"
    assert mapped["wall"] == "TIMESTAMP_NTZ"
    assert mapped["tags"].startswith("ARRAY<")
    assert mapped["loc"].startswith("STRUCT<")
    cols = columns_from_arrow_schema(schema)
    assert cols[0]["source"] == "arrow_schema"
    assert cols[0]["inferred_type"] == "DECIMAL(12,2)"


def test_nested_ddl_databricks_duckdb_clickhouse():
    from services.type_system import ddl_type, normalize_logical_type

    assert normalize_logical_type("ARRAY<INTEGER>") == "array"
    assert normalize_logical_type("STRUCT<lat:FLOAT, lon:FLOAT>") == "struct"
    assert normalize_logical_type("MAP<STRING, INTEGER>") == "map"

    assert ddl_type("databricks", "ARRAY<INTEGER>") == "ARRAY<BIGINT>"
    assert "STRUCT<" in ddl_type("databricks", "STRUCT<lat:FLOAT, lon:FLOAT>")
    assert ddl_type("duckdb", "ARRAY<TEXT>") == "VARCHAR[]"
    assert ddl_type("clickhouse", "ARRAY<INTEGER>") == "Array(Int64)"
    # Bare array on lakehouse must not collapse to STRING.
    assert ddl_type("databricks", "array") == "ARRAY<STRING>"
    assert ddl_type("clickhouse", "array") == "Array(String)"
    # PG typed arrays → native T[]; STRUCT still JSONB (no invent STRUCT DDL).
    assert ddl_type("postgresql", "ARRAY<INTEGER>") == "BIGINT[]"
    assert ddl_type("postgresql", "STRUCT<lat:FLOAT, lon:FLOAT>") in {"JSONB", "JSON"}
    # Snowflake structured OBJECT/ARRAY — not opaque VARIANT when fields declared.
    sf = ddl_type("snowflake", "STRUCT<lat:FLOAT, lon:FLOAT>")
    assert sf.startswith("OBJECT("), sf
    assert "ARRAY(" in ddl_type("snowflake", "ARRAY<INTEGER>")


def test_nested_document_collapse_helpers():
    from services.type_system import (
        decimal_params_would_narrow,
        is_nested_document_collapse,
        is_nested_shape_collapse,
        is_lossy_coercion,
        is_precision_collapse_coercion,
    )

    assert is_nested_document_collapse("STRUCT<a:INT>", "JSONB") is True
    assert is_nested_document_collapse("STRUCT<a:INT>", "STRUCT<a:INT>") is False
    assert is_nested_shape_collapse(
        "STRUCT<a:INT, b:TEXT>", "STRUCT<a:INT>"
    ) is True  # missing b
    assert is_lossy_coercion("STRUCT<a:INT>", "VARIANT") is True
    assert is_lossy_coercion("STRUCT<a:INT>", "STRUCT<a:INT>") is False
    assert is_nested_shape_collapse("ARRAY<FLOAT>", "ARRAY<INTEGER>") is True
    assert is_nested_shape_collapse("ARRAY<INTEGER>", "ARRAY<DECIMAL>") is False
    assert is_lossy_coercion("ARRAY<FLOAT>", "ARRAY<INTEGER>") is True
    assert decimal_params_would_narrow("DECIMAL(38,10)", "DECIMAL(12,2)") is True
    assert is_precision_collapse_coercion("DECIMAL(38,10)", "DECIMAL(12,2)") is True
    assert decimal_params_would_narrow("DECIMAL(12,2)", "DECIMAL(38,10)") is False


def test_varchar_and_unsigned_fidelity_helpers():
    from services.type_system import (
        binary_width_would_narrow,
        enum_set_domain_would_reject,
        is_precision_collapse_coercion,
        string_width_would_narrow,
        unsigned_integer_would_overflow,
    )

    assert string_width_would_narrow("VARCHAR(255)", "VARCHAR(50)") is True
    assert string_width_would_narrow("TEXT", "VARCHAR(10)") is True
    assert string_width_would_narrow("VARCHAR(50)", "VARCHAR(255)") is False
    assert string_width_would_narrow("VARCHAR(50)", "TEXT") is False
    assert is_precision_collapse_coercion("VARCHAR(255)", "VARCHAR(50)") is True

    assert unsigned_integer_would_overflow("INT UNSIGNED", "INTEGER") is True
    assert unsigned_integer_would_overflow("INT UNSIGNED", "BIGINT") is False
    assert unsigned_integer_would_overflow("BIGINT UNSIGNED", "BIGINT") is True
    assert unsigned_integer_would_overflow("INT UNSIGNED", "DECIMAL(20,0)") is False
    assert is_precision_collapse_coercion("INT UNSIGNED", "INTEGER") is True

    assert binary_width_would_narrow("VARBINARY(64)", "VARBINARY(16)") is True
    assert binary_width_would_narrow("VARBINARY(16)", "VARBINARY(64)") is False
    assert binary_width_would_narrow("BYTEA", "VARBINARY(16)") is True
    assert is_precision_collapse_coercion("VARBINARY(64)", "VARBINARY(16)") is True

    assert enum_set_domain_would_reject(
        "ENUM('a','b','c')", "ENUM('a','b')"
    ) is True
    assert enum_set_domain_would_reject(
        "ENUM('a','b')", "ENUM('a','b','c')"
    ) is False
    assert is_precision_collapse_coercion(
        "ENUM('a','b','c')", "ENUM('a','b')"
    ) is True

def test_iceberg_load_fail_closed_on_missing_file(tmp_path: Path):
    from connectors.iceberg_writer import _load_existing_rows

    meta = {"data-files": [{"path": "data/missing.parquet"}]}
    with pytest.raises(ValueError, match="missing"):
        _load_existing_rows(tmp_path, ["id"], meta)


def test_iceberg_load_fail_closed_on_corrupt_jsonl(tmp_path: Path):
    from connectors.iceberg_writer import _load_existing_rows

    data = tmp_path / "data"
    data.mkdir()
    bad = data / "part.jsonl"
    bad.write_text("{not-json\n", encoding="utf-8")
    meta = {"data-files": [{"path": "data/part.jsonl"}]}
    with pytest.raises(ValueError, match="corrupt"):
        _load_existing_rows(tmp_path, ["id"], meta)


def test_iceberg_load_reads_valid_jsonl(tmp_path: Path):
    from connectors.iceberg_writer import _load_existing_rows

    data = tmp_path / "data"
    data.mkdir()
    good = data / "part.jsonl"
    good.write_text(json.dumps({"id": 1, "n": "a"}) + "\n", encoding="utf-8")
    meta = {"data-files": [{"path": "data/part.jsonl"}]}
    rows = _load_existing_rows(tmp_path, ["id", "n"], meta)
    assert rows == [{"id": 1, "n": "a"}]


def test_object_store_schema_union_widens_and_adds_columns():
    from services.object_store_introspect import merge_object_schemas, _sample_prefix_keys

    a = {
        "ok": True,
        "columns": ["id", "amount"],
        "schema": {"id": "INTEGER", "amount": "INTEGER"},
        "row_estimate": 10,
        "quality_score": 0.9,
    }
    b = {
        "ok": True,
        "columns": ["id", "amount", "note"],
        "schema": {"id": "INTEGER", "amount": "DECIMAL(12,2)", "note": "TEXT"},
        "row_estimate": 5,
        "quality_score": 0.8,
    }
    merged = merge_object_schemas([a, b])
    assert merged["ok"] is True
    assert "note" in merged["columns"]
    assert merged["schema"]["amount"] == "DECIMAL(12,2)"
    assert merged["objects_sampled"] == 2

    keys = [f"part-{i}.json" for i in range(20)]
    sampled = _sample_prefix_keys(keys, max_objects=5)
    assert len(sampled) <= 5
    assert sampled[0] == keys[0]
    assert sampled[-1] == keys[-1]
