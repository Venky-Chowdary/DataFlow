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
    pyarrow = pytest.importorskip("pyarrow")
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
    assert normalize_logical_type("STRUCT<lat:FLOAT, lon:FLOAT>") == "json"

    assert ddl_type("databricks", "ARRAY<INTEGER>") == "ARRAY<BIGINT>"
    assert "STRUCT<" in ddl_type("databricks", "STRUCT<lat:FLOAT, lon:FLOAT>")
    assert ddl_type("duckdb", "ARRAY<TEXT>") == "VARCHAR[]"
    assert ddl_type("clickhouse", "ARRAY<INTEGER>") == "Array(Int64)"
    # Bare array on lakehouse must not collapse to STRING.
    assert ddl_type("databricks", "array") == "ARRAY<STRING>"
    assert ddl_type("clickhouse", "array") == "Array(String)"
    # PG still uses JSONB for bare arrays (no invent STRUCT).
    assert ddl_type("postgresql", "ARRAY<INTEGER>") in {"JSONB", "JSON"}


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
