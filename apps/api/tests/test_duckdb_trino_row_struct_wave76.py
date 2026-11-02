"""Wave 76: DuckDB STRUCT(...) / Trino ROW(...) / ClickHouse Nested+Enum SSOT.

Research anchors
----------------
- DuckDB STRUCT(a T, b U) requires fixed named keys (vectorized nested).
- Trino/Presto ROW(a T) ↔ STRUCT (ROW→Tuple on ClickHouse migration guides).
- ClickHouse Nested → parallel arrays; cross-engine ARRAY<STRUCT<…>>.
- ClickHouse Enum8/16 → closed ENUM domain (not opaque String).
- Spark/Databricks VOID must not silently invent STRING.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_duckdb_struct_paren_form():
    from services.type_system import (
        ddl_type,
        is_nested_document_collapse,
        normalize_logical_type,
        parse_struct_fields,
    )

    assert parse_struct_fields("STRUCT(a INTEGER, b VARCHAR)") == [
        ("a", "INTEGER"),
        ("b", "VARCHAR"),
    ]
    assert normalize_logical_type("STRUCT(a INTEGER, b VARCHAR)") == "struct"
    # Nested leaves resolve through the same map as top-level columns: bare
    # "INTEGER" names no width, so it widens (never narrows) exactly as a
    # top-level INTEGER column does, while a declared int32 keeps 32 bits.
    assert ddl_type("duckdb", "STRUCT(a INTEGER, b VARCHAR)") == (
        "STRUCT(a BIGINT, b VARCHAR)"
    )
    assert ddl_type("duckdb", "STRUCT(a int32, b VARCHAR)") == (
        "STRUCT(a INTEGER, b VARCHAR)"
    )
    assert ddl_type("trino", "STRUCT(a INTEGER, b VARCHAR)") == (
        "row(a bigint, b varchar)"
    )
    assert ddl_type("postgresql", "STRUCT(a INTEGER, b VARCHAR)") == "JSONB"
    assert is_nested_document_collapse("STRUCT(a INTEGER, b VARCHAR)", "JSON") is True


def test_trino_row_form():
    from services.type_system import (
        ddl_type,
        normalize_logical_type,
        parse_struct_fields,
    )

    assert parse_struct_fields("row(a integer, b varchar)") == [
        ("a", "integer"),
        ("b", "varchar"),
    ]
    assert normalize_logical_type("row(a integer, b varchar)") == "struct"
    # Bare nested ``integer`` invents never-narrower bigint (audit §2.1).
    assert ddl_type("trino", "row(a integer, b varchar)") == (
        "row(a bigint, b varchar)"
    )
    assert ddl_type("duckdb", "ROW(a INTEGER, b VARCHAR)") == (
        "STRUCT(a BIGINT, b VARCHAR)"
    )
    assert ddl_type("clickhouse", "ROW(a INTEGER, b VARCHAR)").startswith("Tuple(")
    assert ddl_type("snowflake", "ROW(a INTEGER, b VARCHAR)").startswith("OBJECT(")
    # Must not invent bare varchar/string from ROW.
    assert ddl_type("trino", "row(a integer)") != "varchar"


def test_ch_nested_and_enum():
    from services.schema_introspect import _ch_to_logical
    from services.type_system import (
        ddl_type,
        enum_domain_would_collapse,
        normalize_logical_type,
    )

    assert _ch_to_logical("Nested(x String, y Int64)") == (
        "ARRAY<STRUCT<x:TEXT, y:Int64>>"
    )
    assert normalize_logical_type("Nested(x String, y Int64)") == "array"
    assert ddl_type("clickhouse", "Nested(x String, y Int64)") == (
        "Nested(x String, y Int64)"
    )
    assert ddl_type("databricks", "Nested(x String, y Int64)").upper().startswith(
        "ARRAY"
    )
    assert ddl_type("postgresql", "Nested(x String, y Int64)") == "JSONB"

    enum_logical = _ch_to_logical("Enum8('a' = 1, 'b' = 2)")
    assert enum_logical.upper().startswith("ENUM(")
    assert "'a'" in enum_logical and "'b'" in enum_logical
    assert enum_domain_would_collapse(enum_logical, "TEXT") is True
    assert enum_domain_would_collapse(enum_logical, enum_logical) is False


def test_databricks_void_not_string_invent():
    from services.type_system import ddl_type, specialty_carrier_would_collapse

    assert ddl_type("databricks", "VOID") == "VOID"
    assert specialty_carrier_would_collapse("VOID", "STRING") is True
    assert specialty_carrier_would_collapse("VOID", "TEXT") is True
