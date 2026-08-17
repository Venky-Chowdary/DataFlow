"""Wave 74: Snowflake structured ARRAY/OBJECT/MAP dtype fidelity SSOT.

Research anchors
----------------
- Snowflake structured types: ARRAY(T), OBJECT(name TYPE), MAP(K, V)
  vs semi-structured VARIANT / bare OBJECT (Iceberg list/struct/map twins).
- Databricks synced tables: nested → JSONB on Postgres is honest collapse.
- Bug class: ``OBJECT(... NUMBER ...)`` / ``MAP<...,INT>`` must not match
  substring ``NUMBER``/``INT`` and invent DECIMAL (catalog corruption).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_sf_structured_object_map_not_decimal_trap():
    from services.schema_introspect import _sf_to_logical
    from services.type_system import (
        ddl_type,
        is_nested_document_collapse,
        normalize_logical_type,
        parse_map_key_value,
        parse_struct_fields,
    )

    assert _sf_to_logical("OBJECT") == "JSON"
    assert _sf_to_logical("OBJECT(a VARCHAR, b NUMBER(10,2))") == (
        "STRUCT<a:TEXT, b:DECIMAL(10,2)>"
    )
    assert _sf_to_logical("OBJECT(a VARCHAR, b NUMBER(10,2))") != "DECIMAL"

    assert _sf_to_logical("MAP") == "MAP"
    assert _sf_to_logical("MAP(VARCHAR, NUMBER)") == "MAP<TEXT,DECIMAL>"
    # Snowflake INT is an alias of NUMBER(38,0), inside a MAP as much as at the
    # top level: reading it as a 64-bit integer overflows on the 19th digit.
    assert _sf_to_logical("MAP<STRING,INT>") == "MAP<TEXT,DECIMAL(38,0)>"
    assert _sf_to_logical("MAP<STRING,INT>") != "DECIMAL"

    assert parse_struct_fields("OBJECT(a VARCHAR, b NUMBER(10,2))") == [
        ("a", "VARCHAR"),
        ("b", "NUMBER(10,2)"),
    ]
    assert parse_map_key_value("MAP(VARCHAR, NUMBER(38,0))") == (
        "VARCHAR",
        "NUMBER(38,0)",
    )

    assert normalize_logical_type("STRUCT<a:TEXT, b:DECIMAL(10,2)>") == "struct"
    assert normalize_logical_type("MAP<TEXT,INTEGER>") == "map"
    assert normalize_logical_type("OBJECT(a VARCHAR)") == "struct"
    assert normalize_logical_type("MAP(VARCHAR, NUMBER)") == "map"

    assert ddl_type("snowflake", "OBJECT(a VARCHAR, b NUMBER(10,2))") == (
        "OBJECT(a VARCHAR, b NUMBER(10,2))"
    )
    assert ddl_type("snowflake", "STRUCT<a:TEXT, b:DECIMAL(10,2)>") == (
        "OBJECT(a VARCHAR, b NUMBER(10,2))"
    )
    assert ddl_type("databricks", "MAP<STRING,INT>").upper().startswith("MAP")
    assert ddl_type("postgresql", "STRUCT<a:TEXT>") == "JSONB"
    assert is_nested_document_collapse("STRUCT<a:TEXT>", "JSON") is True


def test_sf_typed_array_preserves_element():
    from services.schema_introspect import _sf_to_logical
    from services.type_system import ddl_type

    assert _sf_to_logical("ARRAY") == "ARRAY"
    assert _sf_to_logical("ARRAY(VARCHAR)") == "ARRAY<TEXT>"
    assert _sf_to_logical("ARRAY(NUMBER(10,2))") == "ARRAY<DECIMAL(10,2)>"
    assert _sf_to_logical("ARRAY<STRING>") == "ARRAY<TEXT>"

    assert ddl_type("snowflake", "ARRAY(VARCHAR)") == "ARRAY(VARCHAR)"
    assert ddl_type("snowflake", "ARRAY<TEXT>") == "ARRAY(VARCHAR)"
    assert ddl_type("databricks", "ARRAY<TEXT>") == "ARRAY<STRING>"
    # PG-family emits native T[] for typed arrays (wave 7); bare ARRAY still JSONB.
    assert ddl_type("postgresql", "ARRAY<TEXT>") == "TEXT[]"


def test_sf_nested_map_with_parametric_value():
    """Paren-depth split must not break on NUMBER(p,s) inside MAP/OBJECT."""
    from services.schema_introspect import _sf_to_logical
    from services.type_system import ddl_type, parse_map_key_value

    assert parse_map_key_value("MAP(VARCHAR, NUMBER(18,4))") == (
        "VARCHAR",
        "NUMBER(18,4)",
    )
    logical = _sf_to_logical("MAP(VARCHAR, NUMBER(18,4))")
    assert logical == "MAP<TEXT,DECIMAL(18,4)>"
    assert ddl_type("snowflake", logical) == "MAP(VARCHAR, NUMBER(18,4))"
    assert ddl_type("iceberg", logical).lower().startswith("map<")
