"""Wave 73: BigQuery ARRAY/STRUCT/RANGE/BIGNUMERIC fidelity SSOT.

Research anchors
----------------
- Google SQL data types: ARRAY, STRUCT, RANGE<DATE|DATETIME|TIMESTAMP>,
  NUMERIC (38,9) vs BIGNUMERIC (76,38).
- PeerDB / Airbyte: nested → JSON is valid but must be labeled collapse;
  typed ARRAY/STRUCT must not silently become TEXT on introspect.
- PostgreSQL twins: DATERANGE / TSRANGE / TSTZRANGE (no Snowflake RANGE).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_bq_array_struct_dtype_strings_not_text():
    from services.schema_introspect import _bq_to_logical
    from services.type_system import (
        ddl_type,
        is_nested_document_collapse,
        normalize_logical_type,
    )

    assert _bq_to_logical("ARRAY") == "ARRAY"
    assert _bq_to_logical("ARRAY<STRING>") == "ARRAY<TEXT>"
    assert _bq_to_logical("ARRAY<INT64>") == "ARRAY<INTEGER>"
    assert _bq_to_logical("ARRAY<STRUCT<a:INT64>>") == "ARRAY<STRUCT<a:INTEGER>>"

    assert _bq_to_logical("STRUCT") == "STRUCT"
    assert _bq_to_logical("RECORD") == "STRUCT"
    assert _bq_to_logical("STRUCT<a:INT64, b:STRING>") == "STRUCT<a:INTEGER, b:TEXT>"
    assert _bq_to_logical("STRUCT<a INT64, b STRING>") == "STRUCT<a:INTEGER, b:TEXT>"

    assert normalize_logical_type("ARRAY<INTEGER>") == "array"
    assert normalize_logical_type("STRUCT<a:INTEGER>") == "struct"

    # Lakehouse engines keep nested DDL; PG uses document path (honest collapse).
    assert ddl_type("bigquery", "ARRAY<STRING>") == "ARRAY<STRING>"
    assert ddl_type("snowflake", "ARRAY<STRING>").upper().startswith("ARRAY")
    assert ddl_type("postgresql", "ARRAY<STRING>") == "JSONB"
    assert ddl_type("postgresql", "STRUCT<a:INTEGER>") == "JSONB"
    assert is_nested_document_collapse("STRUCT<a:INTEGER>", "JSON") is True


def test_bq_range_not_timestamptz_substring_trap():
    """``RANGE<TIMESTAMP>`` must never become TIMESTAMPTZ via substring match."""
    from services.schema_introspect import _bq_to_logical
    from services.type_system import (
        ddl_type,
        specialty_carrier_would_collapse,
    )

    assert _bq_to_logical("RANGE<DATE>") == "DATERANGE"
    assert _bq_to_logical("RANGE<DATETIME>") == "TSRANGE"
    assert _bq_to_logical("RANGE<TIMESTAMP>") == "TSTZRANGE"
    assert _bq_to_logical("RANGE") == "RANGE"
    # Must not equal TIMESTAMPTZ (the historical bug).
    assert _bq_to_logical("RANGE<TIMESTAMP>") != "TIMESTAMPTZ"

    assert ddl_type("postgresql", "RANGE<DATE>") == "DATERANGE"
    assert ddl_type("postgresql", "DATERANGE") == "DATERANGE"
    assert ddl_type("postgresql", "TSRANGE") == "TSRANGE"
    assert ddl_type("postgresql", "TSTZRANGE") == "TSTZRANGE"
    assert ddl_type("bigquery", "DATERANGE") == "RANGE<DATE>"
    assert ddl_type("bigquery", "TSTZRANGE") == "RANGE<TIMESTAMP>"
    assert ddl_type("bigquery", "RANGE<DATETIME>") == "RANGE<DATETIME>"

    assert specialty_carrier_would_collapse("DATERANGE", "TEXT") is True
    assert specialty_carrier_would_collapse("RANGE<DATE>", "VARCHAR") is True
    assert specialty_carrier_would_collapse("DATERANGE", "RANGE<DATE>") is False


def test_bq_bignumeric_polarity_vs_numeric():
    from services.schema_introspect import _bq_to_logical
    from services.type_system import (
        ddl_carrier_type,
        ddl_type,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    assert _bq_to_logical("NUMERIC") == "DECIMAL"
    assert _bq_to_logical("BIGNUMERIC") == "BIGNUMERIC"
    assert _bq_to_logical("BIGNUMERIC(76,38)") == "BIGNUMERIC(76,38)"
    assert _bq_to_logical("BIGNUMERIC", precision=40, scale=10) == "BIGNUMERIC(40,10)"

    assert normalize_logical_type("BIGNUMERIC") == "decimal"
    assert normalize_logical_type("BIGNUMERIC(76,38)") == "decimal"
    assert parse_numeric_precision_scale("BIGNUMERIC(76,38)") == (76, 38)

    assert ddl_carrier_type("BIGNUMERIC") == "BIGNUMERIC"
    assert ddl_carrier_type("BIGNUMERIC(40,10)") == "BIGNUMERIC(40,10)"

    assert ddl_type("bigquery", "BIGNUMERIC") == "BIGNUMERIC"
    assert ddl_type("bigquery", "BIGNUMERIC(40,10)") == "BIGNUMERIC(40,10)"
    # PostgreSQL NUMERIC accepts high precision — never invent TEXT for bare BIGNUMERIC.
    assert "NUMERIC" in ddl_type("postgresql", "BIGNUMERIC").upper()
    assert ddl_type("postgresql", "BIGNUMERIC(40,10)") == "NUMERIC(40,10)"
    # Snowflake NUMBER max 38 — oversize BIGNUMERIC falls to lossless text.
    assert ddl_type("snowflake", "BIGNUMERIC(76,38)").upper() in {
        "TEXT",
        "VARCHAR",
        "STRING",
    }


def test_bq_field_repeated_record_still_nested():
    """Live schema fields path must stay aligned with dtype-string path."""
    from types import SimpleNamespace

    from services.schema_introspect import _bq_field_to_logical

    child = SimpleNamespace(
        name="id",
        field_type="INT64",
        mode="NULLABLE",
        fields=[],
        precision=None,
        scale=None,
        max_length=None,
    )
    parent = SimpleNamespace(
        name="items",
        field_type="RECORD",
        mode="REPEATED",
        fields=[child],
        precision=None,
        scale=None,
        max_length=None,
    )
    assert _bq_field_to_logical(parent) == "ARRAY<STRUCT<id:INTEGER>>"

    bare_array = SimpleNamespace(
        name="tags",
        field_type="STRING",
        mode="REPEATED",
        fields=[],
        precision=None,
        scale=None,
        max_length=None,
    )
    assert _bq_field_to_logical(bare_array) == "ARRAY<TEXT>"
