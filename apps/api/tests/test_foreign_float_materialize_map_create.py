"""Map≡CREATE — foreign IEEE float aliases rematerialize to dest ddl_type SSOT.

Headline invents: FLOAT4/HALF/BINARY_FLOAT illegal CREATE tokens; REAL on
MySQL/Snowflake/BQ; Spanner single → FLOAT32 (never invent REAL).
"""

from __future__ import annotations

import pytest

from services.type_system import ddl_type, materialize_dest_ddl


_GAP_CASES: list[tuple[str, str]] = [
    # PostgreSQL — aliases → REAL / DOUBLE PRECISION
    ("postgresql", "FLOAT4"),
    ("postgresql", "FLOAT8"),
    ("postgresql", "HALF"),
    ("postgresql", "FLOAT16"),
    ("postgresql", "FLOAT32"),
    ("postgresql", "FLOAT64"),
    ("postgresql", "BINARY_FLOAT"),
    ("postgresql", "BINARY_DOUBLE"),
    ("postgresql", "DOUBLE"),
    ("postgresql", "FLOAT(24)"),
    ("postgresql", "FLOAT(53)"),
    # MySQL — foreign singles/doubles → FLOAT / DOUBLE
    ("mysql", "FLOAT4"),
    ("mysql", "FLOAT8"),
    ("mysql", "REAL"),
    ("mysql", "HALF"),
    ("mysql", "FLOAT16"),
    ("mysql", "FLOAT32"),
    ("mysql", "FLOAT64"),
    ("mysql", "BINARY_FLOAT"),
    ("mysql", "BINARY_DOUBLE"),
    ("mysql", "DOUBLE PRECISION"),
    # SQL Server
    ("sqlserver", "FLOAT4"),
    ("sqlserver", "FLOAT8"),
    ("sqlserver", "HALF"),
    ("sqlserver", "DOUBLE"),
    ("sqlserver", "DOUBLE PRECISION"),
    ("sqlserver", "BINARY_FLOAT"),
    ("sqlserver", "FLOAT64"),
    # Snowflake — only FLOAT wire
    ("snowflake", "FLOAT4"),
    ("snowflake", "REAL"),
    ("snowflake", "DOUBLE"),
    ("snowflake", "DOUBLE PRECISION"),
    ("snowflake", "HALF"),
    ("snowflake", "BINARY_DOUBLE"),
    # Oracle — BINARY_FLOAT / BINARY_DOUBLE wire
    ("oracle", "FLOAT4"),
    ("oracle", "REAL"),
    ("oracle", "DOUBLE"),
    ("oracle", "FLOAT"),
    ("oracle", "HALF"),
    ("oracle", "FLOAT64"),
    # BigQuery — FLOAT64 only
    ("bigquery", "FLOAT4"),
    ("bigquery", "REAL"),
    ("bigquery", "DOUBLE"),
    ("bigquery", "FLOAT"),
    ("bigquery", "HALF"),
    ("bigquery", "BINARY_FLOAT"),
    # Spanner — FLOAT32 / FLOAT64 (never REAL invent)
    ("spanner", "FLOAT4"),
    ("spanner", "REAL"),
    ("spanner", "HALF"),
    ("spanner", "FLOAT32"),
    ("spanner", "DOUBLE"),
    ("spanner", "BINARY_FLOAT"),
    # DuckDB / SQLite / Databricks / Iceberg / Redshift
    ("duckdb", "FLOAT4"),
    ("duckdb", "HALF"),
    ("duckdb", "FLOAT64"),
    ("duckdb", "DOUBLE PRECISION"),
    ("duckdb", "BINARY_FLOAT"),
    ("sqlite", "FLOAT4"),
    ("sqlite", "DOUBLE"),
    ("sqlite", "HALF"),
    ("sqlite", "FLOAT64"),
    ("sqlite", "DOUBLE PRECISION"),
    ("databricks", "FLOAT4"),
    ("databricks", "REAL"),
    ("databricks", "HALF"),
    ("databricks", "FLOAT16"),
    ("databricks", "DOUBLE PRECISION"),
    ("databricks", "BINARY_DOUBLE"),
    ("iceberg", "FLOAT4"),
    ("iceberg", "REAL"),
    ("iceberg", "HALF"),
    ("iceberg", "DOUBLE PRECISION"),
    ("iceberg", "FLOAT64"),
    ("redshift", "FLOAT4"),
    ("redshift", "HALF"),
    ("redshift", "FLOAT64"),
    ("redshift", "DOUBLE"),
    ("redshift", "BINARY_FLOAT"),
]


_NATIVE_PASS: list[tuple[str, str]] = [
    ("postgresql", "REAL"),
    ("postgresql", "DOUBLE PRECISION"),
    ("mysql", "FLOAT"),
    ("mysql", "DOUBLE"),
    ("sqlserver", "REAL"),
    ("sqlserver", "FLOAT"),
    ("sqlserver", "FLOAT(53)"),
    ("snowflake", "FLOAT"),
    ("oracle", "BINARY_FLOAT"),
    ("oracle", "BINARY_DOUBLE"),
    ("bigquery", "FLOAT64"),
    ("spanner", "FLOAT64"),
    ("spanner", "FLOAT32"),
    ("duckdb", "REAL"),
    ("duckdb", "DOUBLE"),
    ("sqlite", "REAL"),
    ("databricks", "FLOAT"),
    ("databricks", "DOUBLE"),
    ("iceberg", "float"),
    ("iceberg", "double"),
    ("redshift", "REAL"),
    ("redshift", "DOUBLE PRECISION"),
]


_HEADLINES: list[tuple[str, str, str]] = [
    ("sqlserver", "FLOAT4", "REAL"),
    ("mysql", "REAL", "FLOAT"),
    ("postgresql", "FLOAT4", "REAL"),
    ("postgresql", "HALF", "REAL"),
    ("bigquery", "REAL", "FLOAT64"),
    ("oracle", "REAL", "BINARY_FLOAT"),
    ("snowflake", "DOUBLE", "FLOAT"),
    ("spanner", "FLOAT4", "FLOAT32"),
    ("spanner", "REAL", "FLOAT32"),
    ("databricks", "HALF", "FLOAT"),
    ("iceberg", "REAL", "float"),
    ("sqlite", "DOUBLE PRECISION", "REAL"),
]


@pytest.mark.parametrize("dest,carrier", _GAP_CASES)
def test_foreign_float_materialize_matches_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest,carrier", _NATIVE_PASS)
def test_native_float_stamps_still_pass_through(dest: str, carrier: str):
    got = materialize_dest_ddl(dest, carrier)
    # SQL Server FLOAT(n) is native typmod — Map authority keeps the stamp even
    # when ddl_type normalizes bare FLOAT(53) → FLOAT.
    if dest == "sqlserver" and carrier.upper().startswith("FLOAT("):
        assert got.upper() == carrier.upper(), got
        return
    expected = ddl_type(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )
    assert got


@pytest.mark.parametrize("dest,carrier,want", _HEADLINES)
def test_foreign_float_headlines(dest: str, carrier: str, want: str):
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == want.upper().replace(" ", ""), (
        f"{dest} {carrier}: got={got!r} want={want!r}"
    )


def test_spanner_single_precision_is_float32_not_real():
    """Spanner GoogleSQL has FLOAT32/FLOAT64 — never invent REAL."""
    assert ddl_type("spanner", "FLOAT4") == "FLOAT32"
    assert ddl_type("spanner", "REAL") == "FLOAT32"
    assert ddl_type("spanner", "BINARY_FLOAT") == "FLOAT32"
    assert materialize_dest_ddl("spanner", "FLOAT4") == "FLOAT32"
    assert materialize_dest_ddl("spanner", "REAL") == "FLOAT32"
