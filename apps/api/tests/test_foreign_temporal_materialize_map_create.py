"""Map≡CREATE — foreign temporal stamps rematerialize to dest ddl_type SSOT.

Headline invents: SQL Server TIMESTAMP≡ROWVERSION; MySQL TIMESTAMP session-TZ
vs DATETIME(6); Snowflake TIMESTAMP vs TIMESTAMP_NTZ polarity.
"""

from __future__ import annotations

import pytest

from services.type_system import ddl_type, materialize_dest_ddl


_GAP_CASES: list[tuple[str, str]] = [
    # SQL Server — TIMESTAMP must never CREATE as ROWVERSION
    ("sqlserver", "TIMESTAMP"),
    ("sqlserver", "TIMESTAMP(6)"),
    ("sqlserver", "DATETIME"),
    ("sqlserver", "TIMESTAMPTZ"),
    ("sqlserver", "TIMESTAMP_NTZ"),
    ("sqlserver", "TIMESTAMP WITH TIME ZONE"),
    # MySQL — TIMESTAMP/DATETIME → DATETIME(6); TIME → TIME(6)
    ("mysql", "TIMESTAMP"),
    ("mysql", "TIMESTAMP(6)"),
    ("mysql", "DATETIME"),
    ("mysql", "TIME"),
    ("mysql", "DATETIME2"),
    ("mysql", "DATETIME2(7)"),
    ("mysql", "TIMESTAMPTZ"),
    ("mysql", "TIMESTAMP WITH TIME ZONE"),
    # PostgreSQL — foreign aliases
    ("postgresql", "DATETIME"),
    ("postgresql", "DATETIME2(7)"),
    ("postgresql", "TIMESTAMP_NTZ"),
    ("postgresql", "DATETIMEOFFSET"),
    ("postgresql", "TIMESTAMP WITH TIME ZONE"),
    # Snowflake polarity
    ("snowflake", "TIMESTAMP"),
    ("snowflake", "DATETIME"),
    ("snowflake", "TIMESTAMPTZ"),
    ("snowflake", "DATETIME2(7)"),
    ("snowflake", "TIMESTAMP WITH TIME ZONE"),
    # BigQuery — foreign aliases (keep native DATETIME/TIMESTAMP wires)
    ("bigquery", "DATETIME2"),
    ("bigquery", "TIMESTAMP_NTZ"),
    ("bigquery", "TIMESTAMPTZ"),
    ("bigquery", "DATETIMEOFFSET"),
    # Oracle — foreign aliases
    ("oracle", "DATETIME"),
    ("oracle", "DATETIME2(7)"),
    ("oracle", "TIMESTAMP_NTZ"),
    ("oracle", "TIME"),
    # Redshift / DuckDB
    ("redshift", "DATETIME"),
    ("redshift", "DATETIME2"),
    ("redshift", "TIMESTAMP_NTZ"),
    ("duckdb", "DATETIME"),
    ("duckdb", "DATETIME2(7)"),
    ("duckdb", "TIMESTAMP_NTZ"),
]


_NATIVE_PASS: list[tuple[str, str]] = [
    ("mysql", "DATE"),
    ("mysql", "YEAR"),
    ("mysql", "DATETIME(6)"),
    ("mysql", "TIME(6)"),
    ("postgresql", "TIMESTAMP"),
    ("postgresql", "TIMESTAMPTZ"),
    ("postgresql", "DATE"),
    ("postgresql", "TIME"),
    ("sqlserver", "DATETIME2"),
    ("sqlserver", "DATETIME2(7)"),
    ("sqlserver", "DATETIMEOFFSET"),
    ("sqlserver", "DATE"),
    ("sqlserver", "SMALLDATETIME"),
    ("snowflake", "TIMESTAMP_NTZ"),
    ("snowflake", "TIMESTAMP_LTZ"),
    ("snowflake", "TIMESTAMP_TZ"),
    ("snowflake", "DATE"),
    ("bigquery", "DATETIME"),
    ("bigquery", "DATE"),
    ("bigquery", "TIME"),
    ("oracle", "TIMESTAMP"),
    ("oracle", "DATE"),
    ("redshift", "TIMESTAMP"),
    ("redshift", "TIMESTAMPTZ"),
    ("duckdb", "TIMESTAMP"),
    ("duckdb", "TIMESTAMPTZ"),
]


@pytest.mark.parametrize("dest,carrier", _GAP_CASES)
def test_foreign_temporal_materialize_matches_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest,carrier", _NATIVE_PASS)
def test_native_temporal_stamps_still_match_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", "")


def test_sqlserver_timestamp_never_rowversion_invent():
    """T-SQL TIMESTAMP is ROWVERSION — create-new must stamp DATETIME2(7)."""
    assert materialize_dest_ddl("sqlserver", "TIMESTAMP") == "DATETIME2(7)"
    assert ddl_type("sqlserver", "TIMESTAMP") == "DATETIME2(7)"
    from services.type_system import is_rowversion_carrier

    # Logical TIMESTAMP is datetime family; ROWVERSION is a different carrier.
    assert not is_rowversion_carrier("TIMESTAMP")
    assert is_rowversion_carrier("ROWVERSION")
    assert materialize_dest_ddl("sqlserver", "ROWVERSION") == "ROWVERSION"


def test_mysql_timestamp_becomes_datetime6_not_session_tz():
    assert materialize_dest_ddl("mysql", "TIMESTAMP") == "DATETIME(6)"
    assert materialize_dest_ddl("mysql", "DATETIME") == "DATETIME(6)"


def test_snowflake_timestamp_becomes_ntz_polarity():
    assert materialize_dest_ddl("snowflake", "TIMESTAMP").upper() == "TIMESTAMP_NTZ"
    assert materialize_dest_ddl("snowflake", "TIMESTAMPTZ").upper() == "TIMESTAMP_LTZ"
