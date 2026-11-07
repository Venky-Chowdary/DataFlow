"""generic_sql bind path must use the same temporal coerce as MySQL/Postgres."""

from __future__ import annotations

from datetime import date, datetime, timezone

from connectors.generic_sql import _to_sa_value
from connectors.sql_temporal import dest_uses_sql_wire_probe, logical_to_temporal_ddl
from services.coercion_probe import analyze_coercion


def test_logical_to_temporal_ddl():
    assert logical_to_temporal_ddl("datetime") == "DATETIME"
    assert logical_to_temporal_ddl("timestamp") == "DATETIME"
    assert logical_to_temporal_ddl("timestamptz") == "TIMESTAMPTZ"
    assert logical_to_temporal_ddl("datetimeoffset") == "DATETIMEOFFSET"
    assert logical_to_temporal_ddl("timestamp_ntz") == "DATETIME"
    assert logical_to_temporal_ddl("date") == "DATE"
    assert logical_to_temporal_ddl("time") == "TIME"
    assert logical_to_temporal_ddl("string") is None


def test_wire_probe_covers_oracle_sqlserver_generic():
    for dest in ("oracle", "sqlserver", "mssql", "generic_sql", "mariadb"):
        assert dest_uses_sql_wire_probe(dest), dest


def test_generic_sql_iso_z_to_datetime_bind():
    # Oracle TIMESTAMP (NTZ) keeps civil digits — strip Z without inventing UTC tzinfo.
    got = _to_sa_value("2024-08-09T01:58:42Z", "datetime", None, "", "oracle")
    assert isinstance(got, datetime)
    assert got.tzinfo is None
    assert got == datetime(2024, 8, 9, 1, 58, 42)

    # TIMESTAMPTZ carriers must stay aware UTC (refuse naive invent).
    aware = _to_sa_value("2024-08-09T01:58:42Z", "TIMESTAMPTZ", None, "", "oracle")
    assert aware == datetime(2024, 8, 9, 1, 58, 42, tzinfo=timezone.utc)


def test_generic_sql_iso_z_to_date_bind():
    got = _to_sa_value("2024-08-09T01:58:42Z", "date", None, "", "sqlserver")
    assert got == date(2024, 8, 9)


def test_coercion_probe_refuses_iso_z_into_oracle_ntz_like_the_writer():
    """An offset-bearing 'Z' wire into Oracle TIMESTAMP (zoneless) is held out by
    the write-time quarantine matrix; Validate used to call it a mere wire
    normalize and go green (M6). Both now apply the writer's one rule."""
    report = analyze_coercion(
        sample_rows=[{"ts": "2024-08-09T01:58:42Z"}],
        mappings=[{"source": "ts", "target": "ts", "confidence": 0.99}],
        source_types={"ts": "VARCHAR"},
        dest_types={"ts": "TIMESTAMP"},
        dest_db_type="oracle",
    )
    assert report["columns"], report
    col = report["columns"][0]
    assert col["severity"] == "block", col
    assert "timezone-aware value into NTZ destination" in str(col.get("sample_failures")), col
