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
    assert logical_to_temporal_ddl("timestamp_ntz") == "DATETIME"
    assert logical_to_temporal_ddl("date") == "DATE"
    assert logical_to_temporal_ddl("time") == "TIME"
    assert logical_to_temporal_ddl("string") is None


def test_wire_probe_covers_oracle_sqlserver_generic():
    for dest in ("oracle", "sqlserver", "mssql", "generic_sql", "mariadb"):
        assert dest_uses_sql_wire_probe(dest), dest


def test_generic_sql_iso_z_to_datetime_bind():
    got = _to_sa_value("2024-08-09T01:58:42Z", "datetime", None, "", "oracle")
    assert isinstance(got, datetime)
    assert got == datetime(2024, 8, 9, 1, 58, 42, tzinfo=timezone.utc)


def test_generic_sql_iso_z_to_date_bind():
    got = _to_sa_value("2024-08-09T01:58:42Z", "date", None, "", "sqlserver")
    assert got == date(2024, 8, 9)


def test_coercion_probe_warns_iso_z_for_oracle():
    report = analyze_coercion(
        sample_rows=[{"ts": "2024-08-09T01:58:42Z"}],
        mappings=[{"source": "ts", "target": "ts", "confidence": 0.99}],
        source_types={"ts": "VARCHAR"},
        dest_types={"ts": "TIMESTAMP"},
        dest_db_type="oracle",
    )
    assert report["columns"], report
    col = report["columns"][0]
    assert col.get("wire_normalize", 0) >= 1 or col.get("sample_wire_form")
