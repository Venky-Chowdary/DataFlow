"""Snowflake/BigQuery destination wire + BigQuery insert quarantine mapping."""

from __future__ import annotations

from connectors.warehouse_temporal import (
    coerce_mapped_rows_snowflake,
    format_bigquery_bind,
    format_snowflake_bind,
    quarantine_from_bigquery_errors,
    records_for_bigquery,
    wire_check_warehouse,
)
from connectors.sql_temporal import dest_uses_sql_wire_probe
from services.coercion_probe import analyze_coercion


def test_wire_probe_includes_warehouses():
    assert dest_uses_sql_wire_probe("snowflake")
    assert dest_uses_sql_wire_probe("bigquery")


def test_snowflake_iso_z_to_ntz_string():
    got = format_snowflake_bind("2024-08-09T01:58:42Z", "TIMESTAMP_NTZ")
    assert got == "2024-08-09 01:58:42"
    assert "T" not in got and not got.endswith("Z")


def test_bigquery_timestamp_rfc3339():
    got = format_bigquery_bind("2024-08-09T01:58:42Z", "TIMESTAMP")
    assert isinstance(got, str)
    assert got.startswith("2024-08-09T01:58:42")
    assert got.endswith("Z") or "+00:00" in got


def test_bigquery_datetime_no_z():
    got = format_bigquery_bind("2024-08-09T01:58:42Z", "DATETIME")
    assert got == "2024-08-09T01:58:42"


def test_coerce_mapped_rows_snowflake():
    rows = [("2024-08-09T01:58:42Z",)]
    out = coerce_mapped_rows_snowflake(rows, ["TIMESTAMP_NTZ(9)"])
    assert out[0][0] == "2024-08-09 01:58:42"


def test_records_for_bigquery_normalizes():
    records = records_for_bigquery(
        [("2024-08-09T01:58:42Z",)],
        ["ts"],
        ["TIMESTAMP"],
    )
    assert records[0]["ts"].startswith("2024-08-09T01:58:42")


def test_quarantine_from_bigquery_errors():
    batch = [("ok",), ("bad",), ("ok2",)]
    errors = [
        {
            "index": 1,
            "errors": [{"reason": "invalid", "message": "Invalid timestamp", "location": "ts"}],
        }
    ]
    details, bad = quarantine_from_bigquery_errors(
        errors, batch, ["ts"], row_offset=100, policy="quarantine"
    )
    assert bad == {1}
    assert details[0]["row"] == 101
    assert details[0]["column"] == "ts"
    assert "Invalid timestamp" in details[0]["reason"]


def test_wire_check_warehouse_normalize_flag():
    check = wire_check_warehouse("2024-08-09T01:58:42Z", "TIMESTAMP_NTZ", engine="snowflake")
    assert check["ok"] is True
    assert check["needs_normalize"] is True
    assert "2024-08-09 01:58:42" in str(check["wire_value"])


def test_coercion_probe_snowflake_iso_z():
    report = analyze_coercion(
        sample_rows=[{"ts": "2024-08-09T01:58:42Z"}],
        mappings=[{"source": "ts", "target": "ts", "confidence": 0.99}],
        source_types={"ts": "VARCHAR"},
        dest_types={"ts": "TIMESTAMP_NTZ"},
        dest_db_type="snowflake",
    )
    assert report["columns"]
    col = report["columns"][0]
    assert col.get("wire_normalize", 0) >= 1 or col.get("sample_wire_form")


def test_coercion_probe_bigquery_iso_z():
    report = analyze_coercion(
        sample_rows=[{"ts": "2024-08-09T01:58:42Z"}],
        mappings=[{"source": "ts", "target": "ts", "confidence": 0.99}],
        source_types={"ts": "VARCHAR"},
        dest_types={"ts": "TIMESTAMP"},
        dest_db_type="bigquery",
    )
    assert report["columns"]
    col = report["columns"][0]
    assert col.get("wire_normalize", 0) >= 1 or col.get("sample_wire_form")
