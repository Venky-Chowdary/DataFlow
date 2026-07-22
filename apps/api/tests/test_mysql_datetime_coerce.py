"""MySQL/Postgres writers + Validate wire probe for ISO-8601 timestamps."""

from __future__ import annotations

from datetime import date, datetime

from connectors.mysql_writer import _to_mysql_value
from connectors.sql_temporal import (
    coerce_sql_temporal,
    extract_column_from_sql_error,
    is_sql_data_error,
    parse_sql_datetime,
    sql_base_type,
    wire_check_temporal,
)
from connectors.write_resilience import is_connection_lost
from services.coercion_probe import analyze_coercion


def test_mysql_base_type_strips_precision():
    assert sql_base_type("DATETIME(6)") == "DATETIME"
    assert sql_base_type("TIMESTAMP(3)") == "TIMESTAMP"
    assert sql_base_type("DECIMAL(38,15)") == "DECIMAL"


def test_iso_z_datetime_coerced_for_datetime6():
    got = _to_mysql_value("2024-08-09T01:58:42Z", "DATETIME(6)")
    assert isinstance(got, datetime)
    assert got == datetime(2024, 8, 9, 1, 58, 42)


def test_physical_datetime_overrides_text_mapping_type():
    """Existing MySQL DATETIME column must win over a TEXT mapping label."""
    from connectors.mysql_writer import _apply_physical_temporal_types, _to_mysql_value

    types = _apply_physical_temporal_types(
        ["last_updated"],
        ["TEXT"],
        {"last_updated": "datetime"},
    )
    assert types[0].lower().startswith("datetime")
    got = _to_mysql_value("2026-07-04T06:57:37Z", types[0])
    assert got == datetime(2026, 7, 4, 6, 57, 37)


def test_humanize_mysql_1292_datetime_has_mapped_remediation():
    from services.error_handling import humanize_transfer_failure

    msg = "(1292, \"Incorrect datetime value: '2026-07-04T06:57:37Z' for column 'last_updated' at row 1\")"
    explained = humanize_transfer_failure(msg)
    assert explained["code"] == "mysql_incorrect_datetime"
    assert "No mapped remediation" not in (explained.get("fix") or "")
    assert "ISO" in (explained.get("fix") or "") or "DATETIME" in (explained.get("fix") or "")


def test_iso_offset_datetime_normalized_to_utc_naive():
    got = _to_mysql_value("2024-08-09T03:58:42+02:00", "DATETIME(6)")
    assert got == datetime(2024, 8, 9, 1, 58, 42)


def test_date_column_from_iso_datetime():
    got = _to_mysql_value("2024-08-09T01:58:42Z", "DATE")
    assert got == date(2024, 8, 9)


def test_plain_mysql_datetime_string_passthrough_parse():
    got = _to_mysql_value("2024-08-09 01:58:42", "DATETIME")
    assert got == datetime(2024, 8, 9, 1, 58, 42)


def test_postgres_timestamptz_also_coerces_iso_z():
    from datetime import timezone

    got = coerce_sql_temporal("2024-08-09T01:58:42Z", "TIMESTAMPTZ")
    # TIMESTAMPTZ binds as aware UTC so the driver does not reinterpret session TZ.
    assert got == datetime(2024, 8, 9, 1, 58, 42, tzinfo=timezone.utc)


def test_incorrect_datetime_is_data_error_not_connection_lost():
    msg = "(1292, \"Incorrect datetime value: '2024-08-09T01:58:42Z' for column 'column_5' at row 1\")"
    assert is_sql_data_error(msg)
    assert not is_connection_lost(msg)
    assert parse_sql_datetime("2024-08-09T01:58:42Z") == datetime(2024, 8, 9, 1, 58, 42)
    assert extract_column_from_sql_error(msg) == "column_5"


def test_wire_check_flags_iso_normalize_for_mysql_datetime():
    check = wire_check_temporal("2024-08-09T01:58:42Z", "DATETIME(6)")
    assert check["ok"] is True
    assert check["needs_normalize"] is True
    assert check["wire_value"] == "2024-08-09 01:58:42"


def test_wire_check_blocks_unparseable_temporal():
    check = wire_check_temporal("not-a-date", "DATETIME(6)")
    assert check["ok"] is False
    assert check["wire_value"] is None


def test_coercion_probe_warns_iso_z_for_mysql_dest():
    report = analyze_coercion(
        sample_rows=[{"ts": "2024-08-09T01:58:42Z"}],
        mappings=[{"source": "ts", "target": "ts", "confidence": 0.99}],
        source_types={"ts": "VARCHAR"},
        dest_types={"ts": "DATETIME(6)"},
        dest_db_type="mysql",
    )
    assert report["columns"], report
    col = report["columns"][0]
    assert col["severity"] in {"warn", "ok", "block"}
    assert col.get("wire_normalize", 0) >= 1 or col.get("sample_wire_form")
    assert col.get("sample_wire_form") == "2024-08-09 01:58:42" or (
        col.get("wire_examples") and col["wire_examples"][0].get("wire_form") == "2024-08-09 01:58:42"
    )
