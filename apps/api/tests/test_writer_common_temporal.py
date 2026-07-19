"""Shared temporal normalize + JSON export path for all writers."""

from __future__ import annotations

from connectors.writer_common import normalize_temporal_cells, to_json_value


def test_normalize_temporal_cells_generic_sql_engines():
    rows = [("2024-08-09T01:58:42Z", "keep")]
    out = normalize_temporal_cells(rows, ["DATETIME", "VARCHAR"], ["ts", "name"], engine="mysql")
    assert out[0][1] == "keep"
    from datetime import datetime

    assert isinstance(out[0][0], datetime)
    assert out[0][0].year == 2024


def test_normalize_temporal_cells_snowflake_string():
    rows = [("2024-08-09T01:58:42Z",)]
    out = normalize_temporal_cells(rows, ["TIMESTAMP_NTZ"], ["ts"], engine="snowflake")
    assert out[0][0] == "2024-08-09 01:58:42"


def test_to_json_value_normalizes_datetime_for_object_stores():
    got = to_json_value("2024-08-09T01:58:42Z", "ts", {"ts": "datetime"})
    assert isinstance(got, str)
    assert "2024-08-09" in got
    assert "01:58:42" in got
