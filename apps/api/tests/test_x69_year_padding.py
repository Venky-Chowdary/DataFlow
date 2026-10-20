"""X-69: temporal binds/fingerprints always render a 4-digit year.

glibc ``strftime("%Y")`` does not zero-pad years below 1000, so the SQL
Server datetime2 bind wrote ``'1-01-01 00:00:00.0000000'`` and the server
read it as 2001-01-01 — corruption caught only by read-back. Every bind
and every fingerprint/display render now goes through explicit fields.
"""

import sys
from datetime import datetime, date
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_x69_sqlserver_datetime2_bind_pads_year():
    from connectors.sqlserver_datetime2 import sqlserver_datetime2_bind_text

    low = sqlserver_datetime2_bind_text(datetime(1, 1, 1), precision=7)
    assert low.startswith("0001-01-01 00:00:00."), low
    mid = sqlserver_datetime2_bind_text(datetime(999, 12, 31, 23, 59, 59, 999999), precision=7)
    assert mid == "0999-12-31 23:59:59.9999990", mid
    y1000 = sqlserver_datetime2_bind_text(datetime(1000, 1, 1), precision=7)
    assert y1000.startswith("1000-01-01")


def test_x69_fingerprint_pads_year():
    from services.reconciliation import fingerprint_for_reconcile, _canon_dt_text

    assert _canon_dt_text(datetime(1, 1, 1)) == "0001-01-01T00:00:00"
    assert _canon_dt_text(datetime(99, 6, 15)) == "0099-06-15T00:00:00"
    fp = fingerprint_for_reconcile(datetime(1, 1, 1), ddl_type="datetime2(7)")
    assert fp == "0001-01-01T00:00:00", fp


def test_x69_format_wire_value_pads_year():
    from connectors.sql_temporal import format_wire_value

    out = format_wire_value(datetime(9, 9, 9, 10, 0, 0, 500), "DATETIME2", engine="sqlserver")
    assert out.startswith("0009-09-09"), out


def test_x69_date_only_pads_year():
    from services.reconciliation import fingerprint_for_reconcile

    fp = fingerprint_for_reconcile(date(1752, 9, 14), ddl_type="date")
    assert fp.startswith("1752-09-14"), fp


import pytest


@pytest.mark.skipif(
    __import__("os").environ.get("DATAFLOW_X69_LIVE") != "1",
    reason="live engines required — DATAFLOW_X69_LIVE=1",
)
def test_x69_low_years_roundtrip_live():
    """DuckDB/PG/MySQL sources: {0001-01-01, 0099-06-15, 0999-12-31,
    1752-09-14, 1000-01-01} -> MSSQL datetime2/date/datetimeoffset, clean
    exact + tamper; plus one DATETIME(<1753) leg expecting the range finding."""
    pytest.fail("live leg — runs on the user's engine matrix")
