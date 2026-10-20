"""X-66: MSSQL scale-7 temporals read as text; µs-dest compare is disclosed.

(a) ``datetime2(7)``/``datetimeoffset(7)``/``time(7)`` are read via CONVERT/
CAST to ISO text in the engine SELECT — drivers hand back µs datetimes and
silently truncate the 7th digit before Gate-8 can ever see it.

(b)+(c) The verifier compares at the destination carrier's microsecond
granularity and the run discloses which columns lost digits via
``carrier_rounded_columns`` — never a plain "verified" on a lossy column.
"""

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.carrier_instant import carrier_rounded_columns  # noqa: E402
from services.copy_sqlserver_pg import sqlserver_scale7_select_expr  # noqa: E402
from services.reconciliation import fingerprint_for_reconcile  # noqa: E402


def test_x66_scale7_columns_select_as_text():
    assert "CONVERT(varchar(27), [t], 126)" in sqlserver_scale7_select_expr(
        "t", "datetime2(7)"
    )
    assert "CONVERT(varchar(34), [t], 127)" in sqlserver_scale7_select_expr(
        "t", "datetimeoffset(7)"
    )
    assert "CAST([t] AS varchar(16))" in sqlserver_scale7_select_expr("t", "time(7)")
    # Scale <= 6 and non-temporal columns stay plain (driver µs is lossless).
    assert sqlserver_scale7_select_expr("t", "datetime2(6)") == "[t]"
    assert sqlserver_scale7_select_expr("t", "varchar(50)") == "[t]"
    assert sqlserver_scale7_select_expr("t", "datetime") == "[t]"


def test_x66_scale7_text_compares_at_microsecond_granularity():
    """Clean run on a µs dest: the 7-digit source and 6-digit dest must
    compare equal — that is the accept-truncation compare."""
    src = fingerprint_for_reconcile("2024-01-01T10:00:00.1234567", ddl_type="datetime2(7)")
    dst = fingerprint_for_reconcile("2024-01-01T10:00:00.123456", ddl_type="timestamp")
    assert src == dst == "2024-01-01T10:00:00.123456"
    # A µs-visible tamper still fails.
    tampered = fingerprint_for_reconcile(
        "2024-01-01T10:00:00.1234577", ddl_type="datetime2(7)"
    )
    assert src != tampered


def test_x66_datetimeoffset_folds_to_utc_at_us():
    fp = fingerprint_for_reconcile(
        "2024-01-01T10:00:00.1234567+05:30", ddl_type="datetimeoffset(7)"
    )
    assert fp == "2024-01-01T04:30:00.123456"


def test_x66_us_dest_discloses_scale7_narrowing():
    mappings = [
        {"source": "t", "target": "t", "source_type": "datetime2(7)", "target_type": "timestamp"},
        {"source": "d", "target": "d", "source_type": "datetimeoffset(7)", "target_type": "timestamptz(6)"},
        {"source": "v", "target": "v", "source_type": "varchar(50)", "target_type": "text"},
    ]
    rounded = carrier_rounded_columns(mappings, dest_engine="postgresql")
    cols = {r["column"]: r for r in rounded}
    assert set(cols) == {"t", "d"}
    assert cols["t"]["kept_fractional_digits"] == 6
    # Same-engine precision is not a narrowing.
    assert carrier_rounded_columns(
        [{"source": "t", "target": "t", "source_type": "datetime2(7)", "target_type": "datetime2(7)"}],
        dest_engine="sqlserver",
    ) == []


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X66_LIVE") != "1",
    reason="live engines required — DATAFLOW_X66_LIVE=1",
)
def test_x66_dt2_dto_time7_clean_and_tamper_live():
    """5 DT2 + 5 DTO + 5 TIME(7) values -> MySQL/PG/SQLite/DuckDB, clean +
    tamper, plus one row whose only difference is the 7th digit; the verdict
    names the µs-granularity disclosure on those columns."""
    pytest.fail("live leg — runs on the user's engine matrix")
