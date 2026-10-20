"""X-92: parse_date with a date-only format must produce a DATE, not a
midnight datetime — otherwise the column lands datetime(6)/datetime2/
TIMESTAMP instead of DATE.
"""

from __future__ import annotations

from datetime import date, datetime

from services.shape_expr import _fn_to_date


def test_date_only_format_returns_date():
    result = _fn_to_date("31/12/2024", "%d/%m/%Y")
    assert type(result) is date
    assert result == date(2024, 12, 31)


def test_datetime_format_still_returns_datetime():
    result = _fn_to_date("31/12/2024 13:45", "%d/%m/%Y %H:%M")
    assert type(result) is datetime
    assert result == datetime(2024, 12, 31, 13, 45)


def test_no_format_unchanged():
    result = _fn_to_date("2025-01-09")
    assert result == datetime(2025, 1, 9)


def test_iso_directive_counts_as_time():
    result = _fn_to_date("2024-12-31T13:45", "%Y-%m-%dT%H:%M")
    assert type(result) is datetime
