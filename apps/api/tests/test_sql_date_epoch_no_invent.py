"""SQL DATE/TIME bind matches apply_transform — no epoch invent.

DATE is a calendar. Epoch 1704067200 is an instant; binding it as 2024-01-01
invented a UTC day apply_transform date already refuses. TIME fromisoformat
invented 17:04:06.720000 from the same digits. Compact YYYYMMDD still binds.
DATETIME still binds epoch.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402
from connectors.sql_temporal import coerce_sql_temporal, parse_sql_date, parse_sql_datetime  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402


def test_date_bind_refuses_epoch_matches_apply_transform():
    parsed, err = apply_transform("1704067200", "date")
    assert parsed is None and err
    assert parse_sql_date("1704067200") is None
    assert parse_sql_date(1704067200) is None
    assert parse_sql_date(1704067200000) is None
    with pytest.raises(ValueError, match="epoch"):
        normalize_sql_bind_value("1704067200", "DATE")
    with pytest.raises(ValueError, match="epoch"):
        normalize_sql_bind_value(1704067200, "DATE")


def test_date_bind_keeps_calendars_and_compact_yyyymmdd():
    assert apply_transform("20240115", "date")[0] == "2024-01-15"
    assert parse_sql_date("20240115") == date(2024, 1, 15)
    assert parse_sql_date(20240115) == date(2024, 1, 15)
    assert normalize_sql_bind_value("20240115", "DATE") == date(2024, 1, 15)
    assert normalize_sql_bind_value("31/12/2024", "DATE") == date(2024, 12, 31)
    assert normalize_sql_bind_value("12/31/2024", "DATE") == date(2024, 12, 31)
    assert normalize_sql_bind_value("2024-01-15T12:00:00Z", "DATE") == date(2024, 1, 15)
    assert parse_sql_date(datetime(2024, 6, 1, 12, 0, 0)) == date(2024, 6, 1)
    assert parse_sql_date("01/02/2024") is None
    # Unparsed calendar stays the cell — bind does not invent Jan 2 or Feb 1.
    assert normalize_sql_bind_value("01/02/2024", "DATE") == "01/02/2024"


def test_time_bind_refuses_epoch_keeps_clock():
    parsed, err = apply_transform("1704067200", "time")
    assert parsed is None and err
    with pytest.raises(ValueError, match="epoch"):
        normalize_sql_bind_value("1704067200", "TIME")
    with pytest.raises(ValueError, match="epoch"):
        normalize_sql_bind_value(1704067200, "TIME")
    clock = normalize_sql_bind_value("15:30:00", "TIME")
    assert clock == time(15, 30, 0)
    offset = normalize_sql_bind_value("15:30:00+05:30", "TIME")
    assert offset.replace(tzinfo=None) == time(15, 30, 0)


def test_datetime_bind_still_accepts_epoch():
    dt = parse_sql_datetime(1704067200, aware_utc=True)
    assert dt is not None
    assert dt.date() == date(2024, 1, 1)
    bound = coerce_sql_temporal("1704067200", "TIMESTAMP")
    assert isinstance(bound, datetime)
    assert bound.date() == date(2024, 1, 1)
