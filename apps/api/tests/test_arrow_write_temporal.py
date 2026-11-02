"""Arrow / Iceberg / Parquet coerce binds through the write-path parsers.

fromisoformat refused slash calendars and epochs. Decimal(text) invented
Auto 1.234 and refused locale money Execute would store.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pa = pytest.importorskip("pyarrow")

from connectors.iceberg_writer import _coerce_arrow_cell  # noqa: E402
from services.arrow_write import coerce_arrow_cell  # noqa: E402


def test_arrow_date_binds_unambiguous_slash_and_refuses_auto():
    d32 = pa.date32()
    assert coerce_arrow_cell("31/12/2024", d32, pa) == date(2024, 12, 31)
    assert coerce_arrow_cell("12/31/2024", d32, pa) == date(2024, 12, 31)
    assert _coerce_arrow_cell("31/12/2024", d32, pa) == date(2024, 12, 31)
    with pytest.raises(ValueError, match="date|invent"):
        coerce_arrow_cell("01/02/2024", d32, pa)


def test_arrow_timestamp_binds_slash_epoch_iso():
    ts = pa.timestamp("us")
    assert coerce_arrow_cell("31/12/2024", ts, pa) == datetime(2024, 12, 31, 0, 0, 0)
    assert coerce_arrow_cell("2024-06-01T12:00:00", ts, pa) == datetime(2024, 6, 1, 12, 0, 0)
    epoch_s = coerce_arrow_cell("1704451800", ts, pa)
    epoch_ms = coerce_arrow_cell("1704451800000", ts, pa)
    assert epoch_s == epoch_ms
    assert epoch_s == datetime.fromtimestamp(1704451800, tz=timezone.utc).replace(
        tzinfo=None
    )
    with pytest.raises(ValueError, match="timestamp|invent"):
        coerce_arrow_cell("01/02/2024", ts, pa)


def test_arrow_timestamptz_still_refuses_naive():
    tz_type = pa.timestamp("us", tz="UTC")
    with pytest.raises(ValueError, match="naive|TIMESTAMPTZ"):
        coerce_arrow_cell(datetime(2024, 6, 1, 12, 0, 0), tz_type, pa)
    with pytest.raises(ValueError, match="naive|TIMESTAMPTZ"):
        coerce_arrow_cell("2024-06-01T12:00:00", tz_type, pa)
    aware = coerce_arrow_cell("2024-06-01T12:00:00Z", tz_type, pa)
    assert aware.tzinfo is not None


def test_arrow_time_binds_ampm():
    t64 = pa.time64("us")
    assert coerce_arrow_cell("15:30:00", t64, pa) == time(15, 30, 0)
    bound = coerce_arrow_cell("3:30 PM", t64, pa)
    assert bound.hour == 15
    assert bound.minute == 30
    with pytest.raises(ValueError, match="time|invent"):
        coerce_arrow_cell("not-a-time", t64, pa)


def test_arrow_decimal_locale_money_and_auto_refuse():
    dec = pa.decimal128(10, 2)
    assert coerce_arrow_cell("$1,234.56", dec, pa) == Decimal("1234.56")
    assert coerce_arrow_cell("€1.234,56", dec, pa) == Decimal("1234.56")
    assert coerce_arrow_cell("$1,234", dec, pa) == Decimal("1234")
    with pytest.raises(ValueError, match="decimal|invent"):
        coerce_arrow_cell("1,234", dec, pa)
    with pytest.raises(ValueError, match="decimal|invent"):
        coerce_arrow_cell("1.234", dec, pa)


def test_arrow_integer_locale_and_auto_refuse():
    i64 = pa.int64()
    assert coerce_arrow_cell("$1,234", i64, pa) == 1234
    assert coerce_arrow_cell("true", i64, pa) == 1
    assert coerce_arrow_cell(0, i64, pa) == 0
    with pytest.raises(ValueError, match="boolean|INTEGER|invent"):
        coerce_arrow_cell(True, i64, pa)
    with pytest.raises(ValueError, match="boolean|INTEGER|invent"):
        coerce_arrow_cell(False, i64, pa)
    with pytest.raises(ValueError, match="boolean|INTEGER|invent"):
        _coerce_arrow_cell(True, i64, pa)
    with pytest.raises(ValueError, match="INTEGER|invent"):
        coerce_arrow_cell("1,234", i64, pa)
    with pytest.raises(ValueError, match="INTEGER|invent"):
        coerce_arrow_cell("not-an-int", i64, pa)
