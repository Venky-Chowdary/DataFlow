"""ARRAY temporal elements use apply_transform — no fromisoformat invent.

time.fromisoformat('1704067200') invented 17:04:06.720000. ISO-only
parsing refused unambiguous 31/12/2024 / 12/31/2024 the write path binds.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import array_element_unfit_reason  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402


def test_time_transform_does_not_invent_clock_from_epoch_digits():
    parsed, err = apply_transform("1704067200", "time")
    assert parsed is None
    assert err and "Invalid time" in err
    clock, clock_err = apply_transform("17:04:06", "time")
    assert clock_err is None
    assert str(clock).startswith("17:04:06")
    offset, offset_err = apply_transform("15:30:00+05:30", "time")
    assert offset_err is None
    assert offset is not None


def test_date_array_binds_unambiguous_slash_calendars():
    dmy, err = apply_transform("31/12/2024", "date")
    assert err is None and dmy == "2024-12-31"
    mdy, err2 = apply_transform("12/31/2024", "date")
    assert err2 is None and mdy == "2024-12-31"
    assert array_element_unfit_reason("31/12/2024", "DATE") is None
    assert array_element_unfit_reason("12/31/2024", "DATE") is None
    assert array_element_unfit_reason("2024-01-15", "DATE") is None


def test_date_array_refuses_auto_ambiguous_slash_and_epoch():
    amb, err = apply_transform("01/02/2024", "date")
    assert amb is None and err
    assert array_element_unfit_reason("01/02/2024", "DATE")
    epoch_date, epoch_err = apply_transform("1704067200", "date")
    assert epoch_date is None and epoch_err
    assert array_element_unfit_reason("1704067200", "DATE")
    assert array_element_unfit_reason("1704067200", "TIME")


def test_datetime_array_still_binds_epoch():
    dt, err = apply_transform("1704067200", "datetime")
    assert err is None
    assert str(dt).startswith("2024-01-01T00:00:00")
    assert array_element_unfit_reason("1704067200", "TIMESTAMP") is None
    assert array_element_unfit_reason("2024-01-01T00:00:00Z", "TIMESTAMPTZ") is None
