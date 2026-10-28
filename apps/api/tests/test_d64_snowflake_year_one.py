"""D64 — a 0001-01-01 timestamptz failed the whole Snowflake load.

Two connector behaviours, both owned by the converter DataFlow installs on
every Snowflake connection:

* bind: the connector spells the year with ``{year:d}``, so year 1 is sent as
  ``1-01-01 00:00:00+00:00`` - not the zero-padded ISO a ``TIMESTAMP`` /
  ``DATE`` literal parser expects;
* read-back: a TIMESTAMP_LTZ is converted into the *session* zone, and for any
  zone west of UTC the year-1 instant lands in year 0, which ``datetime``
  cannot hold - ``OverflowError``, surfaced as 252005 at the Gate-8 read, so
  the loaded table was dropped for one extreme row.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from connectors.snowflake_conn import snowflake_converter_class

UTC = timezone.utc


@pytest.fixture()
def converter():
    return snowflake_converter_class()()


def test_bind_zero_pads_the_year(converter):
    assert converter.to_snowflake(datetime(1, 1, 1, tzinfo=UTC)) == "0001-01-01 00:00:00+00:00"
    assert converter.to_snowflake(datetime(1, 2, 3, 4, 5, 6, 7)) == "0001-02-03 04:05:06.000007"
    assert converter.to_snowflake(date(1, 1, 1)) == "0001-01-01"
    assert converter.to_snowflake(date(999, 12, 31)) == "0999-12-31"


def test_bind_leaves_ordinary_years_alone(converter):
    assert converter.to_snowflake(datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)) == (
        "2024-01-02 03:04:05+00:00"
    )
    assert converter.to_snowflake(date(2024, 1, 2)) == "2024-01-02"


@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles", "Asia/Kolkata"])
def test_ltz_read_of_year_one_keeps_the_instant(converter, zone):
    converter.set_parameter("TIMEZONE", zone)
    read = converter._TIMESTAMP_LTZ_to_python({"scale": 9})
    # -62135596800 s is 0001-01-01T00:00:00Z.
    got = read("-62135596800.000000000")
    assert got.utcoffset() is not None
    assert got - datetime(1, 1, 1, tzinfo=UTC) == timedelta(0)


def test_ltz_read_still_uses_the_session_zone_when_representable(converter):
    converter.set_parameter("TIMEZONE", "America/Los_Angeles")
    read = converter._TIMESTAMP_LTZ_to_python({"scale": 9})
    got = read("1704067200.000000000")  # 2024-01-01T00:00:00Z
    assert (got.year, got.month, got.day, got.hour) == (2023, 12, 31, 16)
