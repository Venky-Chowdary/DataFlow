"""BigQuery INTERVAL ISO seconds use Decimal identity, not float()+timedelta.

float(seconds) rounded long fractions. timedelta(seconds=2**53+1) OverflowError
instead of a bindable Y-M D H:M:S wire. ISO PT1.234S is 1.234 seconds (dot is
the ISO decimal) — not Auto grouping.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sql_bind import coerce_interval_wire  # noqa: E402
from services.value_serializer import format_bigquery_interval  # noqa: E402


def test_plain_iso_and_timedelta_still_bind():
    assert format_bigquery_interval(timedelta(seconds=1)) == "0-0 0 0:00:01"
    assert (
        format_bigquery_interval(timedelta(days=1, hours=2, minutes=3, seconds=4))
        == "0-0 1 2:03:04"
    )
    assert format_bigquery_interval("P1DT15M") == "0-0 1 0:15:00"
    assert format_bigquery_interval("01:02:03") == "0-0 0 1:02:03"
    assert format_bigquery_interval("PT90S") == "0-0 0 0:01:30"


def test_iso_fractional_seconds_stay_identity():
    # ISO decimal point — not Auto grouping. 1.234 seconds stays 1.234.
    assert format_bigquery_interval("PT1.5S") == "0-0 0 0:00:01.5"
    assert format_bigquery_interval("PT1.234S") == "0-0 0 0:00:01.234"
    assert format_bigquery_interval("PT1.2345S") == "0-0 0 0:00:01.2345"
    assert (
        format_bigquery_interval("PT1.234567890123456789S")
        == "0-0 0 0:00:01.234567890123456789"
    )


def test_auto_grouping_is_not_iso_seconds():
    # Lone grouping text is not an ISO duration — returned for quarantine.
    assert format_bigquery_interval("1,234") == "1,234"
    assert format_bigquery_interval("$1.50") == "$1.50"


def test_ieee_lossy_iso_seconds_stay_exact():
    # 9007199254740993s = 104249991374d 7h 36m 33s. float() collapsed; timedelta overflowed.
    assert (
        format_bigquery_interval("PT9007199254740993S")
        == "0-0 104249991374 7:36:33"
    )
    assert (
        coerce_interval_wire(
            "PT9007199254740993S",
            ddl_type="INTERVAL",
            engine="bigquery",
        )
        == "0-0 104249991374 7:36:33"
    )
