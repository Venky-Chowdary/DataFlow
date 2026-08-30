"""Gate-8 must not report a declared narrowing as a checksum mismatch.

A Snowflake ``TIMESTAMP_NTZ`` landing in MySQL ``DATETIME`` cannot keep its
fractional seconds: the engine rounds ``00:00:00.654321`` to ``00:00:01`` on
write. Fingerprinting the source cell at full precision against that read-back
made every row of the column differ, so a 710k-row append that conserved every
row failed with two opaque hashes and no column named.

The digests are therefore taken at the granularity the carrier keeps, on both
sides, and the report has to disclose which columns those were — a match there
proves every cell the destination can hold, not the digits it dropped.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytest

from services.carrier_instant import (
    carrier_instant_digits,
    carrier_rounded_columns,
    quantize_instant_for_carrier,
)
from services.reconciliation import fingerprint_for_reconcile

# (engine, dest DDL, source cell, value the engine stores) — the stored column is
# measured live in repro/carrier_precision_roundtrip.py.
CARRIER_ROUND_TRIPS = [
    ("mysql", "DATETIME", "2024-01-01 00:00:00.654321", datetime(2024, 1, 1, 0, 0, 1)),
    ("mysql", "TIMESTAMP", "2024-01-01 00:00:00.654321", datetime(2024, 1, 1, 0, 0, 1)),
    (
        "mysql",
        "DATETIME(3)",
        "2024-01-01 00:00:00.654321",
        datetime(2024, 1, 1, 0, 0, 0, 654000),
    ),
    (
        "mysql",
        "DATETIME(6)",
        "2024-01-01 00:00:00.654321",
        datetime(2024, 1, 1, 0, 0, 0, 654321),
    ),
    ("mysql", "TIME", "10:20:30.654321", timedelta(seconds=37231)),
    (
        "postgresql",
        "timestamp(0)",
        "2024-01-01 00:00:00.654321",
        datetime(2024, 1, 1, 0, 0, 1),
    ),
    (
        "postgresql",
        "timestamp(3)",
        "2024-01-01 00:00:00.654321",
        datetime(2024, 1, 1, 0, 0, 0, 654000),
    ),
    ("postgresql", "time(0)", "10:20:30.654321", time(10, 20, 31)),
    (
        "sqlserver",
        "datetime2(2)",
        "2024-01-01 00:00:00.654321",
        datetime(2024, 1, 1, 0, 0, 0, 650000),
    ),
]


@pytest.mark.parametrize("engine,ddl,source_cell,stored", CARRIER_ROUND_TRIPS)
def test_source_and_destination_fingerprint_alike(
    engine: str, ddl: str, source_cell: str, stored: object
) -> None:
    assert fingerprint_for_reconcile(
        source_cell, ddl_type=ddl, engine=engine
    ) == fingerprint_for_reconcile(stored, ddl_type=ddl, engine=engine)


def test_carrier_granularity_needs_the_engine() -> None:
    # ``DATETIME`` keeps whole seconds on MySQL and microseconds on BigQuery, so
    # quantizing without knowing the engine would drop digits a destination can
    # hold.
    assert carrier_instant_digits("DATETIME", engine="mysql") == 0
    assert carrier_instant_digits("DATETIME", engine="bigquery") == 6
    assert carrier_instant_digits("DATETIME", engine="") is None
    assert carrier_instant_digits("VARCHAR(32)", engine="mysql") is None


def test_unknown_carrier_leaves_the_value_alone() -> None:
    value = datetime(2024, 1, 1, 0, 0, 0, 654321)
    assert quantize_instant_for_carrier(value, ddl_type="", engine="mysql") is value
    assert quantize_instant_for_carrier(value, ddl_type="DATETIME", engine="") is value
    assert (
        quantize_instant_for_carrier(value, ddl_type="VARCHAR(32)", engine="mysql")
        is value
    )


def test_a_real_difference_still_fails_gate8() -> None:
    # Rounding must not equate two different instants: only digits the carrier
    # cannot store are removed.
    left = fingerprint_for_reconcile(
        "2024-01-01 00:00:01.000000", ddl_type="DATETIME", engine="mysql"
    )
    right = fingerprint_for_reconcile(
        "2024-01-01 00:00:02.000000", ddl_type="DATETIME", engine="mysql"
    )
    assert left != right


def test_second_carry_does_not_wrap_a_time_column() -> None:
    # A TIME column has no next day to carry into; keep the second it can store.
    assert quantize_instant_for_carrier(
        time(23, 59, 59, 900000), ddl_type="TIME", engine="mysql"
    ) == time(23, 59, 59)
    assert quantize_instant_for_carrier(
        time(10, 20, 30, 654321), ddl_type="TIME", engine="mysql"
    ) == time(10, 20, 31)


def test_sqlserver_legacy_datetime_uses_one_three_hundredth_ticks() -> None:
    # SQL Server ``datetime`` stores 1/300 s: .001 → .000, .002 → .003.
    assert quantize_instant_for_carrier(
        datetime(2024, 1, 1, 0, 0, 0, 1000), ddl_type="datetime", engine="sqlserver"
    ) == datetime(2024, 1, 1, 0, 0, 0, 0)
    assert quantize_instant_for_carrier(
        datetime(2024, 1, 1, 0, 0, 0, 2000), ddl_type="datetime", engine="sqlserver"
    ) == datetime(2024, 1, 1, 0, 0, 0, 3333)


def test_smalldatetime_rounds_to_the_minute() -> None:
    assert quantize_instant_for_carrier(
        datetime(2024, 1, 1, 0, 0, 45), ddl_type="smalldatetime", engine="sqlserver"
    ) == datetime(2024, 1, 1, 0, 1)


def test_date_columns_are_untouched() -> None:
    assert quantize_instant_for_carrier(
        date(2024, 1, 1), ddl_type="DATE", engine="mysql"
    ) == date(2024, 1, 1)


def test_rounded_columns_are_named_for_the_report() -> None:
    rounded = carrier_rounded_columns(
        [
            {"source": "order_ts", "target": "order_ts", "source_type": "TIMESTAMP_NTZ"},
            {"source": "paid_ts", "target": "paid_ts", "source_type": "TIMESTAMP_NTZ(6)"},
            {"source": "name", "target": "name", "source_type": "VARCHAR(64)"},
        ],
        dest_types={
            "order_ts": "DATETIME",
            "paid_ts": "DATETIME(6)",
            "name": "VARCHAR(64)",
        },
        dest_engine="mysql",
    )
    assert [c["column"] for c in rounded] == ["order_ts"]
    assert rounded[0]["kept_fractional_digits"] == 0
