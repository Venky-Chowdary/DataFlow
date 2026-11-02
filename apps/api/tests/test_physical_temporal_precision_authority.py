"""The carrier Gate-8 hashes against is the physical column, not Map's intent.

A destination that already exists contradicts the plan: Studio declared
``TIMESTAMP_NTZ`` for a MySQL column that is physically ``datetime`` (whole
seconds). Reading the plan's precision made reconcile hash microseconds the
column never stored — a strict checksum mismatch on a correct load, reported as
two opaque hashes. Precision is therefore taken from the catalog, and only the
precision: substituting the catalog's spelling wholesale dropped the timezone
polarity and made the digests disagree by a UTC offset instead.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.type_system import (  # noqa: E402
    destination_temporal_fractional_digits,
    temporal_precision_would_narrow,
    with_temporal_fractional_digits,
)


def test_logical_ntz_spelling_resolves_per_engine() -> None:
    # Introspection reports MySQL ``datetime`` as this logical carrier.
    assert destination_temporal_fractional_digits("TIMESTAMP_NTZ", dest_db="mysql") == 0
    assert (
        destination_temporal_fractional_digits("TIMESTAMP_NTZ", dest_db="postgresql")
        == 6
    )
    assert (
        destination_temporal_fractional_digits("TIMESTAMP_NTZ", dest_db="snowflake") == 9
    )
    assert (
        destination_temporal_fractional_digits("TIMESTAMP_NTZ", dest_db="sqlserver") == 7
    )


def test_declared_precision_still_wins_over_the_engine_default() -> None:
    assert (
        destination_temporal_fractional_digits("TIMESTAMP_NTZ(3)", dest_db="mysql") == 3
    )
    assert destination_temporal_fractional_digits("DATETIME(6)", dest_db="mysql") == 6


def test_bare_timestamp_stays_fail_closed_for_an_unnamed_engine() -> None:
    # Claiming microseconds here would hide a narrowing on MySQL.
    assert destination_temporal_fractional_digits("TIMESTAMP") == 0
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP") is True


def test_mysql_datetime_narrowing_is_declared_from_the_logical_spelling() -> None:
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ", dest_db="mysql"
        )
        is True
    )
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ", dest_db="postgresql"
        )
        is False
    )


def test_mssql_legacy_datetime_keeps_three_digits() -> None:
    assert destination_temporal_fractional_digits("DATETIME", dest_db="sqlserver") == 3
    assert destination_temporal_fractional_digits("DATETIME", dest_db="mysql") == 0
    assert destination_temporal_fractional_digits("DATETIME", dest_db="bigquery") == 6


def test_restating_precision_preserves_timezone_polarity() -> None:
    assert with_temporal_fractional_digits("TIMESTAMPTZ", 0) == "TIMESTAMPTZ(0)"
    assert with_temporal_fractional_digits("TIMESTAMP_NTZ(6)", 0) == "TIMESTAMP_NTZ(0)"
    assert (
        with_temporal_fractional_digits("TIMESTAMP(3) WITH TIME ZONE", 0)
        == "TIMESTAMP(0) WITH TIME ZONE"
    )
    # No fractional second to restate, and non-temporal DDL is untouched.
    assert with_temporal_fractional_digits("DATE", 0) == "DATE"
    assert with_temporal_fractional_digits("DECIMAL(18,4)", 0) == "DECIMAL(18,4)"


def test_physical_probe_returns_nothing_when_the_catalog_cannot_answer() -> None:
    from services.dest_physical_types import physical_temporal_digits

    # Unreachable host: an invented type here would silently redefine the
    # comparison basis, so the caller must keep the declared types.
    assert (
        physical_temporal_digits(
            "mysql",
            {"host": "127.0.0.1", "port": 1, "database": "nope", "username": "nope"},
            table="orders",
        )
        == {}
    )
