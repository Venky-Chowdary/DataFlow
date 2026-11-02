"""Integrity dates use write-path DATE / DATETIME polarity.

apply_transform(str(value), datetime) invented a calendar from DATE
epoch seconds. Auto 01/02/2024 still refuses. YYYYMMDD still binds.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality import (  # noqa: E402
    _parse_iso_date,
    _temporal_kind,
    run_integrity_audit,
)

EPOCH_S = "1704067200"  # 2024-01-01T00:00:00Z
FUTURE_EPOCH_S = "4102444800"  # 2100-01-01T00:00:00Z


def test_temporal_kind_follows_write_path():
    assert _temporal_kind("DATE") == "date"
    assert _temporal_kind("TIMESTAMP") == "datetime"
    assert _temporal_kind("DATETIME") == "datetime"
    assert _temporal_kind("TIMESTAMPTZ") == "datetime"
    assert _temporal_kind("TIMESTAMP WITHOUT TIME ZONE") == "datetime"
    assert _temporal_kind("") == "date"


def test_date_refuses_epoch_datetime_binds():
    assert _parse_iso_date(EPOCH_S, temporal="date") is None
    instant = _parse_iso_date(EPOCH_S, temporal="datetime")
    assert instant is not None
    assert instant.year == 2024
    assert instant.month == 1
    assert instant.day == 1


def test_date_keeps_yyyymmdd_and_refuses_ambiguous():
    assert _parse_iso_date("20240102", temporal="date") == datetime(2024, 1, 2)
    assert _parse_iso_date("01/02/2024", temporal="date") is None
    assert _parse_iso_date("01/02/2024", temporal="datetime") is None
    assert _parse_iso_date("31/12/2024 14:30:00", temporal="datetime") is not None


def test_native_temporal_is_identity():
    dt = datetime(2024, 1, 2, 3, 4, 5)
    assert _parse_iso_date(dt, temporal="datetime") is dt
    assert _parse_iso_date(date(2024, 1, 2), temporal="date") == datetime(2024, 1, 2)


def test_date_column_epoch_is_not_a_future_warn():
    report = run_integrity_audit(
        headers=["id", "created"],
        rows=[["1", EPOCH_S], ["2", FUTURE_EPOCH_S]],
        column_types={"id": "INTEGER", "created": "DATE"},
        validation_mode="strict",
    )
    assert report.passed
    assert not any("future" in w.lower() for w in report.warnings)


def test_timestamp_epoch_future_still_warns():
    report = run_integrity_audit(
        headers=["id", "created_at"],
        rows=[["1", FUTURE_EPOCH_S]],
        column_types={"id": "INTEGER", "created_at": "TIMESTAMP"},
        validation_mode="strict",
    )
    assert report.passed
    assert any("future" in w.lower() for w in report.warnings)


def test_name_heuristic_created_does_not_invent_epoch_calendar():
    report = run_integrity_audit(
        headers=["id", "created"],
        rows=[["1", EPOCH_S]],
        column_types={"id": "INTEGER", "created": "VARCHAR"},
        validation_mode="strict",
    )
    assert report.passed
    assert not any("future" in w.lower() for w in report.warnings)
