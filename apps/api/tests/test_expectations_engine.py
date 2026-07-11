"""Tests for expectations engine — dbt/GX standard validation contracts."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.expectations_engine import (  # noqa: E402
    expect_column_distribution_drift,
    expect_column_not_null,
    expect_column_unique,
    expect_column_values_between,
    run_auto_expectations,
    run_expectation_suite,
)


def _rows(*dicts):
    return list(dicts)


def test_expect_unique_fails_on_duplicates():
    rows = _rows({"id": "1"}, {"id": "1"}, {"id": "2"})
    r = expect_column_unique(rows, "id")
    assert not r.passed
    assert r.failing_count == 1


def test_expect_not_null_blocks_empty_ids():
    rows = _rows({"id": ""}, {"id": "2"}, {"id": None})
    r = expect_column_not_null(rows, "id", max_null_rate=0.0)
    assert not r.passed
    assert r.failing_count == 2


def test_expect_between_catches_out_of_range():
    rows = _rows({"amount": "100"}, {"amount": "999999999"}, {"amount": "50"})
    r = expect_column_values_between(rows, "amount", min_value=0, max_value=10000)
    assert not r.passed


def test_distribution_drift_detects_shift():
    baseline = _rows({"amount": "100"}, {"amount": "110"}, {"amount": "105"}, {"amount": "95"})
    current = _rows({"amount": "500"}, {"amount": "600"}, {"amount": "550"}, {"amount": "480"})
    r = expect_column_distribution_drift(current, baseline, "amount", threshold=0.15)
    assert not r.passed
    assert r.details.get("drift_score", 0) > 0.15


def test_auto_expectations_on_financial_schema():
    rows = _rows(
        {"order_id": "1", "amount": "100.00", "status": "paid"},
        {"order_id": "2", "amount": "200.00", "status": "paid"},
        {"order_id": "3", "amount": "150.00", "status": "pending"},
    )
    result = run_auto_expectations(
        rows,
        ["order_id", "amount", "status"],
        {"order_id": "VARCHAR", "amount": "DECIMAL", "status": "VARCHAR"},
        primary_key="order_id",
    )
    assert result["expectations_run"] >= 3
    assert result["passed"] is True


def test_suite_blocks_on_unique_violation():
    rows = _rows({"id": "A"}, {"id": "A"}, {"id": "B"})
    result = run_expectation_suite(rows, [
        {"fn": "expect_column_unique", "column": "id", "severity": "block"},
    ])
    assert result["blocks_transfer"] is True
    assert len(result["blocking_failures"]) == 1
