"""Unit tests for data-quality and anomaly-detection gates."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.data_quality import run_integrity_audit  # noqa: E402


def test_duplicate_primary_key_is_blocked():
    headers = ["id", "amount"]
    rows = [
        ["1", "100.00"],
        ["2", "200.00"],
        ["1", "300.00"],  # duplicate
    ]
    report = run_integrity_audit(headers, rows, primary_key="id")
    assert not report.passed
    assert any("Duplicate primary key" in issue for issue in report.issues)


def test_required_column_null_is_blocked():
    headers = ["id", "email"]
    rows = [
        ["1", "alice@example.com"],
        ["2", ""],  # required target 'email' cannot be null
    ]
    report = run_integrity_audit(headers, rows, required_targets=["email"])
    assert not report.passed
    assert any("null/empty" in issue for issue in report.issues)


def test_fractional_financial_to_integer_is_blocked():
    headers = ["id", "amount"]
    rows = [
        ["1", "100.00"],
        ["2", "10.50"],
    ]
    report = run_integrity_audit(
        headers,
        rows,
        column_types={"amount": "INTEGER"},
        mappings=[{"source": "amount", "target": "amount"}],
    )
    assert not report.passed
    assert any("fractional" in issue for issue in report.issues)


def test_future_date_warns_in_strict_mode():
    headers = ["id", "created_at"]
    rows = [
        ["1", "2099-01-01"],
    ]
    report = run_integrity_audit(
        headers,
        rows,
        column_types={"created_at": "TIMESTAMP"},
        validation_mode="strict",
    )
    assert report.passed
    assert any("future" in warning.lower() for warning in report.warnings)


def test_maximum_mode_turns_warnings_into_blockers():
    headers = ["id", "created_at"]
    rows = [
        ["1", "2099-01-01"],
    ]
    report = run_integrity_audit(
        headers,
        rows,
        column_types={"created_at": "TIMESTAMP"},
        validation_mode="maximum",
    )
    assert not report.passed
    assert any("future" in issue.lower() for issue in report.issues)


def test_null_rate_spike_warns():
    headers = ["id", "amount"]
    rows = [[str(i), "" if i < 9 else str(i)] for i in range(10)]
    report = run_integrity_audit(headers, rows, validation_mode="strict")
    assert report.passed
    assert any("null" in warning.lower() for warning in report.warnings)


def test_pii_in_unprotected_column_warns():
    headers = ["id", "note"]
    rows = [
        ["1", "Contact alice@example.com for details"],
    ]
    report = run_integrity_audit(headers, rows, validation_mode="strict")
    assert report.passed
    assert any("PII" in warning for warning in report.warnings)


def test_clean_batch_passes_all_checks():
    headers = ["id", "amount", "created_at"]
    rows = [
        ["1", "100.00", "2024-01-01"],
        ["2", "200.50", "2024-01-02"],
    ]
    report = run_integrity_audit(
        headers,
        rows,
        column_types={"id": "INTEGER", "amount": "DECIMAL", "created_at": "DATE"},
        primary_key="id",
        validation_mode="maximum",
    )
    assert report.passed
    assert report.checks_failed == 0
