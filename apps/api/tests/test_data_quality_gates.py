"""Data-quality / anomaly-detection gate escalation."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.data_quality import BatchDriftDetector, run_integrity_audit  # noqa: E402


def test_audit_passes_clean_sample():
    headers = ["id", "amount"]
    rows = [
        ["1", "100.00"],
        ["2", "200.00"],
        ["3", "300.00"],
    ]
    report = run_integrity_audit(headers, rows, validation_mode="strict")
    assert report.passed is True
    assert not report.issues


def test_maximum_mode_blocks_duplicate_primary_keys():
    headers = ["id", "amount"]
    rows = [
        ["1", "100.00"],
        ["2", "100.00"],
        ["1", "200.00"],
    ]
    report = run_integrity_audit(headers, rows, validation_mode="maximum")
    assert report.passed is False
    assert any("duplicate primary key" in msg.lower() for msg in report.issues)


def test_strict_mode_warns_pii_in_unprotected_column():
    headers = ["id", "email"]
    rows = [
        ["1", "user@example.com"],
        ["2", "other@example.com"],
    ]
    report = run_integrity_audit(headers, rows, validation_mode="strict")
    assert report.passed is True
    assert any("pii" in msg.lower() for msg in report.warnings)


def test_drift_detector_catches_schema_drift():
    detector = BatchDriftDetector()
    base = {
        "total_rows": 100,
        "columns": {
            "amount": {"null_rate": 0.0, "cardinality": 50, "mean": 100.0, "stdev": 10.0},
        },
    }
    later = {
        "total_rows": 100,
        "columns": {
            "amount": {"null_rate": 0.0, "cardinality": 50, "mean": 100.0, "stdev": 10.0},
            "new_col": {"null_rate": 0.0, "cardinality": 10},
        },
    }
    assert detector.check(base) == []
    warnings = detector.check(later)
    assert any("new column" in w.lower() for w in warnings)


def test_drift_detector_catches_row_count_drift():
    detector = BatchDriftDetector()
    base = {"total_rows": 1000, "columns": {}}
    later = {"total_rows": 100, "columns": {}}
    detector.check(base)
    warnings = detector.check(later)
    assert any("row count drift" in w.lower() for w in warnings)


def test_drift_detector_catches_range_drift():
    detector = BatchDriftDetector(numeric_threshold=0.10)
    base = {
        "total_rows": 100,
        "columns": {
            "amount": {"null_rate": 0.0, "cardinality": 50, "mean": 100.0, "stdev": 10.0, "min": 10.0, "max": 500.0},
        },
    }
    later = {
        "total_rows": 100,
        "columns": {
            "amount": {"null_rate": 0.0, "cardinality": 50, "mean": 100.0, "stdev": 10.0, "min": 10.0, "max": 2000.0},
        },
    }
    detector.check(base)
    warnings = detector.check(later)
    assert any("max drift" in w.lower() for w in warnings)
