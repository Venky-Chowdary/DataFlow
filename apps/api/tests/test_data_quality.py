"""Data-quality and anomaly-detection gate tests."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.data_quality import (  # noqa: E402
    BatchDriftDetector,
    _parse_iso_date,
    _to_float,
    run_integrity_audit,
)


def test_detects_duplicate_primary_keys():
    report = run_integrity_audit(
        headers=["id", "amount"],
        rows=[["1", "100"], ["2", "200"], ["1", "300"]],
        column_types={"id": "INTEGER", "amount": "DECIMAL"},
    )
    assert not report.passed
    assert "Duplicate primary key" in report.issues[0]


def test_detects_required_nulls():
    report = run_integrity_audit(
        headers=["id", "email"],
        rows=[["1", "a@b.com"], ["2", ""]],
        column_types={"id": "INTEGER", "email": "STRING"},
        required_targets=["email"],
    )
    assert not report.passed
    assert "email" in report.issues[0]


def test_parses_locale_and_currency_floats():
    assert _to_float("$1,234.56") == 1234.56
    assert _to_float("€1.234,56") == 1234.56
    assert _to_float("1 000 000.89") == 1000000.89
    assert _to_float("USD 1 000 000.89") == 1000000.89
    assert _to_float("not a number") is None


def test_parses_dayfirst_dates():
    assert _parse_iso_date("31/12/2024 14:30:00") is not None
    assert _parse_iso_date("2024-12-31T14:30:00Z") is not None
    assert _parse_iso_date("not a date") is None


def test_detects_precision_loss_for_amount_integer():
    report = run_integrity_audit(
        headers=["amount"],
        rows=[["1000.50"], ["2000.00"]],
        column_types={"amount": "INTEGER"},
        mappings=[{"source": "amount", "target": "amount"}],
    )
    assert not report.passed
    assert "precision loss" in report.issues[0]


def test_row_level_anomaly_count():
    rows = [[str(i), str(i * 10)] for i in range(1, 20)]
    rows.append(["99", "1000000"])  # extreme outlier
    report = run_integrity_audit(
        headers=["id", "value"],
        rows=rows,
        column_types={"id": "INTEGER", "value": "DECIMAL"},
    )
    assert report.stats["anomalous_rows"] >= 1
    assert any("|z-score| > 3" in w for w in report.warnings)


def test_batch_drift_detector_warns_on_mean_shift():
    detector = BatchDriftDetector(numeric_threshold=0.05)
    baseline = {
        "columns": {
            "value": {"null_rate": 0.0, "cardinality": 10, "mean": 100.0, "stdev": 10.0},
        }
    }
    current = {
        "columns": {
            "value": {"null_rate": 0.0, "cardinality": 10, "mean": 500.0, "stdev": 10.0},
        }
    }
    detector.update(baseline)
    warnings = detector.check(current)
    assert any("mean drift" in w for w in warnings)


def test_maximum_mode_escalates_warnings_to_blockers():
    # 9 zeros + one large value gives a z-score > 3 on the large value.
    rows = [["0"] for _ in range(9)] + [["1000"]]
    report = run_integrity_audit(
        headers=["value"],
        rows=rows,
        column_types={"value": "INTEGER"},
        validation_mode="maximum",
    )
    assert not report.passed
    assert not report.warnings
    assert any("outlier" in i or "z-score" in i for i in report.issues)
