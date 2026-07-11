"""Tests for full-file CSV validation."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.csv_validator import validate_csv_content  # noqa: E402


def test_validate_csv_detects_type_mismatch():
    content = b"id,amount\n1,not_a_number\n2,50.00\n"
    report = validate_csv_content(
        content,
        ["id", "amount"],
        {"id": "INTEGER", "amount": "DECIMAL"},
    )
    assert report["rows_scanned"] == 2
    assert report["issue_count"] >= 1
    assert any("amount" in i for i in report["issues"])


def test_validate_csv_clean_file():
    content = b"id,amount\n1,10.50\n2,20.00\n"
    report = validate_csv_content(
        content,
        ["id", "amount"],
        {"id": "INTEGER", "amount": "DECIMAL"},
    )
    assert report["ok"] is True
    assert report["issue_count"] == 0
