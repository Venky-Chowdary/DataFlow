"""Tests for full-file CSV validation."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.csv_validator import validate_csv_content  # noqa: E402


def test_validate_csv_handles_quoted_commas():
    content = b'id,amount\n1,"1,234.56"\n2,"5,000.00"\n'
    report = validate_csv_content(
        content,
        ["id", "amount"],
        {"id": "INTEGER", "amount": "DECIMAL"},
    )
    assert report["ok"] is True
    assert report.get("parser") == "csv_stdlib"


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


def test_validate_csv_canonical_boolean_passes():
    content = b"id,flag\n1,true\n2,false\n3,1\n4,0\n"
    report = validate_csv_content(
        content,
        ["id", "flag"],
        {"id": "INTEGER", "flag": "BOOLEAN"},
    )
    assert report["ok"] is True
    assert report["issue_count"] == 0


def test_validate_csv_informal_yes_fails_boolean_schema():
    """Write path refuses yes — CSV validate must not green-light BOOLEAN dest."""
    content = b"id,flag\n1,yes\n2,no\n"
    report = validate_csv_content(
        content,
        ["id", "flag"],
        {"id": "INTEGER", "flag": "BOOLEAN"},
    )
    assert report["ok"] is False
    assert any("expected boolean" in i and "yes" in i for i in report["issues"])
