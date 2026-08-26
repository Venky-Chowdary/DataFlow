"""Sample-quality IQR uses write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — it is a non-numeric parse fail, not an IQR
outlier of 1234. Locale money the write path stores still counts.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.sample_quality import (  # noqa: E402
    _sample_wire,
    analyze_column_quality,
    analyze_dataset_quality,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_auto_grouping_is_parse_fail_not_iqr_outlier():
    values = ["10", "11", "12", "10", "11", "1,234", "9", "10", "11", "12"]
    report = analyze_column_quality("amount", values, inferred_type="DECIMAL")
    issues = " ".join(report.get("issues") or []).lower()
    assert "non-numeric" in issues
    assert "outlier" not in issues


def test_locale_money_binds_for_iqr():
    values = [
        "$10.00",
        "$11.00",
        "$12.00",
        "$10.00",
        "$11.00",
        "$9.00",
        "$10.00",
        "$11.00",
    ]
    report = analyze_column_quality("amount", values, inferred_type="DECIMAL")
    issues = " ".join(report.get("issues") or []).lower()
    assert "non-numeric" not in issues


def test_sample_wire_collapses_reader_null():
    assert _sample_wire(None) == ""
    assert _sample_wire("") == ""
    assert _sample_wire("   ") == ""
    assert _sample_wire(SQL_NULL_SENTINEL) == ""
    assert _sample_wire("kept") == "kept"
    assert _sample_wire(0) == "0"


def test_reader_null_is_absence_not_a_token():
    report = analyze_column_quality(
        "note",
        [SQL_NULL_SENTINEL, "", None, "kept"],
        inferred_type="VARCHAR",
    )
    assert report["null_rate"] == 0.75
    assert report["distinct_count"] == 1


def test_dataset_null_and_duplicate_share_one_absence_wire():
    rows = [
        {"id": "1", "note": None},
        {"id": "1", "note": SQL_NULL_SENTINEL},
        {"id": "2", "note": "kept"},
    ]
    result = analyze_dataset_quality(
        ["id", "note"],
        rows,
        schema={"id": "INTEGER", "note": "VARCHAR"},
    )
    note = next(c for c in result["columns"] if c["column"] == "note")
    assert note["null_rate"] == 0.667
    assert result["duplicate_row_count"] == 1
