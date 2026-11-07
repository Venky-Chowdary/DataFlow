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


def test_sample_wire_keeps_null_and_empty_string_distinct():
    """QA T19: one NULL wire spelling; '' stays a stored value."""
    assert _sample_wire(None) == SQL_NULL_SENTINEL
    assert _sample_wire(SQL_NULL_SENTINEL) == SQL_NULL_SENTINEL
    assert _sample_wire("") == ""
    assert _sample_wire("kept") == "kept"
    assert _sample_wire(0) == "0"


def test_reader_null_is_absence_and_text_empty_is_data():
    report = analyze_column_quality(
        "note",
        [_sample_wire(v) for v in (SQL_NULL_SENTINEL, "", None, "kept")],
        inferred_type="VARCHAR",
    )
    # Two NULLs of four; '' on a text carrier is stored data, not a null.
    assert report["null_rate"] == 0.5
    assert report["distinct_count"] == 1
    assert any("empty string" in i for i in report["issues"])


def test_numeric_blank_counts_as_absent():
    """On numeric carriers a blank wire cell is written as NULL."""
    report = analyze_column_quality(
        "amount", [_sample_wire(v) for v in ("", None, "1.5", "2.5")],
        inferred_type="DECIMAL",
    )
    assert report["null_rate"] == 0.5


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
