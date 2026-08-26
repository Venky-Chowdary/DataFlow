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

from services.sample_quality import analyze_column_quality  # noqa: E402


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
