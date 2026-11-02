"""Data-quality IQR uses write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — it is not an invented 1234 outlier among 10–19.
Locale money the write path stores still enters the fence.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality import run_integrity_audit  # noqa: E402


def test_auto_grouping_is_not_iqr_outlier():
    rows = [[str(i)] for i in range(10, 20)] + [["1,234"]]
    report = run_integrity_audit(
        headers=["amount"],
        rows=rows,
        column_types={"amount": "DECIMAL"},
    )
    blob = " ".join(report.warnings + report.issues).lower()
    assert "outlier" not in blob


def test_locale_money_outlier_still_warns():
    rows = [["$10.00"] for _ in range(8)] + [["$1,000,000.00"]]
    report = run_integrity_audit(
        headers=["amount"],
        rows=rows,
        column_types={"amount": "DECIMAL"},
    )
    blob = " ".join(report.warnings + report.issues).lower()
    assert "outlier" in blob or "z-score" in blob
