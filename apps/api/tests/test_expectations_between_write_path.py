"""expect_column_values_between compares write-path Decimals, not IEEE float.

float(parsed) after a successful bind collapsed money scale. Auto 1,234
cannot bind — it is not_numeric, not invented 1234 inside the band.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.expectations_engine import expect_column_values_between  # noqa: E402


def test_locale_money_is_inside_band():
    rows = [{"amount": "$1,234.56"}, {"amount": "€1.234,56"}]
    r = expect_column_values_between(rows, "amount", min_value=1000, max_value=2000)
    assert r.passed is True
    assert r.failing_count == 0


def test_auto_ambiguous_grouping_is_not_numeric_not_invented():
    rows = [{"amount": "1,234"}, {"amount": "1.000"}]
    r = expect_column_values_between(rows, "amount", min_value=0, max_value=10)
    assert r.passed is False
    assert r.failing_count == 2
    assert all(f.get("reason") == "not_numeric" for f in r.failing_samples)
