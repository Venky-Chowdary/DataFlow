"""Distribution drift histograms write-path Decimals, not IEEE float(parsed).

Auto 1,234 cannot bind — stay categorical on the raw token. Locale money
the write path stores must enter the numeric histogram as the same
magnitude on both sides.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.expectations_engine import expect_column_distribution_drift  # noqa: E402


def test_locale_money_same_magnitude_uses_numeric_histogram():
    baseline = [{"amount": f"${n:,}.00"} for n in range(1000, 1008)]
    current = [{"amount": f"€{n // 1000}.{n % 1000:03d},00"} for n in range(1000, 1008)]
    r = expect_column_distribution_drift(current, baseline, "amount", threshold=0.25)
    assert r.details.get("method") == "js_histogram"
    assert r.passed is True
    assert r.details.get("drift_score", 1) <= 0.01


def test_auto_grouping_stays_categorical_not_invented_thousands():
    baseline = [{"amount": "1,234"}] * 8
    current = [{"amount": "1,234"}] * 8
    r = expect_column_distribution_drift(current, baseline, "amount", threshold=0.25)
    assert r.details.get("method") == "categorical"
    assert r.passed is True
