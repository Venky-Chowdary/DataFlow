"""History mean-drift robust-z uses write-path Decimals, not float(mean).

Same locale-money load is not drift. A 10.00 → 2000.00 mean still warns.
Auto 1,234 never invents a mean to compare.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality_history import (  # noqa: E402
    ColumnProfile,
    detect_anomalies,
    profile_column,
)


def test_locale_money_stable_mean_is_not_drift():
    hist = profile_column(["$10.00", "$1,234.56", "€2.000,00"], "amount", "decimal")
    cur = profile_column(["$10.00", "$1,234.56", "€2.000,00"], "amount", "decimal")
    issues = detect_anomalies({"amount": cur}, {"amount": hist})
    assert not any("mean" in i.lower() for i in issues)


def test_locale_money_mean_shift_still_warns():
    historical = {
        "amount": ColumnProfile(
            column="amount",
            count=3,
            null_count=0,
            dtype="decimal",
            mean=Decimal("10.00"),
            std=Decimal("1.00"),
        )
    }
    current = {
        "amount": ColumnProfile(
            column="amount",
            count=3,
            null_count=0,
            dtype="decimal",
            mean=Decimal("2000.00"),
            std=Decimal("1.00"),
        )
    }
    issues = detect_anomalies(current, historical)
    assert any("mean" in i.lower() or "standard deviations" in i for i in issues)


def test_auto_grouping_does_not_invent_mean_drift():
    hist = profile_column(["1,234", "1.000", "1.234"], "amount", "decimal")
    cur = profile_column(["1,234", "1.000", "1.234"], "amount", "decimal")
    assert hist.mean is None
    assert cur.mean is None
    issues = detect_anomalies({"amount": cur}, {"amount": hist})
    assert not any("mean" in i.lower() for i in issues)
