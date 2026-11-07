"""Batch drift compare uses Decimals, not float(cur) - float(base).

Integrity stores write-path Decimal means. IEEE subtract invents a second
magnitude on scale-20 money and collapses 2**53+1 against 2**53+3.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality import BatchDriftDetector  # noqa: E402


def test_same_decimal_money_is_not_drift():
    detector = BatchDriftDetector(numeric_threshold=0.05)
    money = Decimal("12345678901234567890.12")
    baseline = {"columns": {"amount": {"mean": money, "stdev": Decimal("1.00")}}}
    current = {"columns": {"amount": {"mean": money, "stdev": Decimal("1.00")}}}
    detector.update(baseline)
    warnings = detector.check(current)
    assert not any("drift" in w for w in warnings)


def test_decimal_mean_shift_still_warns():
    detector = BatchDriftDetector(numeric_threshold=0.05)
    baseline = {
        "columns": {
            "amount": {"mean": Decimal("10.00"), "stdev": Decimal("1.00")},
        }
    }
    current = {
        "columns": {
            "amount": {"mean": Decimal("2000.00"), "stdev": Decimal("1.00")},
        }
    }
    detector.update(baseline)
    warnings = detector.check(current)
    assert any("mean drift" in w for w in warnings)
