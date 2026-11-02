"""Profiler numeric stats use write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — parse rate stays 0 and min/max stay unset.
Locale money the write path stores still sets the range.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_profiler import profile_column  # noqa: E402


def test_locale_money_minmax_are_decimals():
    prof = profile_column("amount", ["$10.00", "$1,234.56", "€2.000,00"])
    stats = prof.get("statistics") or {}
    assert stats.get("min") == Decimal("10.00")
    assert stats.get("max") == Decimal("2000.00")
    assert stats.get("numeric_parse_rate") == 1.0


def test_auto_grouping_does_not_invent_stats():
    prof = profile_column("amount", ["1,234", "1.000", "1.234"])
    stats = prof.get("statistics") or {}
    assert stats.get("min") is None
    assert stats.get("max") is None
    assert (stats.get("numeric_parse_rate") or 0) == 0
