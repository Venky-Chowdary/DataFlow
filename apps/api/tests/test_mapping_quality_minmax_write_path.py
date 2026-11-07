"""Map column min/max use write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — it must not become min/max 1234. Locale money
the write path stores still sets the observed range.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mapping_quality import analyze_column_profile  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_locale_money_minmax_are_decimals():
    profile = analyze_column_profile(
        "amount",
        ["$10.00", "$1,234.56", "€2.000,00"],
    )
    assert profile.get("min") == Decimal("10.00")
    assert profile.get("max") == Decimal("2000.00")


def test_auto_grouping_does_not_invent_minmax():
    profile = analyze_column_profile("amount", ["1,234", "1.000", "1.234"])
    assert profile.get("min") is None
    assert profile.get("max") is None


def test_reader_null_is_absence_not_a_token():
    profile = analyze_column_profile(
        "note",
        [SQL_NULL_SENTINEL, "", None, "kept", "   "],
    )
    assert profile["non_empty_count"] == 1
    assert profile["null_rate"] == 0.8
    assert profile["samples"] == ["kept"]
