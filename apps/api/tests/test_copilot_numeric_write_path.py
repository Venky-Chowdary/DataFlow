"""Pilot sample compare/stats use write-path Decimals, not float(dec).

Auto 1,234 cannot bind. Locale money still orders. 2**53+1 still > 2**53.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from src.ai.copilot.query_tools import _cmp, _numeric_stats, _try_float  # noqa: E402


def test_try_float_keeps_locale_money_decimal():
    assert _try_float("$1,234.56") == Decimal("1234.56")
    assert isinstance(_try_float("$1,234.56"), Decimal)
    assert _try_float("€2.000,00") == Decimal("2000.00")
    assert _try_float("1,234") is None
    assert _try_float("1.234") is None


def test_cmp_mantissa_beyond_float_still_orders():
    assert _cmp("9007199254740993", "9007199254740992", "gt") is True
    assert _cmp("9007199254740992", "9007199254740993", "gt") is False
    assert _cmp("$1,234.56", "$10.00", "gt") is True
    assert _cmp("$1,234.56", "1234.56", "eq") is True


def test_numeric_stats_locale_money_range():
    nums = [_try_float(v) for v in ("$10.00", "$1,234.56", "€2.000,00")]
    stats = _numeric_stats([n for n in nums if n is not None])
    assert stats["min"] == Decimal("10.00")
    assert stats["max"] == Decimal("2000.00")
    assert stats["mean"] == Decimal("1081.52")
