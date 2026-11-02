"""CDC FLOAT watermarks compare write-path Decimals, not float(parsed).

Auto 1,234 cannot bind — compare falls through to string (not 1234 > 1.234).
Locale money and 2**53+1 still order correctly.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.cdc_engine import WatermarkType, compare_watermarks, max_watermark  # noqa: E402


def test_locale_money_orders_as_decimals():
    assert compare_watermarks("$1,234.56", "$10.00", WatermarkType.FLOAT) == 1
    assert compare_watermarks("$10.00", "€2.000,00", WatermarkType.FLOAT) == -1
    assert compare_watermarks("$1,234.56", "1234.56", WatermarkType.FLOAT) == 0
    assert max_watermark(["$10.00", "€2.000,00", "$1,234.56"], WatermarkType.FLOAT) == "€2.000,00"


def test_mantissa_beyond_float_still_advances():
    """2**53+1 and 2**53 collapse to the same IEEE float; Decimal still orders."""
    assert compare_watermarks("9007199254740993", "9007199254740992", WatermarkType.FLOAT) == 1
    assert compare_watermarks("9007199254740992", "9007199254740993", WatermarkType.FLOAT) == -1


def test_auto_grouping_does_not_invent_float_order():
    """1,234 vs 1.234 must not invent 1234 > 1.234."""
    assert compare_watermarks("1,234", "1.234", WatermarkType.FLOAT) == ("1,234" > "1.234") - ("1,234" < "1.234")
