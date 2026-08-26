"""Iceberg Float/Double leftover keys refuse IEEE-lossy binds.

2**53+1 would delete 2**53 if coerced through float(). Locale money that
survives binary64 still binds. Auto grouping already refused upstream.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import _iceberg_float_carrier  # noqa: E402


def test_locale_money_that_fits_float_is_kept():
    assert _iceberg_float_carrier(Decimal("1234.56")) == 1234.56
    assert _iceberg_float_carrier(Decimal("10.00")) == 10.0


def test_mantissa_beyond_float_is_refused():
    with pytest.raises(ValueError, match="refused"):
        _iceberg_float_carrier(Decimal("9007199254740993"))
