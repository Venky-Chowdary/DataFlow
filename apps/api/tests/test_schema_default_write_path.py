"""DDL default literals compare dest-canonical Decimals, not float().

IEEE float(2**53+1) == float(2**53) invented a matching default.
1.0 and 1 still match. Auto 1,234 does not invent 1234.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.schema_fidelity import _default_exprs_equivalent  # noqa: E402


def test_scale_only_defaults_still_match():
    assert _default_exprs_equivalent("1.0", "1") is True
    assert _default_exprs_equivalent("10.00", "10") is True
    assert _default_exprs_equivalent("1.2345", "1.2345") is True


def test_mantissa_beyond_float_does_not_match():
    assert _default_exprs_equivalent("9007199254740993", "9007199254740993") is True
    assert _default_exprs_equivalent("9007199254740993", "9007199254740992") is False


def test_auto_grouping_does_not_invent_default_match():
    assert _default_exprs_equivalent("1,234", "1234") is False
    assert _default_exprs_equivalent("$1,234.56", "1234.56") is True
