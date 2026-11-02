"""to_number is write-path only — no Decimal(text) fallback.

allow_grouping=False used to Decimal(text) Auto 1.234 after the wire
refused. That branch had no callers. Money the write path stores still
binds. Typed Decimal dest-canonical 1.234 stays identity.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_expr import EvalError, compile_expression  # noqa: E402


def _value(source: str, row: dict | None = None):
    return compile_expression(source).evaluate(row or {})


def test_to_number_auto_three_digit_group_refuses():
    for token in ("1.234", "1,234", "1.000", "1.005"):
        with pytest.raises(EvalError, match="ambiguous number grouping"):
            _value("to_number([x])", {"x": token})


def test_to_number_locale_money_and_bindable_scale():
    assert _value("to_number('$1,234')") == Decimal("1234")
    assert _value("to_number('€1.234')") == Decimal("1234")
    assert _value("to_number('$1,000')") == Decimal("1000")
    assert _value("to_number('(1,234.50)')") == Decimal("-1234.50")
    assert _value("to_number('1.2345')") == Decimal("1.2345")
    assert _value("to_number('1.2300')") == Decimal("1.2300")
    assert _value("to_number('')") is None


def test_to_number_typed_decimal_stays_dest_canonical():
    assert _value("to_number([x])", {"x": Decimal("1.234")}) == Decimal("1.234")
    assert _value("to_number([x])", {"x": 42}) == Decimal("42")


def test_boolean_is_not_a_magnitude():
    with pytest.raises(EvalError, match="boolean is not a number"):
        _value("to_number([x])", {"x": True})
    with pytest.raises(EvalError, match="boolean is not a number"):
        _value("[x] + 1", {"x": False})
    with pytest.raises(EvalError, match="not a number"):
        _value("to_number('true')")
