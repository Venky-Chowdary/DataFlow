"""A computed NULL is NULL. It must not fail the row."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.shape_expr import EvalError, compile_expression


def value(source: str, row: dict):
    return compile_expression(source).evaluate(row)


def test_multiply_null_and_blank_yield_null():
    assert value("[amount] * 2", {"amount": None}) is None
    assert value("[amount] * 2", {"amount": ""}) is None
    assert value("-[amount]", {"amount": None}) is None
    assert value("round([amount], 2)", {"amount": None}) is None


def test_present_number_still_computes():
    from decimal import Decimal

    assert value("[amount] * 2", {"amount": 2}) == Decimal(4)


def test_non_numeric_text_still_refuses():
    with pytest.raises(EvalError, match="not a number"):
        value("[amount] * 2", {"amount": "n/a"})
