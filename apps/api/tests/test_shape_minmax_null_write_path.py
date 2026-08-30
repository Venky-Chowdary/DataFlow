"""Shape least / greatest skip is_blank, not only ``v is not None``.

Reader-wired SQL_NULL_SENTINEL used to enter the sort as a present
string, so least returned the sentinel token and greatest could rank
it against customer text. Empty / whitespace / NaN stay absent.
0 and False stay present.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_expr import compile_expression  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def value(source: str, row: dict | None = None):
    return compile_expression(source).evaluate(row or {})


def test_least_skips_reader_null():
    assert value(
        "least([a], [b], [c])",
        {"a": SQL_NULL_SENTINEL, "b": None, "c": "z"},
    ) == "z"
    assert value(
        "least([a], [b])",
        {"a": SQL_NULL_SENTINEL, "b": ""},
    ) is None
    assert value(
        "least([a], [b])",
        {"a": "   ", "b": DF_MISSING_SENTINEL},
    ) is None
    assert value("least([a])", {"a": Missing}) is None
    assert SQL_NULL_SENTINEL not in {
        value("least([a], 'z')", {"a": SQL_NULL_SENTINEL}),
    }


def test_greatest_skips_reader_null():
    assert value(
        "greatest([a], [b], [c])",
        {"a": SQL_NULL_SENTINEL, "b": "a", "c": None},
    ) == "a"
    assert value(
        "greatest([a], [b])",
        {"a": SQL_NULL_SENTINEL, "b": "   "},
    ) is None


def test_least_greatest_keep_zero_and_false():
    assert value("least([a], [b])", {"a": SQL_NULL_SENTINEL, "b": 0}) == 0
    assert value("greatest([a], [b])", {"a": None, "b": 0}) == 0
    assert value("least([a], [b])", {"a": False, "b": SQL_NULL_SENTINEL}) is False


def test_least_greatest_numeric_order():
    assert value("least(10, 3, [x])", {"x": SQL_NULL_SENTINEL}) == Decimal(3)
    assert value("greatest(10, 3, [x])", {"x": SQL_NULL_SENTINEL}) == Decimal(10)
    assert value("least([a], [b])", {"a": Decimal("1E+2"), "b": 99}) == 99


def test_least_does_not_invent_sentinel_as_smallest_text():
    """``__DF_SQL_NULL__`` sorts before customer letters if it enters the key."""
    got = value("least([a], 'z')", {"a": SQL_NULL_SENTINEL})
    assert got == "z"
    assert got != SQL_NULL_SENTINEL
