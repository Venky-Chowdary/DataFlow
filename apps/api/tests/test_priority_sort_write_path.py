"""Priority sort uses decimal_wire_value, not float().

stdlib float('1.234') invented 1.234 so Auto grouping sorted as a
milli-scale decimal. IEEE-exact 1.5 and integer strings still sort
numerically. Unambiguous 1.2345 binds. Empty values stay last.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.transform_engine import set_active_number_locale  # noqa: E402
from src.transfer.engine import (  # noqa: E402
    _apply_priority_and_limit,
    _coalesce_sort_value,
)

LONG = "1.234567890123456789"


def setup_function(_fn=None):
    set_active_number_locale("")


def test_coalesce_refuses_auto_ambiguous_grouping():
    assert _coalesce_sort_value("1.234") == (1, "1.234")
    assert _coalesce_sort_value("1,234") == (1, "1,234")
    assert _coalesce_sort_value("1.000") == (1, "1.000")


def test_coalesce_binds_unambiguous_and_leading_zero():
    assert _coalesce_sort_value("1.2345") == (0, Decimal("1.2345"))
    assert _coalesce_sort_value("0.025") == (0, Decimal("0.025"))
    assert _coalesce_sort_value("100") == (0, Decimal("100"))
    assert _coalesce_sort_value(1.5) == (0, 1.5)
    assert _coalesce_sort_value(LONG) == (0, Decimal(LONG))


def test_coalesce_empty_sorts_last():
    assert _coalesce_sort_value(None) == (1, "")
    assert _coalesce_sort_value("") == (1, "")


def test_priority_integers_still_sort_numerically():
    rows = [
        {"id": "1", "score": "10"},
        {"id": "2", "score": "100"},
        {"id": "3", "score": "50"},
    ]
    out = _apply_priority_and_limit(rows, "score", "desc", 2)
    assert [r["id"] for r in out] == ["2", "3"]


def test_priority_limit_does_not_invent_auto_group():
    rows = [
        {"id": "a", "score": "1.234"},
        {"id": "b", "score": "2"},
        {"id": "c", "score": "1.2345"},
    ]
    out = _apply_priority_and_limit(rows, "score", "asc", 0)
    # Bound numbers compare as Decimal; Auto grouping stays lexical after them.
    assert [r["id"] for r in out] == ["c", "b", "a"]
