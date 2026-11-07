"""Pilot numeric predicates keep write-path Decimals, not float(dec).

Auto 1,234 still refuses. Currency money stays exact for SQL binds.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from src.ai.copilot.predicates import PredicateError, ground_filters, parse_filters  # noqa: E402

_COLUMNS = [
    {"name": "amount", "inferred_type": "NUMERIC"},
]


def _resolve(target: str, names: list[str]) -> str:
    return target if target in names else ""


def _type_of(columns, name: str) -> str:
    for col in columns:
        if col.get("name") == name:
            return str(col.get("inferred_type") or "")
    return ""


def _ground(where: str):
    parsed, _ = parse_filters(where)
    return ground_filters(parsed, _COLUMNS, _resolve, _type_of)


def test_currency_literal_stays_decimal():
    preds = _ground("amount > $1,234.56")
    assert preds[0].values == [Decimal("1234.56")]
    assert isinstance(preds[0].values[0], Decimal)


def test_dest_canonical_fraction_stays_decimal():
    preds = _ground("amount > 1.2345")
    assert preds[0].values[0] == Decimal("1.2345")
    assert isinstance(preds[0].values[0], Decimal)


def test_auto_grouping_still_refuses():
    with pytest.raises(PredicateError, match="1,234"):
        _ground("amount > 1,234")
