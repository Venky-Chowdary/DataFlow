"""DECIMAL bind uses decimal_wire_value — no Decimal(text) invent.

Auto 1.234 used to land as 1.234. Locale money the write path stores must
still bind. Scale-preserving 1.2300 and PG airport pads still land.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_decimal_wire, normalize_sql_bind_value  # noqa: E402


def test_auto_three_digit_group_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1.234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("1.005")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_decimal_wire("12.345")


def test_money_text_at_an_identity_bind_is_refused_not_stripped():
    """Nobody declared a currency conversion, so the writer must not invent one.

    Text still carrying ``$``/``€`` at bind means the mapping left it identity:
    a DECIMAL column would silently gain 1234.56 the operator never asked for.
    A declared decimal/currency transform hands bind an exact ``Decimal``
    instead, which is why the typed carriers below still land.
    """
    for money in ("$1,234.56", "€1.234,56", "$1,234"):
        with pytest.raises(ValueError, match="currency marker"):
            coerce_decimal_wire(money)
    assert coerce_decimal_wire(Decimal("1234.56")) == Decimal("1234.56")
    assert coerce_decimal_wire("1.2345") == Decimal("1.2345")
    assert coerce_decimal_wire("1.2300") == Decimal("1.2300")


def test_normalize_sql_bind_decimal_carriers():
    assert normalize_sql_bind_value(Decimal("1234.56"), "DECIMAL(10,2)") == Decimal("1234.56")
    for money, ddl in (("$1,234.56", "DECIMAL(10,2)"), ("€1.234,56", "NUMERIC(10,2)")):
        with pytest.raises(ValueError, match="currency marker"):
            normalize_sql_bind_value(money, ddl)
    with pytest.raises(ValueError, match="refuse invent"):
        normalize_sql_bind_value("1.234", "DECIMAL(10,2)")
