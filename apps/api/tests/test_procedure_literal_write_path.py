"""Procedure SQL number literals bind as Decimal, not float64.

CALL foo(1.2300) is SQL grammar (dot is the decimal mark). float(token)
collapsed scale and lost digits past 2**53. Locale money is not a SQL
number — it must stay quoted or bound. Auto 1,234 is two arguments, not
one thousands token.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.procedure_source import (  # noqa: E402
    ProcedureSourceError,
    _literal,
    compile_callable_sql,
    parse_callable_source,
)


def test_sql_decimal_literal_keeps_scale():
    assert _literal("1.2300") == Decimal("1.2300")
    assert isinstance(_literal("1.2300"), Decimal)
    assert _literal("1.234") == Decimal("1.234")
    assert _literal("1e3") == Decimal("1000")
    assert _literal("42") == 42
    assert isinstance(_literal("42"), int)


def test_call_decimal_literal_survives_compile():
    spec = parse_callable_source(
        "CALL get_orders(1.2300)",
        dialect="mysql",
        mode="procedure",
    )
    _sql, binds = compile_callable_sql(spec)
    val = next(iter(binds.values()))
    assert val == Decimal("1.2300")
    assert isinstance(val, Decimal)


def test_locale_money_is_not_a_sql_number_literal():
    with pytest.raises(ProcedureSourceError):
        _literal("$1,234")
    with pytest.raises(ProcedureSourceError):
        parse_callable_source(
            "CALL get_orders($1234)",
            dialect="mysql",
            mode="procedure",
        )
