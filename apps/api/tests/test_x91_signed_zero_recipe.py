"""X-91: recipe numeric ops and the Gate-8 digest agree on decimal -0.

``truncate_number(places=1)`` on ``'-0.01'`` used to emit
``Decimal('-0.0')`` — a signed decimal zero. No SQL decimal type stores a
sign bit on zero (PG ``trunc(-0.01, 1)`` is ``0.0``, and MySQL/SQL
Server/DuckDB DECIMAL behave the same), so the clean run false-failed the
checksum on 4 engines: the write pass hashed the recipe's ``-0.0`` while
the destination read back ``0``. Rule settled with X-84 (float -0.0):
**-0 is never emitted by value ops and always digests as 0** — decimal
and float alike.
"""

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_x91_value_ops_never_emit_signed_decimal_zero():
    from services.shape_expr import _fn_truncate, _round_half_up

    for fn, value, places in (
        (_fn_truncate, "-0.01", 1),
        (_fn_truncate, "-0.4", 0),
        (_fn_truncate, "-0.09", 1),
        (_round_half_up, "-0.004", 2),
        (_round_half_up, "-0.00000001", 4),
    ):
        out = fn(value, places)
        assert isinstance(out, Decimal)
        assert out == 0 and out.as_tuple().sign == 0, (
            f"{fn.__name__}({value!r}, {places}) emitted signed zero {out!r}"
        )
    # Zero results keep the requested scale ('0.0', '0.00', '0').
    assert _fn_truncate("-0.01", 1) == Decimal("0.0")
    assert _round_half_up("-0.004", 2) == Decimal("0.00")
    assert _fn_truncate("-0.4", 0) == Decimal("0")
    # Non-zero results are untouched — sign, scale, value.
    assert _fn_truncate("-1.99", 1) == Decimal("-1.9")
    assert _round_half_up("99.55", 1) == Decimal("99.6")
    assert _fn_truncate("2.99", 1) == Decimal("2.9")


def test_x91_digest_canonicalizes_decimal_neg_zero():
    """Both digest sides render decimal -0 as 0 — same rule as float -0.0."""
    from services.reconciliation import (
        _canonicalize_number,
        fingerprint_for_reconcile,
        normalize_cell,
    )

    assert _canonicalize_number(Decimal("-0.0")) == "0"
    assert _canonicalize_number(Decimal("0.0")) == "0"
    assert _canonicalize_number(Decimal("-0")) == "0"
    assert _canonicalize_number(-0.0) == "0"
    # Through the two public fingerprint paths too.
    assert normalize_cell(Decimal("-0.0")) == normalize_cell(Decimal("0.0"))
    assert fingerprint_for_reconcile(
        Decimal("-0.0"), ddl_type="DECIMAL(38,1)"
    ) == fingerprint_for_reconcile(Decimal("0.0"), ddl_type="DECIMAL(38,1)")


def test_x91_recipe_chain_lands_unsigned_zero():
    """The step layer (truncate_number / round_number) inherits the rule."""
    from services.shape_engine import ShapeStep, _transform_value

    step = ShapeStep(op="truncate_number", column="m", options={"places": 1})
    out = _transform_value(step, "-0.01")
    assert isinstance(out, Decimal) and out == 0 and out.as_tuple().sign == 0

    step2 = ShapeStep(op="round_number", column="m", options={"places": 2})
    out2 = _transform_value(step2, "-0.004")
    assert isinstance(out2, Decimal) and out2 == 0 and out2.as_tuple().sign == 0


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X91_LIVE") != "1",
    reason="live engines required — DATAFLOW_X91_LIVE=1",
)
def test_x91_signed_zero_recipe_clean_and_tamper_live():
    """Recipe producing negative zero + normal rows ->
    MySQL/SQLite/MSSQL/PG/DuckDB: clean verified + tamper caught."""
    pytest.fail("live leg — runs on the user's engine matrix")
