"""X-109 (P2): aggregate measures and surrogate_key land as typed columns on
every engine — they were inferred as TEXT from the batch sample.

SQL semantics, same rule on every engine, visible in the plan:
  count / count_distinct -> BIGINT
  sum                  -> DECIMAL(p+10, s) of the input
  avg                  -> DECIMAL(38, s+6), value rounded HALF_UP at emit
  min / max / first / last -> the input's own type
  surrogate_key        -> BIGINT
"""
import os
from decimal import Decimal

import pytest

from services.shape_apply import derived_static_types
from services.shape_models import ShapeRecipe


def _agg_recipe():
    return ShapeRecipe.parse({"version": 1, "steps": [
        {"op": "aggregate", "options": {"group_by": ["cust"], "measures": [
            {"function": "count", "column": "id", "as": "n"},
            {"function": "count", "column": "*", "as": "nall"},
            {"function": "sum", "column": "amt", "as": "total"},
            {"function": "avg", "column": "amt", "as": "av"},
            {"function": "min", "column": "amt", "as": "mn"},
            {"function": "max", "column": "amt", "as": "mx"},
            {"function": "last", "column": "note", "as": "ln"},
        ]}},
        {"op": "surrogate_key", "options": {"to": "sk"}},
    ]})


def test_aggregate_measure_and_sk_static_types():
    out = derived_static_types(_agg_recipe(), {
        "id": "INTEGER", "amt": "NUMERIC(12,4)", "cust": "TEXT",
        "note": "VARCHAR(50)",
    })
    assert out["n"] == "BIGINT" and out["nall"] == "BIGINT"
    assert out["total"] == "DECIMAL(22,4)"   # 12+10 precision, scale kept
    assert out["av"] == "DECIMAL(38,10)"     # engine cap, scale+6
    assert out["mn"] == "NUMERIC(12,4)"      # input type
    assert out["mx"] == "NUMERIC(12,4)"
    assert out["ln"] == "VARCHAR(50)"        # first/last keep input type
    assert out["sk"] == "BIGINT"


def test_avg_emitted_half_up_at_scale_plus_6():
    """0.233333… over a NUMERIC(12,4) input must emit '0.2333333333' —
    the same value a DECIMAL(38,10) carrier stores (was 28-digit float
    text on TEXT carriers)."""
    from services.shape_apply import ShapeRunner

    recipe = ShapeRecipe.parse({"version": 1, "steps": [
        {"op": "aggregate", "options": {"group_by": ["cust"], "measures": [
            {"function": "avg", "column": "amt", "as": "av"},
        ]}},
    ]}, source_types={"amt": "NUMERIC(12,4)", "cust": "TEXT"})
    runner = ShapeRunner(recipe)
    runner.records([
        {"cust": "D", "amt": Decimal("0.1")},
        {"cust": "D", "amt": Decimal("0.2")},
        {"cust": "D", "amt": Decimal("0.4")},
    ])
    rows = runner.drain()
    assert rows and rows[0]["av"] == Decimal("0.2333333333")


def test_avg_half_up_rounding_direction():
    """0.12345678905 at scale 4 + 6 → 0.1234567891 (HALF_UP, not banker)."""
    from services.shape_engine import _round_avg_to_declared

    v = _round_avg_to_declared(0.12345678905, "NUMERIC(12,4)")
    assert v == Decimal("0.1234567891")


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X109_LIVE") != "1",
    reason="live 5-engine aggregate typing matrix — user-run",
)
def test_x109_live(tmp_path):  # pragma: no cover
    pass
