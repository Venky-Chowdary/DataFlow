"""X-117/X-118 — one typed comparator + one NULL rule for ordering ops.

X-117 (P0): sort_rows and rank_rows ordered a declared NUMERIC column by
its wire text — lexical '100' < '…BIG' < '2.5' < '9' — so rank top-1
landed id1 amt 9 instead of id9 amt 100 and Gate-8 verified it (silent
wrong top-N). dedupe_rows already compared under the declared type
(`_dedupe_sort_val`); sort/rank/aggregate min/max now share it.
X-118 (P1): NULL sorted as '' — first on ASC, tied with ''. NULL is now
distinct and pinned last by default in BOTH directions; an order entry
may declare ``nulls: 'first'``.

Live 616df100 (all 5 engines identical): sort [amt asc] over
{9,10,-2,-10,2.5,100,BIG,BIG2,NULL} gave lexical order; rank top amt
over {9,10,-2,2.5,100} gave id1.
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

from services.shape_engine import ShapeEngine, _SQL_NULL_SENTINEL
from services.shape_models import ShapeRecipe

# The live "num" recipe set: lexical order is '10'<'100'<'123…'<'2.5'<'9';
# numeric order is -10,-2,2.5,9,10,100,BIG,BIG2 then NULL last.
NUM_ROWS = [
    {"id": 1, "amt": "9"},
    {"id": 2, "amt": "10"},
    {"id": 3, "amt": "-2"},
    {"id": 4, "amt": "-10"},
    {"id": 5, "amt": "2.5"},
    {"id": 9, "amt": "100"},
    {"id": 7, "amt": "12345678901234567.0000000001"},
    {"id": 6, "amt": "12345678901234567.0000000002"},
    {"id": 8, "amt": _SQL_NULL_SENTINEL},
]
NUM_EXPECTED = [4, 3, 5, 1, 2, 9, 7, 6, 8]


def _engine(steps, source_types=None):
    recipe = ShapeRecipe.parse(
        {"steps": steps},
        source_columns=["id", "amt", "grp"],
        source_types=source_types or {"amt": "numeric(38,10)", "grp": "string"},
    )
    engine = ShapeEngine(recipe)
    engine.apply_batch(NUM_ROWS)
    return engine


def test_x117_sort_rows_numeric_compares_as_decimal():
    eng = _engine([{"op": "sort_rows", "options": {"order_by": "amt", "direction": "asc"}}])
    out = eng.drain()
    assert [r["id"] for r in out] == NUM_EXPECTED


def test_x117_sort_rows_numeric_desc():
    eng = _engine([{"op": "sort_rows", "options": {"order_by": "amt", "direction": "desc"}}])
    out = eng.drain()
    # Descending values, NULL still last — the documented X-118 rule.
    assert [r["id"] for r in out] == list(reversed(NUM_EXPECTED[:-1])) + [8]


def test_x117_rank_top_picks_true_max():
    eng = _engine([{"op": "rank_rows", "options": {"order_by": "amt", "direction": "top", "limit": 1}}])
    out = eng.drain()
    assert [r["id"] for r in out] == [6]  # BIG2 — Decimal-exact …0002 > …0001


def test_x117_aggregate_min_max_decimal_exact():
    eng = _engine([{
        "op": "aggregate",
        "options": {
            "group_by": [],
            "measures": [
                {"as": "mn", "function": "min", "column": "amt"},
                {"as": "mx", "function": "max", "column": "amt"},
            ],
        },
    }])
    out = eng.drain()
    assert out[0]["mn"] == "-10" and out[0]["mx"] == "12345678901234567.0000000002"


def test_x118_null_is_not_empty_and_sorts_last():
    rows = [
        {"id": 1, "amt": "5", "grp": ""},
        {"id": 2, "amt": "1", "grp": _SQL_NULL_SENTINEL},
        {"id": 3, "amt": "9", "grp": "b"},
    ]
    recipe = ShapeRecipe.parse(
        {"steps": [{"op": "sort_rows", "options": {"order_by": "grp", "direction": "asc"}}]},
        source_columns=["id", "amt", "grp"],
        source_types={"amt": "numeric(38,10)", "grp": "string"},
    )
    eng = ShapeEngine(recipe)
    eng.apply_batch(rows)
    out = eng.drain()
    # '' (id1) before 'b' (id3); NULL (id2) pinned last — never tied with ''.
    assert [r["id"] for r in out] == [1, 3, 2]


def test_x118_nulls_first_opt_in():
    eng = _engine([{
        "op": "sort_rows",
        "options": {"order": [{"column": "amt", "direction": "asc", "nulls": "first"}]},
    }])
    out = eng.drain()
    assert out[0]["id"] == 8  # NULL first when declared
    assert [r["id"] for r in out[1:]] == NUM_EXPECTED[:-1]


@pytest.mark.skipif(not os.environ.get("DATAFLOW_X117_LIVE"), reason="live engines — user-run")
def test_x117_live_rsort_doc_num_rank_dd():
    pytest.fail("harness: live_616d_rsort.py doc/num/rank/dd x 5 engines")
