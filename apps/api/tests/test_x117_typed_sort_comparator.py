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


# ---- live_616d_rsorti evidence (INT / BIGINT / DATE / TEXT keys) ----

def _run(steps, rows, source_types):
    recipe = ShapeRecipe.parse(
        {"steps": steps},
        source_columns=list(source_types),
        source_types=source_types,
    )
    engine = ShapeEngine(recipe)
    engine.apply_batch(rows)
    return engine.drain()


def test_x117_int_key_sorts_numeric_not_lexical():
    rows = [
        {"id": i, "v": v}
        for i, v in enumerate([9, 10, -2, -10, 100, 0, None, 2147483647], 1)
    ]
    out = _run(
        [{"op": "sort_rows", "options": {"order_by": "v", "direction": "asc"}}],
        rows, {"id": "int", "v": "integer"},
    )
    # -10,-2,0,9,10,100,2147483647 then NULL last (was lexical + NULL first).
    assert [r["v"] for r in out] == [-10, -2, 0, 9, 10, 100, 2147483647, None]


def test_x117_bigint_desc_exact_int_never_float():
    big = 2**53 + 1
    big2 = 2**53
    rows = [{"id": i, "v": v} for i, v in enumerate(
        [9, 10, -2, -10, 100, big, big2, None], 1
    )]
    out = _run(
        [{"op": "sort_rows", "options": {"order_by": "v", "direction": "desc"}}],
        rows, {"id": "int", "v": "bigint"},
    )
    # 2^53+1 and 2^53 stay distinct — an exact int compare, never float.
    assert [r["v"] for r in out] == [big, big2, 100, 10, 9, -2, -10, None]


def test_x118_rank_bottom_n_excludes_nulls():
    rows = [{"id": i, "v": v} for i, v in enumerate(
        [9, 10, -2, -10, 100, 0, None, 2147483647], 1
    )]
    out = _run(
        [{"op": "rank_rows",
          "options": {"order_by": "v", "direction": "bottom", "limit": 2}}],
        rows, {"id": "int", "v": "integer"},
    )
    # SQL bottom-2 is -10,-2 — the NULL is ineligible, not a rank winner.
    assert [r["v"] for r in out] == [-10, -2]


def test_x118_rank_top_n_excludes_nulls():
    rows = [{"id": i, "v": v} for i, v in enumerate(
        [9, 10, -2, -10, 100, 0, None, 2147483647], 1
    )]
    out = _run(
        [{"op": "rank_rows",
          "options": {"order_by": "v", "direction": "top", "limit": 2}}],
        rows, {"id": "int", "v": "integer"},
    )
    assert [r["v"] for r in out] == [2147483647, 100]


def test_x118_rank_null_never_fills_a_short_window():
    """limit larger than the known values must not let a NULL backfill —
    pre-fix rows[:limit] picked the tail NULL; the eligible-set cut leaves
    it out."""
    rows = [{"id": i, "v": v} for i, v in enumerate([9, -2, None], 1)]
    out = _run(
        [{"op": "rank_rows",
          "options": {"order_by": "v", "direction": "bottom", "limit": 3}}],
        rows, {"id": "int", "v": "integer"},
    )
    assert [r["v"] for r in out] == [-2, 9]


def test_x118_rank_nulls_first_opt_in_may_elect_null():
    rows = [{"id": i, "v": v} for i, v in enumerate([9, -2, None], 1)]
    out = _run(
        [{"op": "rank_rows",
          "options": {"order_by": "v", "direction": "top", "limit": 1,
                      "nulls": "first"}}],
        rows, {"id": "int", "v": "integer"},
    )
    assert [r["v"] for r in out] == [None]


def test_x117_date_key_compares_by_instant_incl_year_below_1000():
    rows = [{"id": i, "d": v} for i, v in enumerate(
        ["0001-01-01", "0999-01-01", "2025-12-31", "2026-01-09",
         "2026-01-10", "2026-10-01", None], 1)]
    out = _run(
        [{"op": "sort_rows", "options": {"order_by": "d", "direction": "asc"}}],
        rows, {"id": "int", "d": "date"},
    )
    assert [r["d"] for r in out] == [
        "0001-01-01", "0999-01-01", "2025-12-31", "2026-01-09",
        "2026-01-10", "2026-10-01", None]


def test_x117_text_key_stays_codepoint_even_numeric_looking():
    rows = [{"id": i, "t": v} for i, v in enumerate(
        ["9", "10", "100", "-2", "B", "a", "", None], 1)]
    out = _run(
        [{"op": "sort_rows", "options": {"order_by": "t", "direction": "asc"}}],
        rows, {"id": "int", "t": "text"},
    )
    # TEXT compares by codepoint — '10' < '100' < '9' — '' before letters,
    # NULL last and never merged with ''.
    assert [r["t"] for r in out] == ["", "-2", "10", "100", "9", "B", "a", None]


@pytest.mark.skipif(not os.environ.get("DATAFLOW_X117_LIVE"), reason="live engines — user-run")
def test_x117_live_rsort_doc_num_rank_dd():
    pytest.fail("harness: live_616d_rsort.py doc/num/rank/dd x 5 engines")
