"""X-123: dedupe_rows tie-break must be deterministic and consistent with
`keep`: keep=last on a full ordering tie keeps the LATER row in source
order; keep=first keeps the earlier. An explicit tie_break overrides.
"""

from __future__ import annotations

from services.shape_apply import ShapeRecipe
from services.shape_engine import ShapeEngine


def _dedupe(keep, rows, **options):
    recipe = ShapeRecipe.parse(
        {"steps": [{
            "op": "dedupe_rows",
            "options": {"keys": ["grp"], "keep": keep, "order_by": "amt", **options},
        }]},
        source_columns=["id", "grp", "amt"],
        source_types={"id": "int", "grp": "text", "amt": "float"},
    )
    eng = ShapeEngine(recipe)
    out = eng.apply_batch(rows)
    out.extend(eng.drain())
    return out


def test_keep_last_tie_keeps_later_row():
    # SQLite REAL collapsed BIG/BIG2 to an equal order_by value — the tie
    # must keep the later row (id 8), not the first seen (id 7).
    out = _dedupe("last", [
        {"id": 7, "grp": "c", "amt": 1.2345678901234568e16},
        {"id": 8, "grp": "c", "amt": 1.2345678901234568e16},
    ])
    assert [r["id"] for r in out] == [8]


def test_keep_first_tie_keeps_earlier_row():
    out = _dedupe("first", [
        {"id": 8, "grp": "c", "amt": 5.0},
        {"id": 7, "grp": "c", "amt": 5.0},
    ])
    assert [r["id"] for r in out] == [8]  # first-seen, regardless of id order


def test_explicit_tie_break_overrides():
    out = _dedupe("last", [
        {"id": 7, "grp": "c", "amt": 5.0},
        {"id": 8, "grp": "c", "amt": 5.0},
    ], tie_break="first_seen")
    assert [r["id"] for r in out] == [7]


def test_keep_last_still_picks_ordering_winner():
    out = _dedupe("last", [
        {"id": 7, "grp": "c", "amt": 1.0},
        {"id": 8, "grp": "c", "amt": 9.0},
    ])
    assert [r["id"] for r in out] == [8]
