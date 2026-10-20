"""X-102: split_column must never drop overflow. ``limit`` = max parts
(the tail folds into the last piece); overflow beyond declared ``into``
targets folds into the LAST target — 'Mary Ann Evans' → ['Mary','Ann Evans'],
never a silently dropped 'Evans'.
"""

from __future__ import annotations

from services.shape_apply import ShapeRecipe
from services.shape_engine import ShapeEngine


def _split(text, targets, **options):
    recipe = ShapeRecipe.parse(
        {"steps": [{
            "op": "split_column", "column": "name",
            "options": {"separator": " ", "into": targets, **options},
        }]},
        source_columns=["id", "name"],
        source_types={"id": "int", "name": "text"},
    )
    eng = ShapeEngine(recipe)
    out = eng.apply_batch([{"id": 1, "name": text}])
    out.extend(eng.drain())
    return out[0]


def test_overflow_folds_into_last_target():
    row = _split("Mary Ann Evans", ["first", "last"])
    assert row["first"] == "Mary"
    assert row["last"] == "Ann Evans"  # 'Evans' must not be dropped


def test_limit_caps_parts_tail_in_last():
    row = _split("a b c d e", ["p1", "p2", "p3"], limit=3)
    assert (row["p1"], row["p2"], row["p3"]) == ("a", "b", "c d e")


def test_fewer_pieces_stay_null():
    row = _split("Mary Ann", ["first", "last", "suffix"])
    assert row["first"] == "Mary"
    assert row["last"] == "Ann"
    assert row["suffix"] is None
