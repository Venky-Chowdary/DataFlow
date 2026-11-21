"""Accuracy Track A item 2: assignment and suggestion policy around map_columns.

Names here are invented for the tests (none come from the held-out test split,
which tests/test_mapping_holdout_leak.py polices).
"""

from __future__ import annotations

from services.mapping_assignment_policy import (
    pin_decisive_pairs,
)
from services.semantic_mapper import map_columns


def _typed(cols):
    return [{"name": n, "inferred_type": t, "samples": list(s)} for n, t, s in cols]


def _by_source(rows):
    return {r["source"]: r for r in rows}


# ---------------- item 2: decisive pairs are not traded away ----------------


def test_mutual_best_pair_is_pinned_before_the_solver() -> None:
    scores = {
        ("a", "x"): (0.99, ""), ("a", "y"): (0.70, ""),
        ("b", "x"): (0.70, ""), ("b", "y"): (0.20, ""),
    }
    # Max-sum alone picks a->y + b->x (1.40) over a->x + b->y (1.19).
    assert pin_decisive_pairs({}, ["a", "b"], ["x", "y"], scores) == {"a": "x"}


def test_near_tie_is_left_to_the_solver() -> None:
    scores = {("a", "x"): (0.90, ""), ("a", "y"): (0.88, ""), ("b", "x"): (0.5, ""), ("b", "y"): (0.5, "")}
    assert pin_decisive_pairs({}, ["a", "b"], ["x", "y"], scores) == {}


def test_clear_abbreviation_is_not_traded_for_two_weak_pairs() -> None:
    # Max-sum alone gave div_cd -> first_name (0.70) + misc_txt -> division_code
    # (0.69) over div_cd -> division_code (0.95) + a leftover (0.11).
    src = _typed([("div_cd", "VARCHAR", ["OPS"]), ("misc_txt", "VARCHAR", ["foo"])])
    tgt = [{"name": "division_code", "inferred_type": "VARCHAR"}, {"name": "first_name", "inferred_type": "VARCHAR"}]
    out = _by_source(map_columns(
        [c["name"] for c in src], [t["name"] for t in tgt], source_schemas=src, target_schemas=tgt,
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="oracle",
    ))
    assert out["div_cd"]["target"] == "division_code"
    assert out["misc_txt"]["target"] not in {"division_code", "first_name"}
