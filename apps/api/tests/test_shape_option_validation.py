"""Set-op option validation — bad config refuses at parse, never emits wrong data.

Audit P0-2: dedupe_rows/aggregate/sort_rows/rank_rows/lookup/surrogate_key/
nest_json previously accepted any option silently — aggregate 'bogus'
emitted None measures, dedupe 'bogus' acted as keep-last, lookup 'notalist'
emitted nulls. Each now refuses at ShapeRecipe.parse with the step named.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.shape_models import ShapeError, ShapeRecipe
from services.transform_recipe import RecipeStep, TransformRecipe

COLS = ["id", "ts", "dept", "amt", "name"]


def _parse(steps):
    return ShapeRecipe.parse({"steps": steps}, source_columns=list(COLS))


# ---- refusals, one per op ---------------------------------------------------

def test_dedupe_refuses_unknown_keep():
    with pytest.raises(ShapeError, match="keep must be first or last"):
        _parse([{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "bogus"}}])


def test_dedupe_keep_last_without_order_by_is_last_seen():
    """keep=last with no ordering column is the declared last-seen rule —
    the last-arriving member wins (B2)."""
    recipe = _parse(
        [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "last"}}]
    )
    step = recipe.enabled_steps[0]
    assert step.options["keep"] == "last"
    assert "order_by" not in step.options


def test_dedupe_refuses_a_missing_key_column():
    with pytest.raises(ShapeError, match="not available"):
        _parse([{"op": "dedupe_rows", "options": {"keys": ["zz"], "keep": "first"}}])


def test_aggregate_refuses_an_unknown_measure_function():
    with pytest.raises(ShapeError, match="not one of"):
        _parse(
            [
                {
                    "op": "aggregate",
                    "options": {
                        "group_by": ["dept"],
                        "measures": [{"function": "bogus", "column": "amt", "as": "t"}],
                    },
                }
            ]
        )


def test_aggregate_refuses_a_measure_without_output_name():
    with pytest.raises(ShapeError, match="'as' output name"):
        _parse(
            [
                {
                    "op": "aggregate",
                    "options": {
                        "group_by": ["dept"],
                        "measures": [{"function": "sum", "column": "amt"}],
                    },
                }
            ]
        )


def test_sort_refuses_a_missing_order_column():
    with pytest.raises(ShapeError, match="not available"):
        _parse([{"op": "sort_rows", "options": {"order_by": "zzz"}}])


def test_rank_refuses_an_unknown_direction():
    with pytest.raises(ShapeError, match="top or bottom"):
        _parse(
            [
                {
                    "op": "rank_rows",
                    "options": {"order_by": "amt", "direction": "middle", "limit": 3},
                }
            ]
        )


def test_lookup_refuses_non_list_reference_rows():
    with pytest.raises(ShapeError, match="list of reference"):
        _parse(
            [
                {
                    "op": "lookup",
                    "column": "id",
                    "options": {"lookup_key": "id", "rows": "notalist", "map": {"a": "b"}},
                }
            ]
        )


def test_lookup_refuses_an_empty_map():
    with pytest.raises(ShapeError, match="non-empty map"):
        _parse(
            [
                {
                    "op": "lookup",
                    "column": "id",
                    "options": {"lookup_key": "id", "rows": [{"id": "1"}], "map": {}},
                }
            ]
        )


def test_lookup_refuses_an_unknown_unmatched_policy():
    with pytest.raises(ShapeError, match="null, drop, divert or default"):
        _parse(
            [
                {
                    "op": "lookup",
                    "column": "id",
                    "options": {
                        "lookup_key": "id",
                        "rows": [{"id": "1"}],
                        "map": {"v": "x"},
                        "unmatched": "zz",
                    },
                }
            ]
        )


def test_surrogate_refuses_a_non_numeric_start():
    with pytest.raises(ShapeError, match="whole number"):
        _parse([{"op": "surrogate_key", "options": {"to": "sk", "start": "abc"}}])


def test_nest_json_refuses_empty_fields():
    with pytest.raises(ShapeError, match="non-empty fields"):
        _parse([{"op": "nest_json", "options": {"fields": [], "to": "p"}}])


def test_nest_json_refuses_a_field_naming_no_column():
    with pytest.raises(ShapeError, match="not available"):
        _parse([{"op": "nest_json", "options": {"fields": ["zz"], "to": "p"}}])


# ---- design-time check() catches bind-time failures (audit P0-1/P0-2) -------

def _recipe_status(step: RecipeStep):
    recipe = TransformRecipe(name="t", steps=[step])
    recipe.refresh_status()
    return recipe.status, recipe.issues


def test_strip_controls_recipe_is_active_and_compiles():
    status, issues = _recipe_status(RecipeStep(op="strip_controls", column="name"))
    assert status == "active", issues


def test_bad_aggregate_recipe_stays_draft_with_named_step():
    status, issues = _recipe_status(
        RecipeStep(
            op="aggregate",
            column="amt",
            options={
                "group_by": ["dept"],
                "measures": [{"function": "bogus", "column": "amt", "as": "t"}],
            },
        )
    )
    assert status == "draft"
    assert issues and "step 1" in issues[0]


def test_bad_lookup_recipe_stays_draft_with_named_step():
    status, issues = _recipe_status(
        RecipeStep(
            op="lookup",
            column="id",
            options={"lookup_key": "id", "rows": "notalist", "map": {"a": "b"}},
        )
    )
    assert status == "draft"
    assert issues and "step 1" in issues[0]


def test_strip_controls_cleans_a_control_character_row():
    from services.transform_recipe import compile_recipe
    from services.shape_engine import shape_records

    recipe = TransformRecipe(
        name="strip", steps=[RecipeStep(op="strip_controls", column="name")]
    )
    recipe.refresh_status()
    payload, shape = compile_recipe(recipe, source_columns=["name"])
    shaped, _ = shape_records(shape, [{"name": "a\x00b\x07c\td"}])
    assert shaped == [{"name": "abc\td"}]
