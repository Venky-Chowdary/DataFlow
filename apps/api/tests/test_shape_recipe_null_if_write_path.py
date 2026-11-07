"""null_if persist uses present_cell_text, not str(v).

A programmatic recipe with values=[True] used to store ``True``. After
reload, apply compared that to dest ``true`` and missed. Reader-null
entries are skipped — they are already absent. ``True`` and ``true``
share one recipe hash.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.shape_engine import shape_records  # noqa: E402
from services.shape_models import ShapeError, ShapeRecipe  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402


def test_null_if_true_persists_as_dest_true():
    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {"op": "null_if", "column": "flag", "options": {"values": [True]}},
            ]
        },
        source_columns=["flag"],
    )
    assert recipe.steps[0].options["values"] == ["true"]
    as_text = ShapeRecipe.parse(
        {
            "steps": [
                {"op": "null_if", "column": "flag", "options": {"values": ["true"]}},
            ]
        },
        source_columns=["flag"],
    )
    assert recipe.recipe_hash == as_text.recipe_hash


def test_null_if_true_recipe_matches_dest_true_cell():
    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {"op": "null_if", "column": "flag", "options": {"values": [True]}},
            ]
        },
        source_columns=["flag"],
    )
    shaped, effect = shape_records(
        recipe,
        [{"flag": True}, {"flag": "true"}, {"flag": "keep"}],
    )
    assert [r["flag"] for r in shaped] == [None, None, "keep"]
    assert effect.nulls_introduced == 2


def test_null_if_skips_reader_null_in_values():
    recipe = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "null_if",
                    "column": "flag",
                    "options": {"values": [SQL_NULL_SENTINEL, "N/A"]},
                }
            ]
        },
        source_columns=["flag"],
    )
    assert recipe.steps[0].options["values"] == ["N/A"]


def test_null_if_all_blank_values_still_refuses():
    with pytest.raises(ShapeError, match="sentinel values"):
        ShapeRecipe.parse(
            {
                "steps": [
                    {
                        "op": "null_if",
                        "column": "flag",
                        "options": {"values": [SQL_NULL_SENTINEL]},
                    }
                ]
            },
            source_columns=["flag"],
        )
