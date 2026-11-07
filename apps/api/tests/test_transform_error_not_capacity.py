"""A transform refusal is not a capacity problem."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.error_handling import humanize_transfer_failure


def test_transform_step_does_not_mention_capacity():
    human = humanize_transfer_failure(
        ValueError(
            "transform step 2 (round_number) on column 'amount': "
            "expected a number, got an empty value"
        )
    )
    assert human["code"] == "shape_refused_row"
    assert "address capacity" not in human["message"].lower()
    assert "capacity" not in human["message"].lower()


def test_recipe_validation_refusal_is_not_destination_column_missing():
    """P3a: an unknown tracked column is a recipe refusal before any write —
    not 'a column missing on the destination' and never a capacity problem."""
    human = humanize_transfer_failure(
        ValueError(
            "step 1: tracked column 'zz' is not available at this point "
            "in the recipe. Available: id, name, price, note"
        )
    )
    assert human["code"] == "shape_recipe_refusal"
    assert "capacity" not in human["message"].lower()
    assert "destination" not in human["title"].lower()
    assert "nothing was written" in human["message"].lower()
