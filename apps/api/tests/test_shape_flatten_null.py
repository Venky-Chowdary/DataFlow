"""flatten_json on a NULL cell is a no-op, not a failed run."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.shape_engine import ShapeRowError, shape_records
from services.shape_models import ShapeRecipe


def _plan():
    return ShapeRecipe.parse(
        {"steps": [{"op": "flatten_json", "column": "payload", "options": {"keys": ["sku"]}}]},
        source_columns=["id", "payload"],
    )


def test_null_json_does_not_fail_the_run():
    plan = _plan()
    shaped, effect = shape_records(
        plan,
        [
            {"id": "1", "payload": None},
            {"id": "2", "payload": "null"},
            {"id": "3", "payload": '{"sku": "A"}'},
        ],
    )
    assert [row["id"] for row in shaped] == ["1", "2", "3"]
    assert shaped[0].get("sku") is None
    assert shaped[2]["sku"] == "A"
    assert effect.rows_out == 3
    assert effect.steps[0].errors == 0


def test_non_object_json_still_refuses():
    plan = _plan()
    with pytest.raises(ShapeRowError, match="not a JSON object"):
        shape_records(plan, [{"id": "1", "payload": "[1, 2]"}])
