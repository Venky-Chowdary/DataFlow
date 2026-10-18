"""nest_json — relational columns grouped into a JSON-object column.

The inverse of flatten_json (matrix row 23). A field naming a column the row
does not carry is refused — a key silently missing from the object is a
data-integrity event, not a null to paper over.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.shape_engine import ShapeRowError, shape_records
from services.shape_models import ShapeRecipe


def test_nest_groups_columns_and_drops_sources():
    plan = ShapeRecipe.parse(
        {
            "steps": [
                {
                    "op": "nest_json",
                    "options": {
                        "fields": ["street", {"from": "zip_code", "to": "zip"}],
                        "to": "address",
                        "drop": True,
                    },
                }
            ]
        },
        source_columns=["id", "street", "zip_code"],
    )
    shaped, effect = shape_records(
        plan, [{"id": 1, "street": "1 Main", "zip_code": "02139"}]
    )
    assert shaped == [{"id": 1, "address": '{"street":"1 Main","zip":"02139"}'}]
    assert json.loads(shaped[0]["address"]) == {"street": "1 Main", "zip": "02139"}
    assert effect.rows_out == 1
    assert effect.rows_out == effect.rows_in


def test_nest_keeps_sources_without_drop():
    plan = ShapeRecipe.parse(
        {"steps": [{"op": "nest_json", "options": {"fields": ["a"], "to": "p"}}]},
        source_columns=["a", "b"],
    )
    shaped, _ = shape_records(plan, [{"a": 1, "b": 2}])
    assert shaped[0]["a"] == 1
    assert shaped[0]["p"] == '{"a":1}'


def test_nest_refuses_a_field_naming_no_declared_column_at_parse():
    from services.shape_models import ShapeError

    with pytest.raises(ShapeError, match="not available"):
        ShapeRecipe.parse(
            {"steps": [{"op": "nest_json", "options": {"fields": ["zz"], "to": "p"}}]},
            source_columns=["a"],
        )


def test_nest_refuses_a_field_absent_from_the_row():
    plan = ShapeRecipe.parse(
        {"steps": [{"op": "nest_json", "options": {"fields": ["zz"], "to": "p"}}]},
        source_columns=["a", "zz"],
    )
    with pytest.raises(ShapeRowError, match="not a column"):
        shape_records(plan, [{"a": 1}])


def test_nest_requires_fields_and_to_at_parse():
    with pytest.raises(Exception, match="needs"):
        ShapeRecipe.parse(
            {"steps": [{"op": "nest_json", "options": {"to": "p"}}]},
            source_columns=["a"],
        )
