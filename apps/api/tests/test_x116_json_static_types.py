"""X-116: structurally-typed JSON outputs declare statically — nest_json's
object lands json (jsonb on PG), unnest index lands BIGINT. Flatten keys
and unnest values stay sample-driven: a column may legitimately mix an
object and a plain string, so forcing json would mis-store scalars.
"""

from __future__ import annotations

from services.shape_apply import ShapeRecipe, derived_static_types


def _recipe(steps):
    return ShapeRecipe.parse(
        {"steps": steps},
        source_columns=["id", "addr", "tags", "doc", "city"],
        source_types={"id": "int"},
    )


def test_nest_json_to_declares_json():
    r = _recipe([
        {"op": "nest_json", "column": "addr",
         "options": {"to": "addr", "fields": [{"from": "city", "to": "c"}]}},
    ])
    assert derived_static_types(r)["addr"] == "json"


def test_unnest_index_to_declares_bigint_to_not_statically_typed():
    r = _recipe([
        {"op": "unnest_json", "column": "tags",
         "options": {"to": "tag", "index_to": "pos"}},
    ])
    declared = derived_static_types(r)
    assert declared["pos"] == "BIGINT"
    assert "tag" not in declared  # element may be scalar — sample inference


def test_flatten_json_key_stays_inferred():
    r = _recipe([
        {"op": "flatten_json", "column": "doc", "options": {"keys": ["meta"]}},
    ])
    assert "meta" not in derived_static_types(r)
