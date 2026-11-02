"""Sample-aware ARRAY strategies — JSON default, normalize/hybrid child fan-out."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.json_intelligence import (  # noqa: E402
    ARRAY_POLICY_HYBRID,
    ARRAY_POLICY_NORMALIZE_CHILD,
    STRUCT_POLICY_STORE_AS_JSON,
)
from services.structural_array import (  # noqa: E402
    STRUCTURAL_ARRAY_OF_OBJECT,
    STRUCTURAL_ARRAY_OF_PRIMITIVE,
    array_strategy_gate_issues,
    build_normalized_child_batches,
    classify_array_samples,
    propose_child_table_spec,
    recommend_array_strategies,
    stamp_mapping_array_strategies,
    validate_child_table_spec,
)


def test_classify_array_of_primitive():
    profile = classify_array_samples([
        '["python", "sql"]',
        '["go"]',
    ])
    assert profile["structural_class"] == STRUCTURAL_ARRAY_OF_PRIMITIVE
    assert profile["element_logical"] in {"VARCHAR", "STRING"} or profile["element_logical"]


def test_classify_array_of_object():
    profile = classify_array_samples([
        json.dumps([{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}]),
        json.dumps([{"sku": "C", "qty": 3}]),
    ])
    assert profile["structural_class"] == STRUCTURAL_ARRAY_OF_OBJECT
    names = {f["name"] for f in profile["child_fields"]}
    assert "sku" in names
    assert "qty" in names


def test_recommend_defaults_json_for_aoo():
    profile = classify_array_samples([
        json.dumps([{"a": 1}]),
    ])
    strategies = recommend_array_strategies(profile, dest_db="mysql", parent_column="items")
    assert strategies[0]["id"] == STRUCT_POLICY_STORE_AS_JSON
    assert strategies[0]["recommended"] is True
    ids = {s["id"] for s in strategies}
    assert ARRAY_POLICY_HYBRID in ids
    assert ARRAY_POLICY_NORMALIZE_CHILD in ids


def test_stamp_keeps_json_default_and_proposes_child():
    mappings = [{
        "source": "items",
        "target": "items",
        "source_type": "ARRAY",
        "target_type": "JSON",
        "confidence": 0.95,
        "reasoning": "identity",
    }]
    samples = {
        "items": [json.dumps([{"sku": "A", "qty": 1}])],
    }
    out = stamp_mapping_array_strategies(
        mappings, source_samples=samples, dest_db="mysql", parent_key_hint=["_id"]
    )
    assert out[0]["struct_policy"] == STRUCT_POLICY_STORE_AS_JSON
    assert out[0]["structural_class"] == STRUCTURAL_ARRAY_OF_OBJECT
    assert out[0].get("proposed_child_table_spec")
    assert out[0]["proposed_child_table_spec"]["parent_key_columns"] == ["_id"]


def test_gate_blocks_normalize_without_spec():
    issues = array_strategy_gate_issues([{
        "source": "items",
        "struct_policy": ARRAY_POLICY_NORMALIZE_CHILD,
    }])
    assert issues
    assert "child_table_spec" in issues[0]


def test_build_child_batches_hybrid():
    spec = {
        "child_table": "items__norm",
        "parent_key_columns": ["_id"],
        "ordinal_column": "_df_ord",
        "columns": [
            {"name": "sku", "type": "VARCHAR"},
            {"name": "qty", "type": "INTEGER"},
        ],
        "keep_parent_json": True,
    }
    ok, err = validate_child_table_spec(spec)
    assert err is None and ok
    headers = ["_id", "items"]
    rows = [
        ["p1", json.dumps([{"sku": "A", "qty": 1}, {"sku": "B", "qty": 2}])],
    ]
    batches, errors = build_normalized_child_batches(
        headers,
        rows,
        [{
            "source": "items",
            "target": "items",
            "struct_policy": ARRAY_POLICY_HYBRID,
            "child_table_spec": spec,
        }],
        dest_db="mysql",
    )
    assert errors == []
    assert len(batches) == 1
    assert batches[0]["child_table"] == "items__norm"
    assert len(batches[0]["rows"]) == 2
    assert batches[0]["rows"][0][:3] == ["p1", 0, "A"]
    assert batches[0]["rows"][1][2] == "B"


def test_missing_parent_pk_errors_not_silent():
    spec = {
        "child_table": "items__norm",
        "parent_key_columns": ["_id"],
        "ordinal_column": "_df_ord",
        "columns": [{"name": "sku", "type": "VARCHAR"}],
        "keep_parent_json": True,
    }
    batches, errors = build_normalized_child_batches(
        ["_id", "items"],
        [[None, json.dumps([{"sku": "A"}])]],
        [{
            "source": "items",
            "struct_policy": "hybrid",
            "child_table_spec": spec,
        }],
        dest_db="mysql",
    )
    assert batches == []
    assert errors and "missing parent_key" in errors[0]


def test_no_invented_parent_id_in_proposal():
    profile = classify_array_samples([json.dumps([{"a": 1}])])
    spec = propose_child_table_spec(profile, parent_column="items")
    assert spec["parent_key_columns"] == []
    assert spec.get("needs_parent_keys") is True


def test_pipeline_stamps_array_strategies():
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["_id", "tags"],
        target_columns=[],
        source_schemas=[
            {"name": "_id", "inferred_type": "VARCHAR", "samples": ["a1"], "is_primary_key": True},
            {
                "name": "tags",
                "inferred_type": "ARRAY",
                "samples": ['["x", "y"]'],
            },
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
        source_samples={"_id": ["a1"], "tags": ['["x", "y"]']},
        confidence_threshold=0.5,
    )
    tags = next(m for m in result["mappings"] if m["source"] == "tags")
    assert tags.get("structural_class") == STRUCTURAL_ARRAY_OF_PRIMITIVE
    assert tags.get("struct_policy") == STRUCT_POLICY_STORE_AS_JSON
    assert tags.get("array_strategies")
