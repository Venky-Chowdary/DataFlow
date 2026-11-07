"""Phase C4 — Structural Type Engine kernel facade (never silent flatten)."""

from __future__ import annotations

import json

from services.decision_kernel import (
    StructuralStrategy,
    assert_no_silent_flatten,
    classify_structural_column,
    default_structural_strategy,
    stamp_mapping_array_strategies,
)


def test_default_strategy_is_json_not_flatten():
    assert default_structural_strategy() is StructuralStrategy.STORE_AS_JSON
    assert assert_no_silent_flatten(None) == StructuralStrategy.STORE_AS_JSON.value
    assert assert_no_silent_flatten("silent_flatten") == StructuralStrategy.STORE_AS_JSON.value
    assert assert_no_silent_flatten("auto_flatten") == StructuralStrategy.STORE_AS_JSON.value
    # Named flatten without operator ack also refuses silent flatten.
    assert assert_no_silent_flatten("flatten") == StructuralStrategy.STORE_AS_JSON.value
    assert (
        assert_no_silent_flatten("flatten", operator_ack_flatten=True)
        != StructuralStrategy.STORE_AS_JSON.value
    )


def test_classify_structural_recommends_json_first():
    out = classify_structural_column(
        [json.dumps([{"sku": "A", "qty": 1}])],
        dest_db="postgresql",
        parent_column="items",
    )
    assert out["default_never_silent_flatten"] is True
    assert out["recommended_strategy"] == StructuralStrategy.STORE_AS_JSON.value
    assert out["strategies"][0]["id"] == StructuralStrategy.STORE_AS_JSON.value


def test_stamp_via_kernel_keeps_json_default():
    mappings = [
        {
            "source": "items",
            "target": "items",
            "source_type": "ARRAY",
            "target_type": "JSON",
        }
    ]
    out = stamp_mapping_array_strategies(
        mappings,
        source_samples={"items": [json.dumps([{"sku": "A"}])]},
        dest_db="mysql",
        parent_key_hint=["_id"],
    )
    assert out[0]["struct_policy"] == StructuralStrategy.STORE_AS_JSON.value
