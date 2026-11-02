"""Create-new Decision Kernel honesty — Postgres → Snowflake TPC-H CUSTOMER.

The type path can be correct while the explanation layer lies (identity + parse
integer + flat 95% + BUILDING as a record id). This fixture proves the shared
classifiers, not a route-specific UI string.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.conversion_contract import ConversionClass, classify_conversion, create_new_mapping_reason
from services.mapping_quality import refine_mappings_with_quality
from services.semantic_analyzer import analyze_column
from services.semantic_mapper import map_columns
from services.transform_engine import infer_transform_for_mapping


CUSTOMER_SCHEMAS = [
    {"name": "c_custkey", "inferred_type": "BIGINT", "samples": ["1", "2", "3", "4", "5", "6", "7", "8"]},
    {"name": "c_name", "inferred_type": "VARCHAR(25)", "samples": ["Customer#000000001"]},
    {"name": "c_address", "inferred_type": "VARCHAR(40)", "samples": ["IVhzIApeRb ot,c,E"]},
    {"name": "c_nationkey", "inferred_type": "BIGINT", "samples": ["15"]},
    {"name": "c_phone", "inferred_type": "VARCHAR(15)", "samples": ["25-989-741-2988"]},
    {"name": "c_acctbal", "inferred_type": "DECIMAL(11,6)", "samples": ["711.5600"]},
    {"name": "c_mktsegment", "inferred_type": "VARCHAR(10)", "samples": ["BUILDING", "FURNITURE", "MACHINERY"]},
    {"name": "c_comment", "inferred_type": "VARCHAR(117)", "samples": ["to the even, regular platelets."]},
]


def test_bigint_to_number38_is_lossless_widening_not_identity():
    reason = create_new_mapping_reason("BIGINT", "NUMBER(38,0)", dest_db="snowflake")
    assert "identity mapping" not in reason.lower()
    assert "lossless widening" in reason
    assert "CREATE on first write as NUMBER(38,0)" in reason
    classified = classify_conversion("BIGINT", "NUMBER(38,0)", dest_db="snowflake", transform="none")
    assert classified["conversion_class"] == ConversionClass.WIDENING.value
    assert classified["lossy"] is False
    assert classified["invents_capacity"] is False


def test_decimal_to_number_same_params_is_equivalent():
    reason = create_new_mapping_reason("DECIMAL(11,6)", "NUMBER(11,6)", dest_db="snowflake")
    assert "lossless equivalent" in reason
    classified = classify_conversion(
        "DECIMAL(11,6)", "NUMBER(11,6)", dest_db="snowflake", transform="none"
    )
    assert classified["conversion_class"] == ConversionClass.EQUIVALENT.value


def test_native_numeric_does_not_invent_parse_transform():
    assert infer_transform_for_mapping("c_custkey", "c_custkey", "BIGINT", "NUMBER(38,0)") == "none"
    assert infer_transform_for_mapping("c_acctbal", "c_acctbal", "DECIMAL(11,6)", "NUMBER(11,6)") == "none"
    assert infer_transform_for_mapping("c_nationkey", "c_nationkey", "BIGINT", "NUMBER(38,0)") == "none"
    # Text sources still parse.
    assert infer_transform_for_mapping("qty", "qty", "VARCHAR", "INTEGER") == "integer"
    assert infer_transform_for_mapping("amt", "amt", "TEXT", "DECIMAL(11,6)") == "decimal"


def test_mktsegment_is_categorical_not_generic_identifier():
    analyzed = analyze_column(
        "c_mktsegment",
        "VARCHAR(10)",
        ["BUILDING", "FURNITURE", "MACHINERY", "BUILDING"],
    )
    assert analyzed["semantic_role"] in {"market_segment", "categorical"}
    assert "identifier" not in analyzed["description"].lower()
    assert "categorical" in analyzed["description"].lower() or "segment" in analyzed["description"].lower()


def test_create_new_customer_map_columns_honest_explanations():
    mappings = map_columns(
        [s["name"] for s in CUSTOMER_SCHEMAS],
        [],
        source_schemas=CUSTOMER_SCHEMAS,
        destination_db_type="snowflake",
        destination_table_exists=False,
    )
    refined = refine_mappings_with_quality(
        mappings,
        source_schemas=CUSTOMER_SCHEMAS,
        destination_db_type="snowflake",
    )
    by_src = {m["source"]: m for m in refined}

    custkey = by_src["c_custkey"]
    assert "lossless widening" in custkey["reasoning"]
    assert "identity mapping" not in custkey["reasoning"].lower()
    assert custkey["conversion_class"] == ConversionClass.WIDENING.value

    acctbal = by_src["c_acctbal"]
    assert "lossless equivalent" in acctbal["reasoning"] or "identity" in acctbal["reasoning"]

    segment = by_src["c_mktsegment"]
    assert "generic record identifier" not in segment["reasoning"].lower()

    confs = {name: float(row["confidence"]) for name, row in by_src.items()}
    assert all(c <= 0.93 for c in confs.values())
    # Evidence bands must differ — not a flat 95% wall.
    assert len({round(c, 2) for c in confs.values()}) >= 2
    assert confs["c_acctbal"] > confs["c_custkey"]
    assert confs["c_acctbal"] > confs["c_mktsegment"]
