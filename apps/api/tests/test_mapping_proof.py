"""Tests for universal mapping proof payload."""

from services.mapping_proof import build_mapping_proof, confidence_breakdown, transform_fidelity
from services.mapping_pipeline import run_mapping_pipeline


def test_transform_fidelity_preserve():
    assert transform_fidelity("none") == "preserve"
    assert transform_fidelity("identity") == "preserve"


def test_transform_fidelity_mutate():
    assert transform_fidelity("trim") == "mutate"
    assert transform_fidelity("hash_pii") == "mutate"


def test_create_new_proof_mode_and_cap():
    proof = build_mapping_proof(
        [{
            "source": "id",
            "target": "id",
            "confidence": 0.92,
            "source_type": "INTEGER",
            "target_type": "NUMBER(38,0)",
            "transform": "none",
            "assignment_strategy": "identity_passthrough",
            "create_new": True,
            "reasoning": "New destination table — identity mapping; types will CREATE on first write as NUMBER(38,0)",
        }],
        target_columns=[],
        destination_db_type="snowflake",
    )
    assert proof["dest_mode"] == "create_new"
    assert proof["summary"]["max_confidence"] <= 0.93
    assert proof["quarantine_posture"]
    assert proof["mappings"][0]["schema_decision"].startswith("CREATE column")
    assert proof["mappings"][0]["match_quality"] == "exact_name"
    bd = proof["mappings"][0]["evidence"]["confidence_breakdown"]
    assert abs(sum(bd.values()) - proof["mappings"][0]["confidence"]) < 0.002


def test_match_existing_proof_mode():
    proof = build_mapping_proof(
        [{
            "source": "amount",
            "target": "payment_amount",
            "confidence": 0.96,
            "source_type": "DECIMAL",
            "target_type": "NUMERIC",
            "transform": "decimal",
            "assignment_strategy": "optimal_bipartite_hungarian",
            "reasoning": "Exact name match",
            "exists_in_destination": True,
        }],
        target_columns=["payment_amount", "customer_id"],
        destination_db_type="postgresql",
    )
    assert proof["dest_mode"] == "match_existing"
    assert "MATCH existing" in proof["mappings"][0]["schema_decision"]


def test_proof_lists_trim_fidelity_risk():
    proof = build_mapping_proof(
        [{
            "source": "name",
            "target": "name",
            "confidence": 0.9,
            "source_type": "VARCHAR",
            "target_type": "VARCHAR",
            "transform": "trim",
            "assignment_strategy": "identity_passthrough",
            "create_new": True,
        }],
        target_columns=[],
        destination_db_type="mysql",
    )
    risks = proof["mappings"][0]["risks"]
    assert any(r["code"] == "trim_mutates" for r in risks)


def test_unsigned_and_float_sku_risks():
    proof = build_mapping_proof(
        [
            {
                "source": "qty",
                "target": "qty",
                "confidence": 0.88,
                "source_type": "INT UNSIGNED",
                "target_type": "NUMBER(38,0)",
                "transform": "none",
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            },
            {
                "source": "score",
                "target": "score",
                "confidence": 0.85,
                "source_type": "DOUBLE",
                "target_type": "DECIMAL",
                "transform": "decimal",
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            },
        ],
        target_columns=[],
        destination_db_type="snowflake",
    )
    codes = {r["code"] for m in proof["mappings"] for r in m["risks"]}
    assert "unsigned_range" in codes
    assert "float_to_decimal" in codes


def test_semi_structured_and_sample_preview():
    proof = build_mapping_proof(
        [{
            "source": "payload",
            "target": "payload",
            "confidence": 0.9,
            "source_type": "JSON",
            "target_type": "VARIANT",
            "transform": "none",
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
            "samples": ['{"a":1}', '{"a":2}'],
            "sample_count": 2,
        }],
        target_columns=[],
        destination_db_type="snowflake",
    )
    row = proof["mappings"][0]
    assert any(r["code"] == "semi_structured" for r in row["risks"])
    assert row["sample_preview"]
    assert row["evidence"]["sample_preview"]


def test_confidence_breakdown_sums_to_display():
    evidence = {
        "create_new": True,
        "name_match": True,
        "type_aligned": True,
        "sample_n": 12,
        "sample_parse_rate": 1.0,
        "score_gap": None,
    }
    display = 0.93
    bd = confidence_breakdown({}, evidence, display)
    assert abs(sum(bd.values()) - display) < 0.002
    assert bd["strategy"] >= bd["sample"]


def test_pipeline_returns_mapping_proof():
    result = run_mapping_pipeline(
        ["id"],
        [],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]}],
        destination_db_type="snowflake",
        use_llm=False,
    )
    assert "mapping_proof" in result
    assert result["mapping_proof"]["dest_mode"] == "create_new"
    assert result["mapping_proof"]["mappings"][0]["confidence"] <= 0.93
    assert "confidence_breakdown" in result["mapping_proof"]["mappings"][0]["evidence"]
