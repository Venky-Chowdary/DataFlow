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


def test_bigint_unsigned_lakehouse_risk():
    proof = build_mapping_proof(
        [{
            "source": "id",
            "target": "id",
            "confidence": 0.9,
            "source_type": "BIGINT UNSIGNED",
            "target_type": "DECIMAL",
            "transform": "none",
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
        }],
        target_columns=[],
        destination_db_type="databricks",
    )
    row = proof["mappings"][0]
    # Auto-widen: unsigned 64-bit → DECIMAL DDL on lakehouse
    assert "DECIMAL" in str(row["dest_native_type"]).upper() or "NUMERIC" in str(row["dest_native_type"]).upper()
    codes = {r["code"] for r in row["risks"]}
    assert "unsigned_bigint_widened" in codes or "unsigned_bigint_range" in codes


def test_bigint_unsigned_forced_signed_still_warns():
    proof = build_mapping_proof(
        [{
            "source": "id",
            "target": "id",
            "confidence": 0.9,
            "source_type": "BIGINT UNSIGNED",
            "target_type": "BIGINT",
            "transform": "none",
            "create_new": False,
            "exists_in_destination": True,
        }],
        target_columns=["id"],
        destination_db_type="postgresql",
    )
    # dest_native still widens from source type for CREATE path; match_existing keeps warn if tgt BIGINT
    row = proof["mappings"][0]
    # Source normalizes to decimal; ddl_type from source is NUMERIC — widened info
    assert any(
        r["code"] in {"unsigned_bigint_widened", "unsigned_bigint_range"}
        for r in row["risks"]
    )


def test_cdc_metadata_and_delivery_posture():
    proof = build_mapping_proof(
        [
            {
                "source": "id",
                "target": "id",
                "confidence": 0.9,
                "source_type": "INTEGER",
                "target_type": "long",
                "transform": "none",
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            },
            {
                "source": "__op",
                "target": "__op",
                "confidence": 0.9,
                "source_type": "VARCHAR",
                "target_type": "string",
                "transform": "none",
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            },
            {
                "source": "__deleted",
                "target": "__deleted",
                "confidence": 0.9,
                "source_type": "BOOLEAN",
                "target_type": "boolean",
                "transform": "none",
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            },
        ],
        target_columns=[],
        destination_db_type="iceberg",
        sync_mode="cdc",
    )
    assert proof["summary"]["cdc_detected"] is True
    assert any(r["code"] == "cdc_delivery_posture" for r in proof["global_risks"])
    meta = next(m for m in proof["mappings"] if m["source"] == "__op")
    assert any(r["code"] == "cdc_metadata_column" for r in meta["risks"])
    tomb = next(m for m in proof["mappings"] if m["source"] == "__deleted")
    assert any(r["code"] == "cdc_tombstone" for r in tomb["risks"])


def test_cdc_append_only_sink_surfaces_in_mapping_proof():
    proof = build_mapping_proof(
        [{
            "source": "id",
            "target": "id",
            "confidence": 0.95,
            "source_type": "INTEGER",
            "target_type": "integer",
            "transform": "none",
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
        }],
        target_columns=[],
        destination_db_type="csv",
        sync_mode="cdc",
    )
    assert any(r["code"] == "cdc_append_only_sink" for r in proof["global_risks"])
    assert any(r["severity"] == "error" for r in proof["global_risks"] if r["code"] == "cdc_append_only_sink")


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


def test_mapping_proof_or_build_prefers_persisted():
    from services.mapping_proof import mapping_proof_or_build

    existing = {
        "dest_mode": "match_existing",
        "mappings": [{"source": "a", "target": "b", "confidence": 0.8}],
        "summary": {"mapped_count": 1},
    }
    out = mapping_proof_or_build(
        [{"source": "a", "target": "b", "confidence": 0.5}],
        existing=existing,
        destination_db_type="postgresql",
    )
    assert out is existing

    rebuilt = mapping_proof_or_build(
        [{
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
            "confidence": 0.9,
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
        }],
        destination_db_type="snowflake",
        sync_mode="cdc",
    )
    assert rebuilt["mappings"]
    assert rebuilt["summary"]["mapped_count"] == 1
    assert rebuilt.get("sync_mode") == "cdc" or rebuilt["summary"].get("cdc_detected")
