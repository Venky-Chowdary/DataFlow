"""Validate-stage fail-fast: critical hazards must block before Run."""

from __future__ import annotations

import json
from pathlib import Path

from services.ddl_compatibility import evaluate_ddl_compatibility
from services.preflight_rules import explain_issue
from services.type_coercion_validator import validate_mapping_coercions

_API_ROOT = Path(__file__).resolve().parents[1]
_PROOF = _API_ROOT / "data" / "proofs" / "validate_failfast_critical_hazards.json"


def test_unknown_dest_schema_blocks_non_overwrite():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "_id", "target": "_id", "confidence": 0.95}],
        source_schema={"_id": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="snowflake",
        allow_create=True,
        sync_mode="append",
        destination_table="customers",
    )
    assert not ok
    assert any("Could not load destination schema" in i for i in issues)


def test_overwrite_allows_empty_schema_for_recreate():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_schema={"id": "INTEGER"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
        sync_mode="full_refresh_overwrite",
        destination_table="customers",
    )
    assert ok
    assert not any("Could not load destination schema" in i for i in issues)


def test_decimal_capacity_overflow_blocks_at_validate():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "amt", "target": "amount", "confidence": 0.99}],
        source_schema={"amt": "DECIMAL"},
        target_schema={"amount": "NUMBER(10,2)"},
        table_exists=True,
        dest_connected=True,
        dest_db_type="snowflake",
        sample_rows=[{"amt": "12345678901.99"}],
    )
    assert not ok
    assert any("Decimal capacity overflow" in i for i in issues)


def test_create_new_metadata_allows_missing_column():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[
            {
                "source": "_id",
                "target": "_id",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
                "confidence": 0.95,
            }
        ],
        source_schema={"_id": "VARCHAR"},
        target_schema={"id": "DECIMAL", "name": "VARCHAR"},
        table_exists=True,
        dest_connected=True,
        dest_db_type="snowflake",
        sync_mode="append",
        destination_table="customers",
    )
    assert ok


def test_lossy_coercion_always_blocks_regardless_of_confidence():
    issues = validate_mapping_coercions(
        [{"source": "_id", "target": "id", "confidence": 0.99}],
        source_types={"_id": "VARCHAR"},
        target_types={"id": "DECIMAL"},
        schema_policy="manual_review",
        confidence_floor=0.75,
    )
    assert issues
    assert issues[0]["severity"] == "block"
    assert issues[0]["lossy"] is True


def test_disconnected_dest_still_surfaces_schema_issues():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "email", "target": "missing_col", "confidence": 0.9}],
        source_schema={"email": "VARCHAR"},
        target_schema={"email": "TEXT"},
        table_exists=True,
        dest_connected=False,
        dest_db_type="postgresql",
    )
    assert not ok
    assert any("does not exist" in i for i in issues)


def test_remediation_for_unknown_schema_and_decimal():
    for msg, needle in [
        (
            "Could not load destination schema for existing target — Validate cannot prove mapped columns exist.",
            "refresh",
        ),
        (
            "Decimal capacity overflow: amount (NUMBER(10,2)) cannot hold sample value '12345678901.99'",
            "widen",
        ),
    ]:
        explained = explain_issue(msg)
        blob = f"{explained.get('fix', '')} {explained.get('why', '')}".lower()
        assert needle in blob


def test_write_validate_failfast_proof():
    proof = {
        "title": "Validate fail-fast for critical write hazards",
        "principle": "Run should only surface operational failures (timeouts, connectivity); schema/data hazards block at Validate.",
        "checks": [
            "unknown dest schema on non-overwrite → BLOCK",
            "decimal capacity overflow → BLOCK",
            "create_new metadata preserved → missing column allowed (ADD COLUMN)",
            "lossy coercion always BLOCK (not confidence-softened)",
            "disconnected dest still surfaces schema issues",
            "remediations include why + fix for new hazard classes",
        ],
    }
    _PROOF.parent.mkdir(parents=True, exist_ok=True)
    _PROOF.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    assert _PROOF.is_file()
