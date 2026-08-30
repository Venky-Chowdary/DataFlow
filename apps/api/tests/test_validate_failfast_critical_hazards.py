"""Validate-stage fail-fast: critical hazards must block before Run."""

from __future__ import annotations

import json
from pathlib import Path

from services.ddl_compatibility import (
    _decimal_overflow_issue,
    evaluate_ddl_compatibility,
)
from services.preflight_rules import explain_issue
from services.type_coercion_validator import validate_mapping_coercions

_API_ROOT = Path(__file__).resolve().parents[1]
_PROOF = _API_ROOT / "data" / "proofs" / "validate_failfast_critical_hazards.json"


def test_unknown_dest_schema_blocks_when_table_exists():
    """Existing table + empty introspection must fail-closed (cannot prove columns)."""
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "_id", "target": "_id", "confidence": 0.95}],
        source_schema={"_id": "VARCHAR"},
        target_schema={},
        table_exists=True,
        dest_connected=True,
        dest_db_type="snowflake",
        allow_create=True,
        sync_mode="append",
        destination_table="customers",
    )
    assert not ok
    assert any("Could not load destination schema" in i for i in issues)


def test_create_new_allows_empty_schema_for_non_overwrite():
    """First-run create (SCD2/upsert/append) must not require a live dest schema."""
    ok, issues = evaluate_ddl_compatibility(
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        source_schema={"id": "INTEGER", "name": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="sqlite",
        allow_create=True,
        sync_mode="scd2",
        destination_table="products",
    )
    assert ok, issues
    assert not any("Could not load destination schema" in i for i in issues)


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


def test_padded_trailing_zeros_do_not_invent_an_overflow():
    """'1.50000000' is 1.5 — the writer stores it in (9,2), so Validate must too."""
    assert _decimal_overflow_issue(["1.50000000"], "amount", "NUMBER(9,2)") is None


def test_significant_scale_beyond_the_target_still_blocks():
    issue = _decimal_overflow_issue(["12.123456789012"], "arr_time", "NUMBER(11,8)")
    assert issue and "Decimal capacity overflow" in issue


def test_integer_digits_are_measured_after_padding_is_stripped():
    """Stripping the pad must not hide a genuine integer-width overflow."""
    issue = _decimal_overflow_issue(["12345678901.50000000"], "amount", "NUMBER(9,2)")
    assert issue and "Decimal capacity overflow" in issue


def test_money_scale_within_the_target_is_not_an_overflow():
    assert _decimal_overflow_issue(["1000.00"], "amount", "NUMBER(9,2)") is None


def test_gate_and_writer_agree_on_every_decimal_sample():
    """The Validate forecast and the write-time predicate are one rule."""
    from connectors.writer_common import fits_decimal

    for sample in (
        "1.50000000",
        "12.123456789012",
        "12345678901.50000000",
        "1000.00",
        "0.000000001",
        "-9.87654321",
        "99999999.99",
    ):
        for dest_type, precision, scale in (
            ("NUMBER(9,2)", 9, 2),
            ("NUMBER(11,8)", 11, 8),
        ):
            gate_ok = _decimal_overflow_issue([sample], "amount", dest_type) is None
            writer_ok = fits_decimal(sample, precision, scale, dest_db="snowflake")
            assert gate_ok == writer_ok, (sample, dest_type, gate_ok, writer_ok)


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
