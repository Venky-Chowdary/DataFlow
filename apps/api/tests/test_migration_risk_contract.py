"""Migration Risk Contract — boolean Accept Risk is not an execution contract."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.migration_risk_contract import (  # noqa: E402
    CONTINUE_POLICIES,
    DEFAULT_EXECUTION_POLICY,
    boolean_ack_is_execution_contract,
    contract_clears_validate_block,
    create_migration_risk_contract,
    lossy_mappings_missing_risk_contracts,
    mapping_has_clearing_risk_contract,
    sign_risk_contract,
    verify_risk_contract,
)


def test_boolean_ack_is_never_an_execution_contract():
    assert boolean_ack_is_execution_contract() is False


def test_default_execution_policy_is_fail_job():
    assert DEFAULT_EXECUTION_POLICY == "FAIL_JOB"


def test_create_requires_actor_and_reason():
    with pytest.raises(ValueError, match="approved_by"):
        create_migration_risk_contract(
            column="country_auto_detected",
            source_type="TEXT",
            destination_type="INTEGER",
            approved_by="",
            reason="ok",
        )
    with pytest.raises(ValueError, match="reason"):
        create_migration_risk_contract(
            column="country_auto_detected",
            source_type="TEXT",
            destination_type="INTEGER",
            approved_by="admin@dataflow.app",
            reason="  ",
        )


def test_fail_job_contract_does_not_clear_validate():
    c = create_migration_risk_contract(
        column="country_auto_detected",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Acknowledged lossy path — keep job fail-closed until cast policy chosen",
        execution_policy="FAIL_JOB",
        rows_sampled=25,
        estimated_rows=100,
    )
    assert c.execution_policy == "FAIL_JOB"
    assert verify_risk_contract(c) is True
    assert contract_clears_validate_block(c) is False


def test_cast_and_continue_clears_only_when_signed():
    c = create_migration_risk_contract(
        column="country_auto_detected",
        source_type="TEXT COLLATE UTF8MB4_0900_AI_CI",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Boolean-like TEXT flags; cast empty/non-numeric to NULL via quarantine",
        execution_policy="CAST_AND_CONTINUE",
        quarantine_policy="QUARANTINE_ROW_on_cast_failure",
        expected_precision_loss=True,
        expected_nulls=True,
        rows_sampled=25,
        estimated_rows=100_000,
    )
    assert c.execution_policy in CONTINUE_POLICIES
    assert contract_clears_validate_block(c) is True

    tampered = {**c.to_dict(), "execution_policy": "FAIL_JOB"}
    # Signature no longer matches body → must not clear.
    assert verify_risk_contract(tampered) is False
    assert contract_clears_validate_block(tampered) is False


def test_signature_is_deterministic_for_same_body():
    c = create_migration_risk_contract(
        column="x",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="a@b.c",
        reason="test",
        execution_policy="CAST_AND_CONTINUE",
    )
    body = c.to_dict()
    assert sign_risk_contract(body) == c.signature
    assert sign_risk_contract(body) == sign_risk_contract(body)


def test_legacy_boolean_ack_without_contract_is_incomplete():
    mappings = [
        {
            "source": "country_auto_detected",
            "target": "country_auto_detected",
            "fidelity": "lossy_cast",
            "risk_acknowledged": True,
            # no risk_contract
        }
    ]
    assert mapping_has_clearing_risk_contract(mappings[0]) is False
    missing = lossy_mappings_missing_risk_contracts(mappings)
    assert missing == ["country_auto_detected"]


def test_continue_contract_on_mapping_clears():
    c = create_migration_risk_contract(
        column="referral_credit_processed",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Cast TEXT flags to INTEGER; quarantine non-numeric",
        execution_policy="CAST_AND_CONTINUE",
    )
    mapping = {
        "source": "referral_credit_processed",
        "risk_acknowledged": True,
        "fidelity": "lossy_cast",
        "risk_contract": c.to_dict(),
    }
    assert mapping_has_clearing_risk_contract(mapping) is True
    assert lossy_mappings_missing_risk_contracts([mapping]) == []


def test_create_stamps_migration_id_table_loss_classification():
    c = create_migration_risk_contract(
        column="amt",
        source_type="DECIMAL(18,4)",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Collapse precision for legacy sink",
        execution_policy="CAST_AND_CONTINUE",
        migration_id="mig-99",
        table="orders",
        expected_precision_loss=True,
        expected_truncation=True,
    )
    d = c.to_dict()
    assert d["migration_id"] == "mig-99"
    assert d["table"] == "orders"
    assert d["loss_classification"] == "truncation"
    assert verify_risk_contract(c) is True


def test_safe_normalize_mutate_does_not_require_risk_contract():
    mappings = [
        {
            "source": "email",
            "target": "email",
            "fidelity": "mutate",
            "transform": "email",
            "approved": True,
        }
    ]
    assert lossy_mappings_missing_risk_contracts(mappings) == []


def test_trim_id_mutate_does_not_require_risk_contract():
    """Engine preserves trim_id on Validate; must match Map Ready trim path."""
    mappings = [
        {
            "source": "id",
            "target": "id",
            "fidelity": "mutate",
            "transform": "trim_id",
            "approved": True,
        },
        {
            "source": "stripe_customer_id",
            "target": "stripe_customer_id",
            "fidelity": "mutate",
            "transform": "trim_id",
        },
    ]
    assert lossy_mappings_missing_risk_contracts(mappings) == []


def test_assert_mappings_executable_blocks_boolean_ack_without_contract():
    from services.mapping_pipeline import assert_mappings_executable

    with pytest.raises(ValueError, match="Risk Contract"):
        assert_mappings_executable(
            [
                {
                    "source": "amt",
                    "target": "amt",
                    "fidelity": "lossy_cast",
                    "risk_acknowledged": True,
                    "approved": True,
                    "requires_review": False,
                }
            ]
        )


def test_assert_mappings_executable_signs_unsigned_draft_like_validate():
    """Validate green + Map unsigned draft must not fail Execute (hydrate SSOT)."""
    from services.mapping_pipeline import assert_mappings_executable
    from services.migration_risk_contract import mapping_has_clearing_risk_contract

    mappings = [
        {
            "source": "email_verified",
            "target": "email_verified",
            "fidelity": "cast",
            "risk_acknowledged": True,
            "approved": True,
            "requires_review": False,
            "risk_contract": {
                "column": "email_verified",
                "source_type": "TIMESTAMP",
                "destination_type": "TIMESTAMP",
                "execution_policy": "CAST_AND_CONTINUE",
                "approved_by": "admin@dataflow.app",
                "reason": "Operator acknowledged cast/quarantine path",
                "expected_precision_loss": True,
                "quarantine_policy": "holdout_rejected_rows",
                "retry_policy": "none",
                "rollback_strategy": "DOCUMENT_ONLY",
            },
        }
    ]
    assert_mappings_executable(mappings)
    assert mapping_has_clearing_risk_contract(mappings[0])
    assert str(mappings[0]["risk_contract"].get("signature") or "").startswith(
        "mrc-sha256:"
    )


def test_run_file_preflight_signs_draft_contract_and_approves():
    """Map draft CAST_AND_CONTINUE contract is signed on Validate and unlocks approve."""
    from services.preflight_service import run_file_preflight

    col = "country_auto_detected"
    src_type = "TEXT COLLATE UTF8MB4_0900_AI_CI"
    draft = {
        "column": col,
        "source_type": src_type,
        "destination_type": "INTEGER",
        "execution_policy": "CAST_AND_CONTINUE",
        "approved_by": "admin@dataflow.app",
        "reason": "Cast TEXT flags; quarantine non-numeric",
        "expected_precision_loss": True,
        "quarantine_policy": "QUARANTINE_ROW_on_cast_failure",
        "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY",
    }
    result = run_file_preflight(
        columns=[col],
        column_types={col: src_type},
        row_count=3,
        mappings=[
            {
                "source": col,
                "target": col,
                "confidence": 0.92,
                "source_type": src_type,
                "target_type": "INTEGER",
                "create_new": True,
                "fidelity": "lossy_cast",
                "type_narrowing": True,
                "risk_acknowledged": True,
                "user_override": True,
                "risk_contract": draft,
            }
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mysql",
        sync_mode="full_refresh_append",
        sample_rows=[{col: "0"}, {col: "1"}, {col: "0"}],
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    decision = ((result.get("proof_bundle") or {}).get("transfer_decision") or {}).get(
        "decision"
    )
    assert decision == "approve", result.get("proof_bundle")
    rc = (result.get("proof_bundle") or {}).get("risk_contracts") or {}
    assert rc.get("incomplete") is False


def test_proof_bundle_approves_when_continue_contract_present():
    from services.migration_risk_contract import create_migration_risk_contract
    from services.preflight_proof_bundle import build_preflight_proof_bundle

    c = create_migration_risk_contract(
        column="country_auto_detected",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Cast TEXT flags; quarantine non-numeric",
        execution_policy="CAST_AND_CONTINUE",
    )
    bundle = build_preflight_proof_bundle(
        columns=["country_auto_detected"],
        sample_rows=[{"country_auto_detected": "0"}],
        mappings=[
            {
                "source": "country_auto_detected",
                "target": "country_auto_detected",
                "confidence": 0.92,
                "fidelity": "lossy_cast",
                "risk_acknowledged": True,
                "risk_contract": c.to_dict(),
            }
        ],
        source_schemas=[
            {"name": "country_auto_detected", "inferred_type": "TEXT", "samples": ["0"]},
        ],
        source_records=[{"country_auto_detected": "0"}],
        validation_mode="strict",
        confidence_threshold=0.85,
    )
    decision = (bundle.get("transfer_decision") or {}).get("decision")
    assert decision == "approve", bundle
    assert not (bundle.get("risk_contracts") or {}).get("incomplete")


def test_proof_bundle_blocks_approve_when_risk_contracts_incomplete():
    """Execute-approve must not follow boolean ack alone."""
    from services.preflight_proof_bundle import build_preflight_proof_bundle

    bundle = build_preflight_proof_bundle(
        columns=["country_auto_detected"],
        sample_rows=[{"country_auto_detected": "0"}, {"country_auto_detected": "1"}],
        mappings=[
            {
                "source": "country_auto_detected",
                "target": "country_auto_detected",
                "confidence": 0.92,
                "fidelity": "lossy_cast",
                "risk_acknowledged": True,
            }
        ],
        source_schemas=[
            {"name": "country_auto_detected", "inferred_type": "TEXT", "samples": ["0", "1"]},
        ],
        source_records=[{"country_auto_detected": "0"}, {"country_auto_detected": "1"}],
        validation_mode="strict",
        confidence_threshold=0.85,
    )
    decision = (bundle.get("transfer_decision") or {}).get("decision")
    assert decision != "approve", bundle
    blockers = (bundle.get("transfer_decision") or {}).get("blockers") or []
    blob = " ".join(str(b) for b in blockers).lower() + json_blob(bundle)
    assert (
        "risk contract" in blob
        or "migration risk" in blob
        or "execution policy" in blob
    ), bundle


def json_blob(obj) -> str:
    import json

    return json.dumps(obj, default=str).lower()
