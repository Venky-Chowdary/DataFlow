"""Cycle 4 Enterprise GA — residual fail-closed holes."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_hydrate_stamps_destination_table_on_unsigned_draft():
    from services.preflight_service import _hydrate_risk_contract

    draft = {
        "column": "amt",
        "source_type": "FLOAT",
        "destination_type": "DECIMAL(12,4)",
        "execution_policy": "CAST_AND_CONTINUE",
        "approved_by": "admin@dataflow.app",
        "reason": "IEEE float accepted",
        "expected_precision_loss": True,
    }
    out = _hydrate_risk_contract(
        {"source": "amt", "risk_contract": draft},
        table="orders",
        migration_id="mig-1",
    )
    assert out is not None
    assert out["table"] == "orders"
    assert out["migration_id"] == "mig-1"
    assert out["loss_classification"]
    assert out["signature"].startswith("mrc-sha256:")


def test_proof_pack_strips_proven_when_accepted_risks_missing():
    from services.migration_risk_contract import create_migration_risk_contract
    from services.signed_proof_pack import build_signed_proof_pack

    c = create_migration_risk_contract(
        column="amt",
        source_type="FLOAT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="cast",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    pack = build_signed_proof_pack(
        job_id="job-1",
        reconciliation={
            "passed": True,
            "source_checksum": "aaa",
            "target_checksum": "aaa",
            "phase": "post_write_verified",
            "assurance_level": "full_checksum",
            "checksum_match": True,
        },
        accepted_risks=[],
        expected_risks_from_mappings=[c],
        job_success=True,
    )
    assert pack["assurance"].get("migration_proven") is False
    assert pack["proof_incomplete_reasons"]
    assert "accepted_risks" in pack["proof_incomplete_reasons"][0].lower() or (
        "incomplete" in pack["proof_incomplete_reasons"][0].lower()
    )


def test_root_cause_absorbs_risk_contract_blocker():
    from services.root_cause_engine import build_root_causes

    roots = build_root_causes(
        {
            "gates": [
                {
                    "id": "g3_schema_contract",
                    "status": "block",
                    "message": "Lossy type coercion: amt (FLOAT) → amt (INTEGER)",
                    "details": {"fidelity_collapse": True, "columns": ["amt"]},
                },
                {
                    "id": "g6_target_ddl",
                    "status": "block",
                    "message": "Lossy type coercion: amt (FLOAT) → amt (INTEGER)",
                    "details": {"columns": ["amt"]},
                },
            ],
            "blockers": [
                {
                    "id": "proof_bundle",
                    "message": "Migration Risk Contract required (execution policy) for: amt",
                    "details": {"columns": ["amt"]},
                }
            ],
            "sample_rows": [{"amt": 1.5}],
            "row_count": 1000,
        }
    )
    fidelity = [r for r in roots if r.kind == "fidelity_collapse"]
    assert fidelity, roots
    absorbed = set(fidelity[0].absorbed_blocker_ids)
    assert "g3_schema_contract" in absorbed
    assert "g6_target_ddl" in absorbed
    assert "proof_bundle" in absorbed
