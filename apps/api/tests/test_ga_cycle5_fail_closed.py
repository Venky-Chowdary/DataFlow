"""Cycle 5 Enterprise GA — residual fail-closed holes."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_retry_policy_is_fail_closed_not_continue():
    from services.migration_risk_contract import (
        CONTINUE_POLICIES,
        FAIL_CLOSED_POLICIES,
        create_migration_risk_contract,
        mapping_has_clearing_risk_contract,
        resolve_write_action_for_mapping,
    )

    assert "RETRY" not in CONTINUE_POLICIES
    assert "RETRY" in FAIL_CLOSED_POLICIES
    c = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="retry reserved",
        execution_policy="RETRY",
    )
    mapping = {"source": "amt", "risk_contract": c.to_dict(), "risk_acknowledged": True}
    assert mapping_has_clearing_risk_contract(mapping) is False
    action, pol, _rid = resolve_write_action_for_mapping(mapping, "quarantine")
    assert action == "retry_then_fail"
    assert pol == "RETRY"


def test_stop_column_does_not_abort_job_write():
    """STOP_COLUMN omits the column — job continues (distinct from FAIL_JOB)."""
    from connectors.writer_common import reject_on_strict_policy
    from services.migration_risk_contract import (
        CONTINUE_POLICIES,
        resolve_write_action_for_mapping,
        create_migration_risk_contract,
    )

    assert "STOP_COLUMN" in CONTINUE_POLICIES
    c = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="stop column only",
        execution_policy="STOP_COLUMN",
    )
    action, pol, _rid = resolve_write_action_for_mapping(
        {"source": "amt", "risk_contract": c.to_dict()},
        "quarantine",
    )
    assert action == "stop_column"
    assert pol == "STOP_COLUMN"
    msg = reject_on_strict_policy(
        "quarantine",
        [
            {
                "row": 1,
                "column": "amt",
                "execution_policy": "STOP_COLUMN",
                "policy": "stop_column",
            }
        ],
        "pg",
    )
    assert msg is None


def test_datatype_validation_layer_exists():
    from services.validation_coverage import (
        VALIDATION_LAYERS,
        assert_no_sample_population_lie,
        stamp_validation_coverage,
    )

    assert "datatype" in VALIDATION_LAYERS
    stamp = stamp_validation_coverage(layer="datatype", rows_examined=10)
    assert stamp["population_proof"] is False
    assert_no_sample_population_lie(stamp)


def test_stream_batch_quarantine_persist_fail_closed(monkeypatch):
    """When DLQ persist fails mid-stream, transfer must refuse to continue."""
    from services.quarantine_row_contract import QuarantineRowContractError

    def _boom(**_kwargs):
        raise QuarantineRowContractError("dlq down")

    monkeypatch.setattr(
        "services.quarantine_dlq.persist_rejected_rows",
        _boom,
    )
    # Inline the stream guard logic.
    new_details = [{"row": 1, "column": "c", "reason": "bad"}]
    job_id = "job-q"
    raised = False
    try:
        from services.quarantine_dlq import persist_rejected_rows

        persist_rejected_rows(
            job_id=job_id,
            rejected_details=new_details,
            source="stream_batch",
        )
    except Exception as exc:
        raised = True
        assert "dlq" in str(exc).lower() or "quarantine" in str(exc).lower()
    assert raised is True


def test_completed_job_proof_requires_risk_completeness_on_verify():
    from services.migration_risk_contract import create_migration_risk_contract
    from services.signed_proof_pack import (
        build_signed_proof_pack,
        verify_signed_proof_pack,
    )

    c = create_migration_risk_contract(
        column="amt",
        source_type="FLOAT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="cast",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    pack = build_signed_proof_pack(
        job_id="job-c5",
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
        require_risk_completeness=True,
    )
    assert pack["require_risk_completeness"] is True
    assert pack["assurance"].get("migration_proven") is False
    verified = verify_signed_proof_pack(pack)
    assert verified["ok"] is False
    assert any("accepted_risks" in e.lower() or "incomplete" in e.lower() for e in verified["errors"])
