"""Bugbot P0 fixes — quarantine DLQ dedupe + legacy risk-contract verify."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_final_quarantine_persist_skips_already_written_prefix(monkeypatch):
    from src.transfer.engine import _persist_job_quarantine

    calls: list[list] = []

    def _capture(**kwargs):
        calls.append(list(kwargs.get("rejected_details") or []))
        return {"quarantine_durable": True, "rows": len(calls[-1])}

    monkeypatch.setattr("services.quarantine_dlq.persist_rejected_rows", _capture)
    monkeypatch.setattr(
        "services.quarantine_dlq.assert_quarantine_durable_or_raise",
        lambda _s: None,
    )

    details = [{"row": 1}, {"row": 2}, {"row": 3}]
    already = [2]
    dest = {"rejected_details": details}
    _persist_job_quarantine("job-dedupe", dest, already_persisted=already)
    assert len(calls) == 1
    assert calls[0] == [{"row": 3}]
    assert dest["quarantine_dlq_persisted_count"] == 3
    assert already[0] == 3


def test_final_quarantine_persist_noop_when_fully_checkpointed(monkeypatch):
    from src.transfer.engine import _persist_job_quarantine

    calls: list[list] = []

    def _capture(**kwargs):
        calls.append(list(kwargs.get("rejected_details") or []))
        return {"ok": True}

    monkeypatch.setattr("services.quarantine_dlq.persist_rejected_rows", _capture)
    monkeypatch.setattr(
        "services.quarantine_dlq.assert_quarantine_durable_or_raise",
        lambda _s: None,
    )

    details = [{"row": 1}, {"row": 2}]
    dest = {
        "rejected_details": details,
        "quarantine_dlq_persisted_count": 2,
    }
    _persist_job_quarantine("job-full", dest)
    assert calls == []
    assert dest.get("quarantine_durable") is True


def test_legacy_signed_dict_still_clears_and_resolves_write_action():
    """Contracts signed before migration_id/table fields must still verify."""
    from services.migration_risk_contract import (
        contract_clears_validate_block,
        mapping_risk_contract,
        resolve_write_action_for_mapping,
        sign_risk_contract,
    )

    legacy = {
        "risk_id": "mrc-legacy",
        "severity": "high",
        "root_cause": "TEXT→INTEGER",
        "column": "amount",
        "source_type": "TEXT",
        "destination_type": "INTEGER",
        "transform": None,
        "rows_sampled": 10,
        "estimated_rows": None,
        "expected_failure_pct": None,
        "expected_precision_loss": True,
        "expected_truncation": False,
        "expected_nulls": False,
        "execution_policy": "FAIL_JOB",
        "quarantine_policy": "holdout_rejected_rows",
        "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "ops",
        "approved_at": "2024-01-01T00:00:00+00:00",
        "reason": "legacy signed before migration_id field",
        "proof_pack_ref": None,
        "mapping_hash": "",
        "plan_id": None,
        "target": "amount",
        "version": 1,
        "metadata": {},
        # Intentionally omit migration_id / table / loss_classification
    }
    legacy["signature"] = sign_risk_contract(legacy)

    assert contract_clears_validate_block(legacy) is False  # FAIL_JOB is not continue
    c = mapping_risk_contract({"risk_contract": legacy})
    assert c is not None
    assert c.execution_policy == "FAIL_JOB"
    action, pol, _rid = resolve_write_action_for_mapping(
        {"risk_contract": legacy},
        "quarantine",
    )
    assert action == "fail"
    assert pol == "FAIL_JOB"

    continue_legacy = {**legacy, "execution_policy": "CAST_AND_CONTINUE"}
    continue_legacy["signature"] = sign_risk_contract(continue_legacy)
    assert contract_clears_validate_block(continue_legacy) is True
