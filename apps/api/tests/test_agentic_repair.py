"""Agentic repair propose → approve → apply mappings."""

from __future__ import annotations

from services.agentic_repair import (
    apply_actions_to_mappings,
    apply_actions_with_report,
    decide_proposal,
    propose_from_preflight,
    propose_from_quarantine,
)
from services.validation_assistant import _suggested_actions


def test_quarantine_propose_and_approve_applies_mappings() -> None:
    details = [
        {"column": "amount", "reason": "Invalid decimal: 'abc'", "value": "abc"},
        {"column": "when", "reason": "Invalid date", "value": "n/a"},
    ]
    p = propose_from_quarantine(details, job_id="job1")
    assert p.status == "proposed"
    assert p.actions
    assert p.auto_applicable is False

    mappings = [{"source": "amount", "destination": "amount"}, {"source": "when", "destination": "when"}]
    decided = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert decided.status == "applied"
    updated = decided.apply_result["mappings"]
    amount = next(m for m in updated if m["source"] == "amount")
    assert amount.get("destination_type") == "VARCHAR" or amount.get("target_type") == "VARCHAR"


def test_reject_proposal() -> None:
    p = propose_from_quarantine([{"column": "x", "reason": "bad"}], job_id="j2")
    decided = decide_proposal(p.id, approve=False, actor="tester")
    assert decided.status == "rejected"


def test_apply_actions_to_mappings_transform() -> None:
    out = apply_actions_to_mappings(
        [{"source": "ts", "destination": "ts"}],
        [{"kind": "add_transform", "column": "ts", "transform": "to_timestamp"}],
    )
    assert out[0]["transform"] == "to_timestamp"


def test_apply_transform_is_idempotent() -> None:
    actions = [
        {"kind": "add_transform", "column": "ts", "transform": "to_timestamp"},
        {"kind": "add_transform", "column": "ts", "transform": "to_timestamp"},
    ]
    out = apply_actions_to_mappings([{"source": "ts", "destination": "ts"}], actions)
    assert out[0]["transform"] == "to_timestamp"
    assert len(out[0]["transforms"]) == 1


def test_cannot_redecide_applied_proposal() -> None:
    p = propose_from_quarantine([{"column": "x", "reason": "bad"}], job_id="j3")
    mappings = [{"source": "x", "destination": "x"}]
    decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    try:
        decide_proposal(p.id, approve=False, actor="tester")
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "status" in str(exc)


def test_approved_can_later_apply() -> None:
    p = propose_from_quarantine(
        [{"column": "amount", "reason": "Invalid decimal: 'x'", "value": "x"}],
        job_id="j4",
    )
    audit = decide_proposal(p.id, approve=True, actor="tester")
    assert audit.status == "approved"
    mappings = [{"source": "amount", "destination": "amount"}]
    applied = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert applied.status == "applied"


def test_duplicate_key_actions_are_guidance_not_mutative() -> None:
    actions = _suggested_actions(
        blockers=[
            {
                "id": "g9_data_integrity",
                "message": "Data integrity failed: id: duplicate key values (a×2)",
            },
            {
                "id": "g6_target_ddl",
                "message": "Primary key candidate 'id' has 12 duplicate value(s) in source sample",
            },
            {
                "id": "g8_reconciliation",
                "message": "Dry-run reconciliation failed — 12 duplicate target key(s) on id",
            },
        ],
        column_fixes=[],
    )
    kinds = {a.get("kind") for a in actions}
    assert "fix_source_keys" in kinds
    assert "normalize_control_chars" not in kinds
    assert "quarantine_and_rerun" not in kinds
    assert not any(a.get("kind") == "change_target_type" for a in actions)


def test_non_mutative_approve_does_not_fake_applied() -> None:
    pf = {
        "passed": False,
        "blockers": [
            {
                "id": "g9_data_integrity",
                "message": "id: duplicate key values (abc×2)",
            },
            {
                "id": "g6_target_ddl",
                "message": "Primary key candidate 'id' has 2 duplicate value(s) in source sample",
            },
            {
                "id": "g8_reconciliation",
                "message": "Dry-run reconciliation failed — 2 duplicate target key(s) on id",
            },
        ],
        "gates": [
            {"id": "g9_data_integrity", "status": "block", "message": "id: duplicate key values"},
            {"id": "g6_target_ddl", "status": "block", "message": "Primary key candidate 'id' has duplicates"},
            {"id": "g8_reconciliation", "status": "block", "message": "duplicate target key(s) on id"},
        ],
    }
    p = propose_from_preflight(pf, job_id="dup-job")
    assert p.diagnosis.get("mapping_applyable") is False
    assert p.diagnosis.get("root_cause") == "duplicate_identity_keys"
    mappings = [{"source": "id", "destination": "id"}, {"source": "name", "destination": "name"}]
    decided = decide_proposal(
        p.id,
        approve=True,
        actor="tester",
        apply_fn=lambda actions: apply_actions_with_report(mappings, actions),
    )
    assert decided.status == "approved"
    assert decided.apply_result.get("applied") is False
    assert decided.apply_result.get("reason") == "no_mutative_actions"
