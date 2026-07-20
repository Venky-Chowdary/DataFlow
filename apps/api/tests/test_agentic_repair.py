"""Agentic repair propose → approve → apply mappings."""

from __future__ import annotations

from services.agentic_repair import (
    apply_actions_to_mappings,
    decide_proposal,
    propose_from_quarantine,
)


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
        apply_fn=lambda actions: {
            "applied": True,
            "mappings": apply_actions_to_mappings(mappings, actions),
        },
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
        apply_fn=lambda actions: {
            "applied": True,
            "mappings": apply_actions_to_mappings(mappings, actions),
        },
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
        apply_fn=lambda actions: {
            "applied": True,
            "mappings": apply_actions_to_mappings(mappings, actions),
        },
    )
    assert applied.status == "applied"
