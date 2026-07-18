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
