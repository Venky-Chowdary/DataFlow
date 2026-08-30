"""A PII approval may clear only a PII blocker — never a data/route defect.

Validate used to advertise "Approve PII to unlock Execute" whenever the proof
bundle's own blocker list was compliance-shaped, even when a destination or
policy gate was the real reason. The operator then approved, nothing changed,
and the product looked broken while it was in fact correctly fail-closed.
"""

from __future__ import annotations

from services.preflight_policy_gates import apply_policy_gates


def _bundle(*blockers: str) -> dict:
    return {
        "passed": False,
        "transfer_decision": {
            "decision": "review",
            "blockers": list(blockers),
            "compliance_only": True,
        },
    }


def _result(*, blockers: list[dict] | None = None, gates: list[dict] | None = None) -> dict:
    return {
        "passed": False,
        "gates": gates or [{"id": "g1_source", "status": "pass", "message": "ok"}],
        "blockers": blockers or [],
        "proof_bundle": _bundle("PII/compliance review required"),
    }


def test_pii_only_offers_the_unlocking_approval():
    out = apply_policy_gates(_result(), [], validation_mode="strict")
    decision = out["proof_bundle"]["transfer_decision"]
    assert decision["decision"] == "review"
    assert decision["compliance_only"] is True
    pii = [b for b in out["blockers"] if "PII/compliance" in b["message"]]
    assert pii and pii[0]["details"]["compliance_ack_required"] is True


def test_ordinary_blocker_withdraws_the_pii_approval_offer():
    out = apply_policy_gates(
        _result(
            blockers=[
                {
                    "id": "g2_destination",
                    "message": "Destination unreachable",
                    "details": {},
                }
            ]
        ),
        [],
        validation_mode="strict",
    )
    decision = out["proof_bundle"]["transfer_decision"]
    assert decision["compliance_only"] is False
    assert decision["decision"] == "block"
    pii = [b for b in out["blockers"] if "PII/compliance" in b["message"]]
    assert pii and pii[0]["details"]["compliance_ack_required"] is False


def test_blocking_policy_gate_withdraws_the_pii_approval_offer():
    out = apply_policy_gates(
        _result(),
        [
            {
                "id": "gp_sync_mode",
                "status": "block",
                "message": "Incremental requires a primary key on the destination",
                "details": {},
            }
        ],
        validation_mode="strict",
    )
    decision = out["proof_bundle"]["transfer_decision"]
    assert decision["compliance_only"] is False
    pii = [b for b in out["blockers"] if "PII/compliance" in b["message"]]
    assert pii and pii[0]["details"]["compliance_ack_required"] is False
