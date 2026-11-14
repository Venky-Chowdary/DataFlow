"""UX-6: the PII hold on Validate reads as an instruction, not an API reference.

The message said "Acknowledge with pii_acknowledgement {approved_by, reason}
(API: compliance_acknowledged + acknowledgment_actor/acknowledgment_reason)",
which an operator on the Validate screen cannot act on. The human action
leads; the parameter API and MCP callers need stays in one closing sentence
that the Validate screen drops (validateIssueGrouping.ts).
"""

from __future__ import annotations

from services.preflight_proof_bundle import _compliance_review_message


def test_pii_review_message_names_the_columns_and_the_action_in_words() -> None:
    message = _compliance_review_message(
        {
            "findings": [{"column": "email", "category": "email", "matched_by": "column name"}],
            "risk_score": 0.16,
        }
    )
    assert message.startswith("PII/compliance review required")
    assert "(risk 0.16)" in message
    assert "API:" not in message
    assert "compliance_acknowledged" not in message
    human, _, agents = message.partition(" Agents send ")
    assert "who approved it and why" in human
    assert "pii_acknowledgement" not in human and "{" not in human
    assert agents == "pii_acknowledgement {approved_by, reason}."
