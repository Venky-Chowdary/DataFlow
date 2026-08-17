"""Internal proof-blocker ids must never reach an operator surface.

`proof_0` is a position in the proof bundle, not a reason a transfer cannot run.
Every explain surface (issue cards, narrative sentence, per-gate bullets) must
name the cause from the blocker message instead.
"""

from __future__ import annotations

from services.blocker_titles import blocker_title, is_internal_blocker_id
from services.validation_assistant import explain_validation


def _pii_blocker() -> dict:
    return {
        "id": "proof_0",
        "message": "PII/compliance review required",
        "details": {
            "compliance_ack_required": True,
            "remediation_kind": "acknowledge_compliance",
        },
    }


def _ddl_blocker() -> dict:
    return {
        "id": "g6_target_ddl",
        "message": "Value width overflow: phone sample max 15 chars exceeds phone (VARCHAR(6))",
        "details": {},
    }


def test_internal_id_detection():
    assert is_internal_blocker_id("proof_0")
    assert is_internal_blocker_id("PROOF_12")
    assert not is_internal_blocker_id("g6_target_ddl")
    assert not is_internal_blocker_id("")


def test_blocker_title_names_the_cause_for_proof_ids():
    assert blocker_title("proof_0", "PII/compliance review required") == (
        "PII/compliance review required"
    )
    # First clause only — the operator gets a title, not a paragraph.
    assert blocker_title(
        "proof_1",
        "Row conservation unproven; destination COUNT(*) was not captured",
    ) == "Row conservation unproven"
    assert blocker_title("proof_2", "") == "Transfer proof blocker"
    # A catalog title still wins for named gates.
    assert blocker_title("g6_target_ddl", "anything", catalog_title="Target DDL") == (
        "Target DDL"
    )
    # A catalog title that is itself the internal id is refused.
    assert blocker_title("proof_0", "PII/compliance review required", catalog_title="proof_0") == (
        "PII/compliance review required"
    )


def test_blocker_title_truncates_long_clauses():
    title = blocker_title("proof_0", "x" * 400)
    assert len(title) <= 72
    assert title.endswith("…")


def _explain(blockers: list[dict]) -> dict:
    return explain_validation(
        {"passed": False, "gates": [], "blockers": blockers},
        use_llm=False,
    )


def test_explain_never_emits_internal_ids_for_pii_only():
    out = _explain([_pii_blocker()])
    blob = " ".join(
        [out["narrative"], out["summary"], *(str(i["title"]) for i in out["issues"])]
    )
    assert "proof_0" not in blob
    assert "PII/compliance review required" in blob


def test_explain_never_emits_internal_ids_with_mixed_blockers():
    out = _explain([_ddl_blocker(), _pii_blocker()])
    titles = [str(i["title"]) for i in out["issues"]]
    assert "proof_0" not in " ".join([out["narrative"], *titles])
    # Both causes are still named — approval never hides the real blocker.
    assert any("PII" in t for t in titles)
    assert any("width overflow" in t or "DDL" in t for t in titles)
