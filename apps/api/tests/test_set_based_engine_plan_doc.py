"""The set-based engine plan stays a plan: required sections, honest scope, no production branch.

This is the regression for the phase-1 design note. It does not import the engine.
"""

from pathlib import Path

PLAN = Path(__file__).resolve().parents[3] / "docs" / "design" / "set-based-engine-plan.md"

REQUIRED_HEADINGS = (
    "# Set-based transform engine — research and plan",
    "## 1. Decision",
    "## 2. What exists today",
    "## 3. Estimate",
    "## 4. Research",
    "## 5. Feature matrix",
    "## 6. Migration rules",
    "## 7. AI-assisted rules",
    "## 8. Architecture",
    "## 9. API contracts",
    "## 10. UI integration",
    "## 11. Phased plan",
    "## 12. Test strategy",
    "## 13. Risks that cut across phases",
    "## 14. Decisions the owner is asked to confirm",
)

# The note must keep naming the constraints the implementation is not allowed to forget.
REQUIRED_PHRASES = (
    "feature/set-based-engine-plan",
    "feature/studio-ui-upgrade",
    "feature/fix-qa-bug-fixes",
    "studio_canvas",
    "studio_schema_browser",
    "at-least-once",
    "Exactly-once is not claimed",
    "99–135",
    "39–52",
    "45–70",
    "nulls_equal",
    "set_operations",
    "recipe_hash",
    "set_hash",
    "TransformStepBuilder",
    "post_load_only",
)


def test_plan_doc_exists_and_covers_the_agreed_outline():
    text = PLAN.read_text(encoding="utf-8")
    missing = [heading for heading in REQUIRED_HEADINGS if heading not in text]
    assert not missing, f"plan is missing headings: {missing}"
    missing_phrases = [phrase for phrase in REQUIRED_PHRASES if phrase not in text]
    assert not missing_phrases, f"plan is missing required phrases: {missing_phrases}"


def test_plan_does_not_claim_a_shipped_set_engine_or_a_production_merge():
    text = PLAN.read_text(encoding="utf-8")
    assert "Status: **proposal**" in text
    assert "No engine or UI code in this change" in text
    assert "Do not commit to it, merge it, or merge into it." in text
    # The wizard labels are an operator contract. The note must not rename them.
    for label in ("Source", "Destination", "Transform", "Map", "Validate", "Run"):
        assert label in text
