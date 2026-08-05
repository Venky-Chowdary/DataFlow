"""Encoding blockers must surface Fix bad data — not Map — as the primary CTA."""

from services.preflight_rules import enrich_blockers, explain_gate


def test_encoding_gate_guidance_prefers_fix_bad_data():
    guidance = explain_gate(
        "g5_dry_run",
        "Dry-run / integrity failed: format-control character detected (U+200B)",
    )
    kinds = [a["kind"] for a in guidance.get("suggested_actions") or []]
    assert "open_bad_data_fix" in kinds
    assert "review_mappings" not in kinds


def test_enrich_blockers_promotes_nested_encoding_cta():
    enriched = enrich_blockers(
        [{
            "id": "g5_dry_run",
            "message": "Dry-run / integrity failed",
            "details": {
                "errors": ["description: format-control character (U+200B)"],
            },
        }],
    )
    actions = enriched[0]["guidance"]["suggested_actions"]
    kinds = [a["kind"] for a in actions]
    assert kinds == ["open_bad_data_fix"]
    assert actions[0]["label"] == "Fix bad data…"


def test_encoding_column_name_does_not_steal_type_mismatch_cta():
    """Column names containing 'encoding' must not open Fix bad data."""
    guidance = explain_gate(
        "g5_dry_run",
        "Dry-run / integrity failed: encoding_id (VARCHAR) → encoding_id (NUMBER(38,0))",
    )
    kinds = [a["kind"] for a in guidance.get("suggested_actions") or []]
    assert "open_bad_data_fix" not in kinds
    assert "review_mappings" in kinds


def test_nested_encoding_id_type_mismatch_does_not_steal_cta():
    """Nested dry-run errors mentioning encoding_id must stay on Map."""
    enriched = enrich_blockers(
        [{
            "id": "g5_dry_run",
            "message": "Dry-run / integrity failed",
            "details": {
                "errors": [
                    "encoding_id (VARCHAR) → encoding_id (NUMBER(38,0))",
                    "status (VARCHAR) → status (BOOLEAN)",
                ],
            },
        }],
    )
    kinds = [a["kind"] for a in enriched[0]["guidance"].get("suggested_actions") or []]
    assert "open_bad_data_fix" not in kinds
    assert "review_mappings" in kinds
