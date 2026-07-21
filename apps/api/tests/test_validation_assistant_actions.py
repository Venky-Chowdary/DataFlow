"""Suggested Validate actions must match the blocker class (type vs encoding)."""

from services.validation_assistant import _suggested_actions, explain_validation


def test_type_mismatch_suggests_remap_not_strip():
    actions = _suggested_actions(
        [{
            "id": "g5_dry_run",
            "message": "Dry-run / integrity failed: population (VARCHAR) → population (NUMBER(38,0))",
            "details": {"errors": ["population (VARCHAR) → population (NUMBER(38,0))"]},
        }],
        [],
    )
    kinds = [a["kind"] for a in actions]
    assert "change_target_type" in kinds
    assert "normalize_control_chars" not in kinds
    assert "quarantine_and_rerun" not in kinds
    widen = next(a for a in actions if a["kind"] == "change_target_type")
    assert widen["column"] == "population"
    assert widen["to_type"] == "VARCHAR"


def test_encoding_blocker_still_suggests_strip_quarantine():
    actions = _suggested_actions(
        [{
            "id": "g5_dry_run",
            "message": "Dry-run / integrity failed: format-control character detected (U+200B)",
            "details": {"errors": ["description: format-control character"]},
        }],
        [],
    )
    kinds = [a["kind"] for a in actions]
    assert "normalize_control_chars" in kinds
    assert "quarantine_and_rerun" in kinds


def test_explain_type_mismatch_narrative_mentions_remap():
    explained = explain_validation(
        {
            "passed": False,
            "gates": [{
                "id": "g5_dry_run",
                "status": "block",
                "message": "Dry-run / integrity failed: population (VARCHAR) → population (NUMBER(38,0))",
            }],
            "blockers": [{
                "id": "g5_dry_run",
                "message": "Dry-run / integrity failed: population (VARCHAR) → population (NUMBER(38,0))",
                "details": {"errors": ["population (VARCHAR) → population (NUMBER(38,0))"]},
            }],
            "coercion_report": {"columns": []},
        },
        dest_kind="snowflake",
        use_llm=False,
    )
    assert any(a["kind"] == "change_target_type" for a in explained["suggested_actions"])
    assert not any(a["kind"] == "normalize_control_chars" for a in explained["suggested_actions"])
