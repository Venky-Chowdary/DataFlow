"""Suggested Validate actions must match the blocker class (type vs encoding)."""

from services.validation_assistant import (
    _is_duplicate_key_blocker,
    _suggested_actions,
    explain_validation,
)


def test_null_empty_blocker_is_not_labeled_duplicate_identity():
    """G9 why-boilerplate mentions 'duplicate keys' — must not steal the root cause."""
    blob = (
        "The sample violates one or more integrity rules: duplicate keys, required nulls, "
        "financial precision loss, or encoding anomalies. "
        "googleId: 68% null/empty (max 5% for required field)"
    )
    assert _is_duplicate_key_blocker(blob, {"g9_data_integrity"}) is False
    assert _is_duplicate_key_blocker(
        "duplicate key values in 'id': 2 keys repeat",
        {"g9_data_integrity"},
    ) is True


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
    # Dialect text invent (TEXT/VARCHAR) — never keep truncated NUMBER(38,0.
    assert widen["to_type"].upper() in {"VARCHAR", "TEXT", "STRING"}


def test_destination_auth_suggests_check_connection_not_strip():
    actions = _suggested_actions(
        [{
            "id": "g2_destination",
            "message": "Destination error: Authentication failed. Check the username/password and the Auth source field.",
            "details": {},
        }],
        [],
    )
    kinds = [a["kind"] for a in actions]
    assert "check_connection" in kinds
    assert "normalize_control_chars" not in kinds
    assert "quarantine_and_rerun" not in kinds


def test_encoding_blocker_suggests_single_fix_bad_data_cta():
    """Strip/Quarantine live inside the drawer — UI must not stack three encoding buttons."""
    actions = _suggested_actions(
        [{
            "id": "g5_dry_run",
            "message": "Dry-run / integrity failed: format-control character detected (U+200B)",
            "details": {"errors": ["description: format-control character"]},
        }],
        [],
    )
    kinds = [a["kind"] for a in actions]
    assert kinds.count("open_bad_data_fix") == 1
    assert "normalize_control_chars" not in kinds
    assert "quarantine_and_rerun" not in kinds
    assert next(a for a in actions if a["kind"] == "open_bad_data_fix")["label"] == "Fix bad data…"


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


def test_population_fit_overflow_reaches_explain_remap():
    explained = explain_validation(
        {
            "passed": False,
            "gates": [{
                "id": "g3f_population_fit",
                "status": "block",
                "message": "1 value(s) cannot fit NUMBER(9,6)",
            }],
            "blockers": [{
                "id": "g3f_population_fit",
                "message": "1 value(s) in DEP_TIME do not fit NUMBER(9,6)",
                "details": {},
            }],
            "coercion_report": {"columns": []},
            "population_fit": {
                "evidence": "exact",
                "findings": [{
                    "source": "DEP_TIME",
                    "target": "DEP_TIME",
                    "target_type": "NUMBER(9,6)",
                    "unfit_rows": 1,
                    "example_rows": [293],
                    "example_values": ["7.9166665"],
                    "suggested_target_type": "NUMBER(10,7)",
                    "suggested_fix": "Open Map → widen DEP_TIME to NUMBER(10,7)",
                }],
            },
            "destination_table_exists": True,
        },
        dest_kind="snowflake",
        use_llm=False,
    )
    assert explained["column_fixes"][0]["suggested_target_type"] == "NUMBER(10,7)"
    widen = next(a for a in explained["suggested_actions"] if a["kind"] == "change_target_type")
    assert widen["to_type"] == "NUMBER(10,7)"
    assert widen["column"] == "DEP_TIME"


def test_create_new_fit_widen_does_not_add_review_mappings():
    actions = _suggested_actions(
        [{
            "id": "g3f_population_fit",
            "message": "cannot fit the peeked CREATE type",
            "details": {"create_new_table": True},
        }],
        [
            {
                "column": "DEP_TIME",
                "suggested_target_type": "NUMBER(15,11)",
                "severity": "block",
                "apply_proven": True,
            },
            {
                "column": "ARR_TIME",
                "suggested_target_type": "NUMBER(15,11)",
                "severity": "block",
                "apply_proven": True,
            },
        ],
    )
    kinds = [a["kind"] for a in actions]
    assert kinds.count("change_target_type") == 2
    assert "review_mappings" not in kinds
