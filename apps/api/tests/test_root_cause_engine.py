"""Root Cause Engine — one TEXT→INTEGER root, not N duplicate gate blockers."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.root_cause_engine import (  # noqa: E402
    apply_root_causes_to_preflight,
    build_root_causes,
)


def _fidelity_preflight() -> dict:
    return {
        "passed": False,
        "row_count": 100_000,
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "block",
                "message": "5 type coercion issue(s)",
                "details": {
                    "fidelity_collapse": True,
                    "issues_detail": [
                        {
                            "source": "country_auto_detected",
                            "source_type": "TEXT",
                            "target_type": "INTEGER",
                            "severity": "block",
                            "fidelity_collapse": True,
                        },
                        {
                            "source": "referral_credit_processed",
                            "source_type": "TEXT",
                            "target_type": "INTEGER",
                            "severity": "block",
                            "fidelity_collapse": True,
                        },
                    ],
                    "evidence_scope": {"sample_rows": 25},
                },
            },
            {
                "id": "g4_mapping_confidence",
                "status": "block",
                "message": "5 mapping(s) require explicit risk acknowledgment (lossy/narrowing/mutate)",
                "details": {
                    "issues": [
                        "country_auto_detected → INTEGER lossy",
                        "referral_credit_processed → INTEGER lossy",
                    ]
                },
            },
            {
                "id": "g9_data_integrity",
                "status": "block",
                "message": (
                    "Data integrity failed: country_auto_detected "
                    "(TEXT) → country_auto_detected (INTEGER) (+12 more)"
                ),
                "details": {
                    "issues": [
                        "country_auto_detected (TEXT) → country_auto_detected (INTEGER)",
                        "referral_credit_processed (TEXT) → referral_credit_processed (INTEGER)",
                    ],
                    "evidence_scope": {"sample_rows": 25, "coverage": "sample"},
                },
            },
            {"id": "g1_source", "status": "pass", "message": "ok", "details": {}},
        ],
        "blockers": [
            {
                "id": "g3_schema_contract",
                "message": "5 type coercion issue(s)",
                "details": {"fidelity_collapse": True},
            },
            {
                "id": "g4_mapping_confidence",
                "message": "5 mapping(s) require explicit risk acknowledgment (lossy/narrowing/mutate)",
                "details": {},
            },
            {
                "id": "g9_data_integrity",
                "message": "Data integrity failed: country_auto_detected (TEXT) → INTEGER",
                "details": {},
            },
        ],
        "coercion_report": {
            "sampled_rows": 25,
            "columns": [
                {
                    "source": "country_auto_detected",
                    "severity": "block",
                    "fidelity_collapse": True,
                },
                {
                    "source": "referral_credit_processed",
                    "severity": "block",
                    "fidelity_collapse": True,
                },
            ],
        },
    }


def test_one_fidelity_root_not_three_blockers():
    pf = _fidelity_preflight()
    roots = build_root_causes(pf)
    fidelity = [r for r in roots if r.kind == "fidelity_collapse"]
    assert len(fidelity) == 1, roots
    root = fidelity[0]
    assert "g3_schema_contract" in root.impacted_gates
    assert "g4_mapping_confidence" in root.impacted_gates
    assert "g9_data_integrity" in root.impacted_gates
    assert "country_auto_detected" in root.affected_columns
    assert root.affected_rows_sample == 25
    assert root.estimated_total_rows == 100_000
    assert root.recommended_fix
    assert root.business_impact
    assert root.quarantine_policy
    assert root.rollback_policy
    assert root.documentation


def test_apply_collapses_operator_blockers():
    pf = apply_root_causes_to_preflight(_fidelity_preflight())
    assert len(pf["root_causes"]) == 1
    blockers = pf["blockers"]
    # One root blocker — not g3+g4+g9 duplicates
    gate_dupes = [
        b
        for b in blockers
        if b.get("id") in {"g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"}
    ]
    assert gate_dupes == [], blockers
    roots = [b for b in blockers if (b.get("details") or {}).get("root_cause")]
    assert len(roots) == 1
    assert roots[0]["details"]["kind"] == "fidelity_collapse"
    # Gates remain for audit
    assert len([g for g in pf["gates"] if g["status"] == "block"]) == 3


def test_single_unrelated_blocker_does_not_invent_fidelity_root():
    pf = {
        "gates": [
            {
                "id": "g2_destination",
                "status": "block",
                "message": "Destination not writable",
                "details": {},
            }
        ],
        "blockers": [
            {"id": "g2_destination", "message": "Destination not writable", "details": {}},
        ],
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "fidelity_collapse" for r in roots)


def test_run_file_preflight_emits_root_causes_for_text_to_int():
    from services.preflight_service import run_file_preflight

    col = "country_auto_detected"
    src_type = "TEXT COLLATE UTF8MB4_0900_AI_CI"
    result = run_file_preflight(
        columns=[col],
        column_types={col: src_type},
        row_count=1000,
        mappings=[
            {
                "source": col,
                "target": col,
                "confidence": 0.92,
                "source_type": src_type,
                "target_type": "INTEGER",
                "create_new": True,
                "fidelity": "lossy_cast",
                "type_narrowing": True,
            }
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mysql",
        sync_mode="full_refresh_append",
        sample_rows=[{col: "0"}, {col: "1"}, {col: "x"}],
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    assert result.get("passed") is False
    roots = result.get("root_causes") or []
    fidelity = [r for r in roots if r.get("kind") == "fidelity_collapse"]
    assert fidelity, result.get("blockers")
    assert len(fidelity) == 1
    absorbed = set(fidelity[0].get("absorbed_blocker_ids") or [])
    # Operator blockers must not re-list absorbed gate ids
    for b in result.get("blockers") or []:
        assert b.get("id") not in absorbed or (b.get("details") or {}).get("root_cause")
