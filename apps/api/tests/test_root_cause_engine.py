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


def test_fidelity_summary_names_the_type_path_not_just_the_column():
    """A bare column name makes the operator hunt Map for what is wrong with it.

    The pair is the finding, and it is what lets them judge the verdict — the
    reported Snowflake→MySQL block named neither column nor types.
    """
    root = next(
        r for r in build_root_causes(_fidelity_preflight()) if r.kind == "fidelity_collapse"
    )
    assert "country_auto_detected TEXT → INTEGER" in root.summary


def test_fidelity_summary_reads_type_path_from_the_coercion_probe():
    pf = {
        "passed": False,
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "block",
                "message": "1 type coercion issue(s)",
                "details": {"fidelity_collapse": True, "columns": ["order_ts"]},
            }
        ],
        "coercion_report": {
            "columns": [
                {
                    "source": "order_ts",
                    "source_type": "TIMESTAMP_NTZ",
                    "target_type": "DATETIME",
                    "severity": "block",
                }
            ]
        },
    }
    root = next(r for r in build_root_causes(pf) if r.kind == "fidelity_collapse")
    assert "order_ts TIMESTAMP_NTZ → DATETIME" in root.summary


def _encoding_preflight() -> dict:
    """G9 encoding finding on a TEXT → TEXT column — nothing about the type path."""
    return {
        "passed": False,
        "row_count": 4,
        "gates": [
            {
                "id": "g9_data_integrity",
                "status": "block",
                "message": (
                    "Data integrity failed: txt: format-control character "
                    "detected (U+200B) — normalize before transfer"
                ),
                "details": {
                    "encoding_issues": [
                        {
                            "column": "txt",
                            "row": 2,
                            "chars": ["U+200B"],
                            "message": "format-control character detected (U+200B)",
                            "suggested_transform": "strip_controls",
                        }
                    ],
                    "evidence_scope": {"sample_rows": 4},
                },
            }
        ],
        "blockers": [],
    }


def test_control_characters_are_not_a_fidelity_collapse_root():
    """TEXT → TEXT cannot collapse fidelity — remapping the type fixes nothing.

    The G9 message says "integrity failed", which the fidelity matcher read as a
    lossy type path and answered with the wrong corrective action.
    """
    roots = build_root_causes(_encoding_preflight())
    kinds = [r.kind for r in roots]
    assert "fidelity_collapse" not in kinds, kinds
    assert kinds == ["encoding_normalization"], kinds


def test_encoding_root_names_the_column_character_and_transform():
    root = build_root_causes(_encoding_preflight())[0]
    assert "txt" in root.summary
    assert "U+200B" in root.summary
    assert "strip_controls" in root.recommended_fix
    assert root.affected_columns == ["txt"]


def test_encoding_gate_stamped_fidelity_collapse_stays_fidelity():
    """An explicit fidelity stamp still wins — charset narrowing is a real cast."""
    pf = _encoding_preflight()
    pf["gates"][0]["details"]["fidelity_collapse"] = True
    kinds = [r.kind for r in build_root_causes(pf)]
    assert "fidelity_collapse" in kinds, kinds
    assert "encoding_normalization" not in kinds, kinds


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


def test_single_fidelity_gate_collapses_to_one_root():
    pf = {
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "block",
                "message": "TEXT → INTEGER lossy",
                "details": {
                    "fidelity_collapse": True,
                    "issues_detail": [
                        {
                            "source": "amt",
                            "source_type": "TEXT",
                            "target_type": "INTEGER",
                            "fidelity_collapse": True,
                        }
                    ],
                },
            }
        ],
        "blockers": [
            {
                "id": "g3_schema_contract",
                "message": "TEXT → INTEGER lossy",
                "details": {"fidelity_collapse": True},
            }
        ],
    }
    roots = build_root_causes(pf)
    assert len([r for r in roots if r.kind == "fidelity_collapse"]) == 1


def test_apply_rewrites_proof_transfer_decision_blockers():
    base = _fidelity_preflight()
    base["proof_bundle"] = {
        "transfer_decision": {
            "decision": "block",
            "blockers": [
                "g3 coercion",
                "g4 risk ack",
                "g9 integrity",
            ],
        }
    }
    pf = apply_root_causes_to_preflight(base)
    td = pf["proof_bundle"]["transfer_decision"]
    assert len(td["blockers"]) == 1
    assert "fidelity" in td["blockers"][0].lower() or "lossy" in td["blockers"][0].lower()
    assert td["root_causes"]


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


def test_g8_transform_errors_are_not_fidelity_collapse_root():
    """Empty url coerce on image→image must not look like type-path fidelity collapse."""
    from services.root_cause_engine import build_root_causes

    pf = {
        "gates": [
            {
                "id": "g8_reconciliation",
                "status": "block",
                "message": (
                    "Dry-run reconciliation failed — transform errors: "
                    "row 285 image→image: Empty value cannot coerce to url (+1 more)"
                ),
                "details": {
                    "kind": "transform_errors",
                    "errors": [
                        "row 285 image→image: Empty value cannot coerce to url",
                    ],
                },
            }
        ],
        "blockers": [
            {
                "id": "g8_reconciliation",
                "message": (
                    "Dry-run reconciliation failed — transform errors: "
                    "row 285 image→image: Empty value cannot coerce to url"
                ),
                "details": {"kind": "transform_errors"},
            }
        ],
        "coercion_report": {"sampled_rows": 306},
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "fidelity_collapse" for r in roots)
    assert any(r.kind == "sample_transform" for r in roots)


def test_g5_transform_errors_are_not_fidelity_collapse_root():
    """G5 empty-url dry-run must be sample_transform — not Lossy / fidelity collapse."""
    from services.root_cause_engine import build_root_causes

    pf = {
        "gates": [
            {
                "id": "g5_dry_run",
                "status": "block",
                "message": "Dry-run failed: image→image: Empty value cannot coerce to url",
                "details": {
                    "kind": "transform_errors",
                    "errors": ["image→image: Empty value cannot coerce to url"],
                },
            },
            {
                "id": "g9_data_integrity",
                "status": "block",
                "message": (
                    "Data integrity failed: image→image: Empty value cannot coerce to url"
                ),
                "details": {
                    "kind": "transform_errors",
                    "issues": ["image→image: Empty value cannot coerce to url"],
                    "note": (
                        "Preflight blocked the transfer (0 rows written). "
                        "Findings below are for inspection."
                    ),
                },
            },
        ],
        "blockers": [
            {
                "id": "g5_dry_run",
                "message": "Dry-run failed: image→image: Empty value cannot coerce to url",
                "details": {"kind": "transform_errors"},
            },
            {
                "id": "g9_data_integrity",
                "message": "Data integrity failed: image→image: Empty value cannot coerce to url",
                "details": {
                    "kind": "transform_errors",
                    "issues": [
                        "Preflight blocked the transfer (0 rows written). Findings.",
                        "image→image: Empty value cannot coerce to url",
                    ],
                },
            },
        ],
        "coercion_report": {
            "sampled_rows": 306,
            "columns": [
                {
                    "source": "country_auto_detected",
                    "fidelity_collapse": True,
                    "severity": "warn",
                },
                {
                    "source": "image",
                    "severity": "block",
                },
            ],
        },
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "fidelity_collapse" for r in roots), roots
    xf = [r for r in roots if r.kind == "sample_transform"]
    assert len(xf) == 1, roots
    assert "image" in xf[0].affected_columns
    assert "Preflight blocked the transfer" not in xf[0].affected_columns
    # Signed/warn fidelity columns must not re-inflate a fidelity root.
    assert "country_auto_detected" not in xf[0].affected_columns


def test_g9_duplicate_integrity_is_not_fidelity_collapse_root():
    """Duplicate-key integrity failures must stay duplicate_identity — not Accept cast."""
    from services.root_cause_engine import build_root_causes

    pf = {
        "gates": [
            {
                "id": "g9_data_integrity",
                "status": "block",
                "message": "Data integrity failed: id: duplicate key values in sample",
                "details": {"duplicate_keys": 3, "identity_duplicates": True},
            }
        ],
        "blockers": [
            {
                "id": "g9_data_integrity",
                "message": "Data integrity failed: id: duplicate key values in sample",
                "details": {"duplicate_keys": 3},
            }
        ],
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "fidelity_collapse" for r in roots), roots
    assert any(r.kind == "duplicate_identity" for r in roots)


def test_risk_unacknowledged_is_not_zero_column_fidelity_collapse():
    """Map→Validate: missing contracts list columns — never '0 columns collapse'."""
    from services.root_cause_engine import build_root_causes

    pf = {
        "gates": [
            {
                "id": "g4_mapping_confidence",
                "status": "block",
                "message": (
                    "4 mapping(s) require a signed Migration Risk Contract "
                    "with a continue execution policy (lossy/narrowing/mutate)"
                ),
                "details": {
                    "risk_unacknowledged": [
                        "id→id",
                        "stripe_customer_id→stripe_customer_id",
                        "google_id→google_id",
                        "provider_id→provider_id",
                    ]
                },
            }
        ],
        "blockers": [
            {
                "id": "g4_mapping_confidence",
                "message": (
                    "4 mapping(s) require a signed Migration Risk Contract "
                    "with a continue execution policy (lossy/narrowing/mutate)"
                ),
                "details": {
                    "risk_unacknowledged": [
                        "id→id",
                        "stripe_customer_id→stripe_customer_id",
                        "google_id→google_id",
                        "provider_id→provider_id",
                    ]
                },
            }
        ],
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "fidelity_collapse" for r in roots), roots
    risk = [r for r in roots if r.kind == "risk_contract_incomplete"]
    assert len(risk) == 1, roots
    assert "id" in risk[0].affected_columns
    assert "stripe_customer_id" in risk[0].affected_columns
    assert "0 column" not in risk[0].summary.lower()


def test_fidelity_absorbs_risk_contract_incomplete_same_path():
    """Charter: one root when G3 fidelity + proof/G4 missing-contract coexist."""
    pf = {
        "gates": [
            {
                "id": "g3_schema_contract",
                "status": "block",
                "message": "Lossy type coercion: amt (FLOAT) → amt (INTEGER)",
                "details": {"fidelity_collapse": True, "columns": ["amt"]},
            },
            {
                "id": "g4_mapping_confidence",
                "status": "block",
                "message": (
                    "1 mapping(s) require a signed Migration Risk Contract "
                    "with a continue execution policy (lossy/narrowing/mutate)"
                ),
                "details": {"risk_unacknowledged": ["amt→amt"]},
            },
        ],
        "blockers": [
            {
                "id": "proof_bundle",
                "message": "Migration Risk Contract required (execution policy) for: amt",
                "details": {"columns": ["amt"]},
            }
        ],
    }
    roots = build_root_causes(pf)
    assert not any(r.kind == "risk_contract_incomplete" for r in roots), roots
    fidelity = [r for r in roots if r.kind == "fidelity_collapse"]
    assert len(fidelity) == 1, roots
    absorbed = set(fidelity[0].absorbed_blocker_ids)
    assert "g3_schema_contract" in absorbed
    assert "g4_mapping_confidence" in absorbed
    assert "proof_bundle" in absorbed
    assert "amt" in fidelity[0].affected_columns


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
