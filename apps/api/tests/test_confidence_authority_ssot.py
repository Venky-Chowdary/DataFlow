"""Module 3: G4 owns mapping confidence — proof/G9 must not re-block.

Challenge: low confidence appeared as G4 gate + proof_bundle blocker
(\"Semantic mapping confidence too low\") + G9 mapping_confidence check —
three operator faces for one threshold.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_PREFLIGHT_SRC = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
if str(_PREFLIGHT_SRC) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_SRC))


def test_proof_bundle_reports_confidence_but_does_not_reblock():
    from services.preflight_proof_bundle import build_preflight_proof_bundle

    bundle = build_preflight_proof_bundle(
        columns=["id", "email"],
        sample_rows=[{"id": "1", "email": "alice@example.com"}],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.45},
            {"source": "email", "target": "email", "confidence": 0.50},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["alice@example.com"]},
        ],
        source_records=[{"id": "1", "email": "alice@example.com"}],
        target_records=[{"id": "1", "email": "alice@example.com"}],
        primary_key="id",
        confidence_threshold=0.85,
    )

    # Metrics still honest
    assert bundle["min_confidence"] < 0.85
    assert bundle.get("confidence_authority") == "g4_mapping_confidence"
    # Must NOT invent a sibling blocker — G4 is the hard gate
    blockers = bundle["transfer_decision"]["blockers"]
    assert "Semantic mapping confidence too low" not in blockers, blockers
    assert not any("confidence too low" in str(b).lower() for b in blockers)


def test_g9_mapping_confidence_does_not_blocks_transfer():
    from services.data_integrity import run_integrity_audit

    report = run_integrity_audit(
        source_columns=["id", "email"],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.40},
            {"source": "email", "target": "email", "confidence": 0.50},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "email", "inferred_type": "VARCHAR"},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "email", "inferred_type": "VARCHAR"},
        ],
        sample_rows=[{"id": "1", "email": "a@b.c"}],
        destination_db_type="postgresql",
        validation_mode="strict",
    )
    mc = next(c for c in report["checks"] if c["check"] == "mapping_confidence")
    assert mc.get("blocks_transfer") is False, mc
    assert mc.get("authority") == "g4_mapping_confidence"
    # Still surfaced as warnings for explainability
    assert (mc.get("warnings") or mc.get("issues") or []), mc


def test_g4_still_blocks_low_confidence():
    from preflight.gates import gate_g4_mapping_confidence
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="email", inferred_type="VARCHAR"),
            ],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=True,
            can_write=True,
            target_columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="email", inferred_type="VARCHAR"),
            ],
        ),
        mappings=[
            ColumnMapping(source="id", target="id", confidence=0.99),
            ColumnMapping(source="email", target="email", confidence=0.50),
        ],
        confidence_threshold=0.85,
        validation_mode="strict",
    )
    result = gate_g4_mapping_confidence(PreflightContext(plan=plan, sample_rows=[]))
    assert result.status.value == "block"
    assert "below floor" in result.message.lower() or "mapping" in result.message.lower()


def test_root_cause_emits_mapping_confidence_kind():
    from services.root_cause_engine import build_root_causes

    roots = build_root_causes(
        {
            "gates": [
                {
                    "id": "g4_mapping_confidence",
                    "status": "block",
                    "message": "2 mapping(s) below floor 0.85",
                    "details": {
                        "low_confidence": [
                            "email→email (0.50)",
                            "team_invitations→team_invitations (0.80)",
                        ]
                    },
                }
            ],
            "blockers": [
                {
                    "id": "g4_mapping_confidence",
                    "message": "2 mapping(s) below floor 0.85",
                    "details": {
                        "low_confidence": [
                            "email→email (0.50)",
                            "team_invitations→team_invitations (0.80)",
                        ]
                    },
                }
            ],
            "row_count": 1000,
        }
    )
    conf = [r for r in roots if r.kind == "mapping_confidence"]
    assert len(conf) == 1, roots
    assert "g4_mapping_confidence" in conf[0].impacted_gates
    assert conf[0].documentation
