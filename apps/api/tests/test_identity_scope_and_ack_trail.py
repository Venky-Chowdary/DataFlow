"""Continuation: identity probe scope + acknowledgment trail."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.data_integrity import run_integrity_audit  # noqa: E402
from services.preflight_proof_bundle import build_preflight_proof_bundle  # noqa: E402
from preflight.gates import evidence_scope, gate_g9_data_integrity  # noqa: E402
from preflight.models import ColumnMapping, DestinationConfig, PreflightContext, SourceConfig, TransferPlan  # noqa: E402


def test_integrity_audit_marks_full_selected_when_probe_ran() -> None:
    report = run_integrity_audit(
        source_columns=["id", "name"],
        mappings=[{"source": "id", "target": "id", "confidence": 0.95}],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER"}],
        sample_rows=[{"id": "1", "name": "a"}, {"id": "2", "name": "b"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode="incremental_deduped",
        contract_primary_key="id",
        source_duplicate_findings=[],
        source_duplicate_probe_ran=True,
        source_duplicate_probe_pk="id",
    )
    probe = report["source_uniqueness_probe"]
    assert probe["ran"] is True
    assert probe["coverage"] == "full_selected"
    assert probe["primary_key"] == "id"


def test_g9_evidence_scope_full_selected_when_probe_ran() -> None:
    class Ctx(PreflightContext):
        def __init__(self) -> None:
            plan = TransferPlan(
                source=SourceConfig(kind="database", connected=True, columns=[]),
                destination=DestinationConfig(kind="database", db_type="postgresql", connected=True),
                mappings=[ColumnMapping(source="id", target="id", confidence=0.95)],
                sync_mode="incremental_deduped",
                contract_primary_key="id",
            )
            super().__init__(plan=plan)
            self.sample_rows = [{"id": "1"}]
            self.source_duplicate_probe_ran = True
            self.source_duplicate_probe_pk = "id"

        def run_integrity_audit(self, sample_size: int = 1000) -> dict:
            return {
                "blocks_transfer": False,
                "checks_passed": 1,
                "checks_failed": 0,
                "issues": [],
                "warnings": [],
                "summary": "ok",
                "source_uniqueness_probe": {
                    "ran": True,
                    "primary_key": "id",
                    "finding_count": 0,
                    "coverage": "full_selected",
                },
            }

    result = gate_g9_data_integrity(Ctx())
    scope = result.details.get("evidence_scope") or {}
    assert scope.get("coverage") == "full_selected"
    assert "probe" in (scope.get("note") or "").lower() or "identity" in (scope.get("note") or "").lower()


def test_compliance_acknowledgment_trail() -> None:
    bundle = build_preflight_proof_bundle(
        columns=["id", "email"],
        sample_rows=[{"id": "1", "email": "a@b.com"}],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.96},
            {"source": "email", "target": "email", "confidence": 0.95},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
            {"name": "email", "inferred_type": "VARCHAR", "samples": ["a@b.com"]},
        ],
        source_records=[{"id": "1", "email": "a@b.com"}],
        target_records=[{"id": "1", "email": "a@b.com"}],
        compliance_acknowledged=True,
        acknowledgment_actor="alice@acme.com",
        acknowledgment_reason="Approved for pilot migration",
    )
    ack = bundle["compliance"].get("acknowledgment") or {}
    assert ack.get("actor") == "alice@acme.com"
    assert "pilot" in (ack.get("reason") or "").lower()
    assert ack.get("at")


def test_evidence_scope_helper() -> None:
    scope = evidence_scope(kind="x", sample_rows=25, coverage="full_selected", note="probe")
    assert scope["coverage"] == "full_selected"
    assert scope["sample_rows"] == 25
