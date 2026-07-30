"""Strict sentinel NULL loss + mapping proof clear samples + gate evidence scope."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.coercion_probe import analyze_coercion  # noqa: E402
from services.mapping_proof import build_mapping_proof  # noqa: E402
from preflight.gates import gate_g3_schema_contract, gate_g6_target_ddl, gate_g7_capacity  # noqa: E402
from preflight.models import (  # noqa: E402
    ColumnMapping,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)


def test_strict_sentinel_nulls_block() -> None:
    report = analyze_coercion(
        sample_rows=[{"amt": "N/A"}, {"amt": "12"}, {"amt": "null"}],
        mappings=[{"source": "amt", "target": "amt"}],
        source_types={"amt": "VARCHAR"},
        dest_types={"amt": "INTEGER"},
        dest_db_type="postgresql",
        validation_mode="strict",
    )
    col = next(c for c in report["columns"] if c["source"] == "amt")
    assert col["sentinel_nulls"] >= 1
    assert col["severity"] == "block"
    assert report["has_blocking_failures"] is True


def test_balanced_sentinel_nulls_warn() -> None:
    report = analyze_coercion(
        sample_rows=[{"amt": "N/A"}, {"amt": "12"}],
        mappings=[{"source": "amt", "target": "amt"}],
        source_types={"amt": "VARCHAR"},
        dest_types={"amt": "INTEGER"},
        dest_db_type="postgresql",
        validation_mode="balanced",
    )
    col = next(c for c in report["columns"] if c["source"] == "amt")
    assert col["sentinel_nulls"] >= 1
    assert col["severity"] in {"warn", "info"}
    assert report["has_blocking_failures"] is False


def test_mapping_proof_returns_clear_and_masked_preview() -> None:
    proof = build_mapping_proof(
        mappings=[
            {
                "source": "email",
                "target": "email",
                "source_type": "VARCHAR",
                "target_type": "VARCHAR",
                "confidence": 0.95,
                "is_pii": True,
                "samples": ["alice@example.com"],
            }
        ],
        destination_db_type="postgresql",
        sync_mode="append",
    )
    row = proof["mappings"][0]
    assert row["sample_preview"]
    assert row["sample_preview_clear"]
    assert "alice@" not in row["sample_preview"][0]
    assert "alice@example.com" in row["sample_preview_clear"]
    assert row["evidence"]["sample_preview_masked"] is True
    assert any(r["code"] == "append_may_duplicate" for r in proof["global_risks"])


def test_g6_g7_g3_attach_evidence_scope() -> None:
    plan = TransferPlan(
        source=SourceConfig(kind="database", connected=True, columns=[]),
        destination=DestinationConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            can_write=True,
            table_exists=True,
        ),
        mappings=[ColumnMapping(source="id", target="_id", confidence=0.95)],
        sync_mode="full_refresh",
        estimated_bytes=1024,
        available_staging_bytes=10_000_000,
    )
    ctx = PreflightContext(plan=plan)
    g3 = gate_g3_schema_contract(ctx)
    assert g3.details.get("evidence_scope", {}).get("kind") == "schema_contract"
    g6 = gate_g6_target_ddl(ctx)
    assert g6.details.get("evidence_scope", {}).get("kind") == "target_ddl"
    g7 = gate_g7_capacity(ctx)
    assert g7.details.get("evidence_scope", {}).get("kind") == "capacity"
    assert g7.status.value == "pass"


def test_g8_blocks_without_sample_attaches_evidence_scope() -> None:
    from preflight.gates import gate_g8_reconciliation

    plan = TransferPlan(
        source=SourceConfig(kind="database", connected=True, columns=[]),
        destination=DestinationConfig(kind="database", db_type="postgresql", connected=True),
        mappings=[ColumnMapping(source="id", target="id", confidence=0.95)],
    )
    ctx = PreflightContext(plan=plan)
    result = gate_g8_reconciliation(ctx)
    assert result.status.value == "block"
    scope = (result.details or {}).get("evidence_scope") or {}
    assert scope.get("kind") == "reconciliation"
    assert scope.get("coverage") == "none"
