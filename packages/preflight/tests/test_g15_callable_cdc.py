"""Package preflight G15 + CDC/callable parity — hosted SSOT when importable."""

from __future__ import annotations

import sys
from pathlib import Path

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[1] / "src"
_API_ROOT = Path(__file__).resolve().parents[3] / "apps" / "api"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateId,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from preflight.gates import gate_g9_sync_contract, gate_g15_dest_exists_shape


def _ctx(*, source_read_mode: str = "table", sync_mode: str = "full_refresh_overwrite") -> PreflightContext:
    return PreflightContext(
        plan=TransferPlan(
            source=SourceConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                parseable=True,
                source_read_mode=source_read_mode,
                columns=[
                    ColumnSchema(name="id", inferred_type="INTEGER"),
                    ColumnSchema(name="loyalty_tier", inferred_type="VARCHAR"),
                ],
            ),
            destination=DestinationConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                can_write=True,
                table_exists=True,
                target_columns=[
                    ColumnSchema(name="id", inferred_type="INTEGER"),
                    ColumnSchema(name="updated_at", inferred_type="TIMESTAMP"),
                ],
                column_nullability={"id": False, "updated_at": True},
            ),
            mappings=[
                ColumnMapping(source="id", target="id", confidence=0.99),
            ],
            sync_mode=sync_mode,
            dry_run_passed=True,
        )
    )


def test_g15_names_source_superset_and_does_not_block() -> None:
    result = gate_g15_dest_exists_shape(_ctx())
    assert result.gate_id == GateId.G15_DEST_EXISTS_SHAPE
    assert result.status != GateStatus.BLOCK
    assert result.details.get("shape") in {"source_superset", "overlap"}
    assert result.details.get("write_by") == "name"
    extras = result.details.get("extra_source_columns") or result.details.get("unaccounted_sources")
    assert extras and "loyalty_tier" in extras
    assert result.details.get("primary_action") in {"review_map", "review_mappings"}


def test_g9_sync_blocks_cdc_on_procedure_source() -> None:
    result = gate_g9_sync_contract(_ctx(source_read_mode="procedure", sync_mode="cdc"))
    assert result.gate_id == GateId.G9_SYNC_CONTRACT
    assert result.status == GateStatus.BLOCK
    blob = f"{result.message} {result.details}".lower()
    assert "snapshot" in blob or "cdc" in blob or "procedure" in blob


def test_g9_sync_allows_full_refresh_on_procedure_source() -> None:
    result = gate_g9_sync_contract(
        _ctx(source_read_mode="procedure", sync_mode="full_refresh_overwrite")
    )
    assert result.status != GateStatus.BLOCK
