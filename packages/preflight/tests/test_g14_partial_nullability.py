"""Package G14 must pass live dest names — mapping targets are not enough."""

from __future__ import annotations

import sys
from pathlib import Path

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[1] / "src"
_API_ROOT = Path(__file__).resolve().parents[3] / "apps" / "api"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.gates import gate_g14_destination_requirements
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


def _ctx(
    *,
    column_types: dict[str, str] | None = None,
    column_nullability: dict[str, bool] | None = None,
    target_columns: list[ColumnSchema] | None = None,
) -> PreflightContext:
    return PreflightContext(
        plan=TransferPlan(
            source=SourceConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                parseable=True,
                columns=[ColumnSchema(name="id", inferred_type="INTEGER")],
            ),
            destination=DestinationConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                can_write=True,
                table_exists=True,
                target_columns=target_columns
                or [ColumnSchema(name="id", inferred_type="INTEGER")],
                column_nullability=column_nullability or {"id": False},
                column_types=column_types or {},
            ),
            mappings=[ColumnMapping(source="id", target="id", confidence=0.99)],
            dry_run_passed=True,
        )
    )


def test_g14_partial_nullability_skips_when_column_types_wider() -> None:
    result = gate_g14_destination_requirements(
        _ctx(
            column_types={"id": "INTEGER", "tenant_id": "TEXT"},
            column_nullability={"id": False},
        )
    )
    assert result.gate_id == GateId.G14_DESTINATION_REQUIREMENTS
    assert result.status == GateStatus.SKIP
    assert result.details.get("unmeasured") is True
    assert result.details.get("reason") == "nullability_metadata_partial"
    assert "tenant_id" in (result.details.get("unmeasured_columns") or [])


def test_g14_does_not_false_pass_when_types_cover_unmapped_not_null() -> None:
    result = gate_g14_destination_requirements(
        _ctx(
            column_types={"id": "INTEGER", "tenant_id": "TEXT"},
            column_nullability={"id": False, "tenant_id": False},
        )
    )
    assert result.status == GateStatus.BLOCK
    assert "tenant_id" in (result.details.get("unfilled_required_columns") or [])
