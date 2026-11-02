"""Package G15 must classify dest-exists from live dest names, not mapping targets.

Informatica Snowflake jumble / dbt-databricks#1289: write-by-name is only as
good as the live dest name list. Mapping ``target_columns`` omit unmapped dest
columns and invent dest-only mapping targets.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[1] / "src"
_API_ROOT = Path(__file__).resolve().parents[3] / "apps" / "api"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.gates import gate_g15_dest_exists_shape
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
    source_columns: list[str],
    target_columns: list[str],
    column_types: dict[str, str] | None = None,
    mappings: list[tuple[str, str]] | None = None,
) -> PreflightContext:
    maps = mappings or [(source_columns[0], target_columns[0])]
    return PreflightContext(
        plan=TransferPlan(
            source=SourceConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                parseable=True,
                columns=[ColumnSchema(name=n, inferred_type="INTEGER") for n in source_columns],
            ),
            destination=DestinationConfig(
                kind="database",
                db_type="postgresql",
                connected=True,
                can_write=True,
                table_exists=True,
                target_columns=[
                    ColumnSchema(name=n, inferred_type="INTEGER") for n in target_columns
                ],
                column_nullability={n: True for n in (column_types or target_columns)},
                column_types=column_types or {},
            ),
            mappings=[
                ColumnMapping(source=src, target=tgt, confidence=0.99) for src, tgt in maps
            ],
            dry_run_passed=True,
        )
    )


def _dest_only_names(result) -> list[str]:
    return [
        str(row.get("target") or "")
        for row in (result.details.get("dest_only") or [])
        if str(row.get("target") or "").strip()
    ]


def test_g15_uses_column_types_when_wider_than_mapping_targets() -> None:
    """Live dest {id, tenant_id} + extra source → overlap, not source_superset."""
    result = gate_g15_dest_exists_shape(
        _ctx(
            source_columns=["id", "loyalty_tier"],
            target_columns=["id"],
            column_types={"id": "INTEGER", "tenant_id": "TEXT"},
            mappings=[("id", "id")],
        )
    )
    assert result.gate_id == GateId.G15_DEST_EXISTS_SHAPE
    assert result.status != GateStatus.BLOCK
    assert result.details.get("shape") == "overlap"
    assert result.details.get("write_by") == "name"
    assert "tenant_id" in _dest_only_names(result)
    extras = result.details.get("extra_source_columns") or result.details.get(
        "unaccounted_sources"
    )
    assert extras and "loyalty_tier" in extras


def test_g15_names_dest_superset_when_types_reveal_unmapped_dest() -> None:
    """Mapping-only dest list would invent equal; live types name dest_superset."""
    result = gate_g15_dest_exists_shape(
        _ctx(
            source_columns=["id"],
            target_columns=["id"],
            column_types={"id": "INTEGER", "tenant_id": "TEXT"},
            mappings=[("id", "id")],
        )
    )
    assert result.status != GateStatus.BLOCK
    assert result.details.get("shape") == "dest_superset"
    assert result.details.get("write_by") == "name"
    assert "tenant_id" in _dest_only_names(result)
    counts = result.details.get("counts") or {}
    assert counts.get("dest_only_preserve") == 1


def test_g15_falls_back_to_target_columns_when_types_empty() -> None:
    """No live types — mapping targets remain the dest list (create-new host)."""
    result = gate_g15_dest_exists_shape(
        _ctx(
            source_columns=["id", "loyalty_tier"],
            target_columns=["id", "updated_at"],
            column_types={},
            mappings=[("id", "id")],
        )
    )
    assert result.status != GateStatus.BLOCK
    assert result.details.get("shape") == "overlap"
    assert "updated_at" in _dest_only_names(result)
    assert "tenant_id" not in _dest_only_names(result)
