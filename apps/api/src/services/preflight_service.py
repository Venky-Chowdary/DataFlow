"""Preflight validation for DataTransfer transfers."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Add preflight package to path
_PREFLIGHT_ROOT = Path(__file__).resolve().parents[4] / "packages" / "preflight" / "src"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))

from preflight import PreflightEngine
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)


class FilePreflightContext(PreflightContext):
    """Preflight context for file → database transfers."""

    def __init__(self, plan: TransferPlan, sample_rows: list[dict] | None = None):
        super().__init__(plan=plan)
        self.sample_rows = sample_rows or []

    def run_dry_run(self, sample_size: int = 1000) -> tuple[bool, list[str]]:
        if not self.sample_rows:
            return True, []
        errors: list[str] = []
        for i, row in enumerate(self.sample_rows[:sample_size]):
            for m in self.plan.mappings:
                if m.source not in row and m.source in {c.name for c in self.plan.source.columns}:
                    errors.append(f"Row {i}: missing source column '{m.source}'")
                    if len(errors) >= 10:
                        return False, errors
        return len(errors) == 0, errors

    def probe_unique_constraint(self, columns: list[str]) -> list[dict[str, Any]]:
        if not columns or not self.sample_rows:
            return []
        col = columns[0]
        source_col = col
        for m in self.plan.mappings:
            if m.target == col:
                source_col = m.source
                break
        seen: dict[str, int] = {}
        dupes: list[dict[str, Any]] = []
        for row in self.sample_rows:
            val = str(row.get(source_col, ""))
            seen[val] = seen.get(val, 0) + 1
        for val, count in seen.items():
            if count > 1 and val:
                dupes.append({"column": col, "value": val, "count": count})
        return dupes[:5]


def run_file_preflight(
    *,
    columns: list[str],
    column_types: dict[str, str],
    row_count: int,
    mappings: list[dict[str, Any]],
    destination_connected: bool = True,
    sample_rows: list[dict] | None = None,
    estimated_bytes: int = 0,
) -> dict[str, Any]:
    """Run 8 preflight gates for a file-based transfer."""
    source_cols = [
        ColumnSchema(name=c, inferred_type=column_types.get(c, "VARCHAR").upper())
        for c in columns
    ]
    dest_cols = [
        ColumnSchema(name=m["target"], inferred_type=column_types.get(m["source"], "VARCHAR").upper())
        for m in mappings
    ]
    plan_mappings = [
        ColumnMapping(
            source=m["source"],
            target=m["target"],
            confidence=float(m.get("confidence", 0.9)),
            reasoning=m.get("reason", ""),
        )
        for m in mappings
    ]

    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=source_cols,
            row_count_estimate=row_count,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=destination_connected,
            can_create_table=True,
            can_write=destination_connected,
            target_columns=dest_cols,
            table_exists=False,
        ),
        mappings=plan_mappings,
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=estimated_bytes,
        available_staging_bytes=max(estimated_bytes * 2, 1_000_000_000),
        confidence_threshold=0.70,
    )

    ctx = FilePreflightContext(plan, sample_rows)
    engine = PreflightEngine(fail_fast=False)
    result = engine.run(ctx)

    return {
        "passed": result.passed,
        "passed_count": result.passed_count,
        "total_gates": result.total_gates,
        "readiness_score": round(result.passed_count / result.total_gates * 100, 1),
        "gates": [
            {
                "id": g.gate_id.value,
                "status": g.status.value,
                "message": g.message,
                "duration_ms": round(g.duration_ms, 2),
                "details": g.details,
            }
            for g in result.gates
        ],
        "blockers": [
            {"id": b.gate_id.value, "message": b.message, "details": b.details}
            for b in result.blockers
        ],
    }
