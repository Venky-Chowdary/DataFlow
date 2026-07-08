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
    GateStatus,
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
            return False, ["No sample rows available for dry-run validation"]

        headers = list(self.sample_rows[0].keys()) if self.sample_rows else []
        rows = [[str(row.get(h, "")) for h in headers] for row in self.sample_rows[:sample_size]]
        column_types = {c.name: c.inferred_type for c in self.plan.source.columns}
        mapping_dicts = [
            {"source": m.source, "target": m.target, "transform": getattr(m, "transform", "")}
            for m in self.plan.mappings
        ]

        try:
            from services.transform_engine import dry_run_sample

            return dry_run_sample(
                headers=headers,
                sample_rows=rows,
                mappings=mapping_dicts,
                column_types=column_types,
            )
        except Exception:
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


VALIDATION_CONFIDENCE_THRESHOLDS = {
    "balanced": 0.75,
    "strict": 0.85,
    "maximum": 0.95,
}


def confidence_threshold_for_mode(validation_mode: str | None) -> float:
    return VALIDATION_CONFIDENCE_THRESHOLDS.get((validation_mode or "strict").lower(), 0.85)


def run_transfer_policy_gates(
    *,
    sync_mode: str = "full_refresh_overwrite",
    schema_policy: str = "manual_review",
    validation_mode: str = "strict",
    stream_contracts: list[dict[str, Any]] | None = None,
    backfill_new_fields: bool = False,
) -> list[dict[str, Any]]:
    """Validate enterprise run policy that sits above source/destination probes."""
    contracts = [c for c in stream_contracts or [] if c.get("selected", True)]
    sync = (sync_mode or "full_refresh_overwrite").lower()
    schema = (schema_policy or "manual_review").lower()
    validation = (validation_mode or "strict").lower()
    requires_cursor = sync in {"incremental_append", "incremental_deduped", "cdc"}
    requires_primary_key = sync in {"incremental_deduped", "cdc", "full_refresh_overwrite_deduped"}

    missing_cursor = [
        c.get("name") or c.get("stream") or "stream"
        for c in contracts
        if requires_cursor and not (c.get("cursor_field") or c.get("cursor"))
    ]
    missing_primary_key = [
        c.get("name") or c.get("stream") or "stream"
        for c in contracts
        if requires_primary_key and not (c.get("primary_key") or c.get("primary_keys"))
    ]

    gates: list[dict[str, Any]] = []
    sync_issues: list[str] = []
    if missing_cursor:
        sync_issues.append(f"Missing cursor field for {', '.join(missing_cursor[:5])}")
    if missing_primary_key:
        sync_issues.append(f"Missing primary key for {', '.join(missing_primary_key[:5])}")

    if sync_issues:
        gates.append({
            "id": "g9_sync_contract",
            "status": GateStatus.BLOCK.value,
            "message": "Sync mode contract incomplete",
            "duration_ms": 0,
            "details": {"issues": sync_issues, "sync_mode": sync},
        })
    else:
        gates.append({
            "id": "g9_sync_contract",
            "status": GateStatus.PASS.value,
            "message": f"Sync contract valid for {sync.replace('_', ' ')}",
            "duration_ms": 0,
            "details": {
                "sync_mode": sync,
                "streams": len(contracts),
                "requires_cursor": requires_cursor,
                "requires_primary_key": requires_primary_key,
            },
        })

    schema_issues: list[str] = []
    if schema not in {"manual_review", "propagate_columns", "propagate_all", "pause_on_change"}:
        schema_issues.append(f"Unknown schema policy '{schema}'")
    if backfill_new_fields and schema not in {"propagate_columns", "propagate_all"}:
        schema_issues.append("Backfill new fields requires automatic column propagation")

    if schema_issues:
        gates.append({
            "id": "g10_schema_policy",
            "status": GateStatus.BLOCK.value,
            "message": "Schema change policy incomplete",
            "duration_ms": 0,
            "details": {"issues": schema_issues, "schema_policy": schema},
        })
    else:
        gates.append({
            "id": "g10_schema_policy",
            "status": GateStatus.PASS.value,
            "message": f"Schema policy set to {schema.replace('_', ' ')}",
            "duration_ms": 0,
            "details": {
                "schema_policy": schema,
                "backfill_new_fields": backfill_new_fields,
                "breaking_changes": "pause_for_manual_review",
            },
        })

    gates.append({
        "id": "g11_validation_posture",
        "status": GateStatus.PASS.value,
        "message": f"Validation posture {validation} uses confidence threshold {confidence_threshold_for_mode(validation):.2f}",
        "duration_ms": 0,
        "details": {
            "validation_mode": validation,
            "confidence_threshold": confidence_threshold_for_mode(validation),
        },
    })

    return gates


def apply_policy_gates(result: dict[str, Any], policy_gates: list[dict[str, Any]]) -> dict[str, Any]:
    if not policy_gates:
        return result

    gates = [*result.get("gates", []), *policy_gates]
    blockers = [
        *result.get("blockers", []),
        *[
            {"id": g["id"], "message": g["message"], "details": g.get("details", {})}
            for g in policy_gates
            if g.get("status") == GateStatus.BLOCK.value
        ],
    ]
    passed_count = sum(1 for g in gates if g.get("status") == GateStatus.PASS.value)
    total_gates = len(gates)
    return {
        **result,
        "passed": bool(result.get("passed")) and not blockers,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": gates,
        "blockers": blockers,
    }


def run_file_preflight(
    *,
    columns: list[str],
    column_types: dict[str, str],
    row_count: int,
    mappings: list[dict[str, Any]],
    destination_connected: bool = False,
    destination_error: str | None = None,
    source_connected: bool = True,
    source_error: str | None = None,
    source_kind: str = "file",
    sample_rows: list[dict] | None = None,
    estimated_bytes: int = 0,
    confidence_threshold: float = 0.85,
) -> dict[str, Any]:
    """Run 8 preflight gates for a file-based transfer."""
    if row_count <= 0 and sample_rows:
        row_count = len(sample_rows)

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
            confidence=float(m.get("confidence", 0.0)),
            transform=m.get("transform"),
            user_override=bool(m.get("user_override", False)),
            reasoning=m.get("reason", ""),
        )
        for m in mappings
    ]

    has_samples = bool(sample_rows)
    est_bytes = estimated_bytes if estimated_bytes > 0 else max(row_count * 128, 0)
    is_file_source = source_kind == "file"

    plan = TransferPlan(
        source=SourceConfig(
            kind=source_kind,
            connected=source_connected and bool(columns),
            parseable=(is_file_source and has_samples and bool(columns))
            or (not is_file_source and bool(columns)),
            columns=source_cols,
            row_count_estimate=row_count,
            error=source_error,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=destination_connected,
            can_create_table=destination_connected,
            can_write=destination_connected,
            target_columns=dest_cols,
            table_exists=False,
            error=destination_error,
        ),
        mappings=plan_mappings,
        dry_run_passed=False,
        ddl_compatible=destination_connected and bool(mappings),
        ddl_issues=[] if destination_connected else ["Destination not verified"],
        estimated_bytes=est_bytes,
        available_staging_bytes=max(est_bytes * 3, 50_000_000) if est_bytes else 50_000_000,
        confidence_threshold=confidence_threshold,
    )

    ctx = FilePreflightContext(plan, sample_rows)
    engine = PreflightEngine(fail_fast=False)
    result = engine.run(ctx)

    return {
        "passed": result.passed,
        "passed_count": result.passed_count,
        "total_gates": result.total_gates,
        "readiness_score": round(result.passed_count / max(result.total_gates, 1) * 100, 1),
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


def probe_destination(endpoint) -> tuple[bool, str]:
    """Live connectivity probe for database destinations (Gate G2)."""
    from ..transfer.adapters import resolve_connector_config

    if endpoint.kind != "database":
        return True, "Non-database destination"

    db_type = (endpoint.format or "").lower()
    cfg = resolve_connector_config(endpoint)

    if db_type == "mongodb":
        from ..services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if endpoint.connector_id:
            try:
                client, _ = mongo.get_client_for_connector(endpoint.connector_id)
                if client:
                    client.admin.command("ping")
                    client.close()
                    return True, "MongoDB reachable"
            except Exception as exc:
                return False, str(exc)
        return False, "MongoDB connector required for destination probe"

    probes = {
        "postgresql": ("connectors.postgresql", "test_postgresql"),
        "mysql": ("connectors.mysql", "test_mysql"),
        "snowflake": ("connectors.snowflake", "test_snowflake"),
        "bigquery": ("connectors.bigquery", "test_bigquery"),
    }
    if db_type not in probes:
        return False, f"No connectivity probe for {db_type}"

    import importlib

    mod_name, fn_name = probes[db_type]
    mod = importlib.import_module(mod_name)
    probe_fn = getattr(mod, fn_name)
    result = probe_fn(
        host=cfg.get("host", ""),
        port=int(cfg.get("port", 443 if db_type in ("snowflake", "bigquery") else 5432)),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        schema=cfg.get("schema", "PUBLIC" if db_type == "snowflake" else "dataflow" if db_type == "bigquery" else "public"),
        connection_string=cfg.get("connection_string", ""),
        ssl=cfg.get("ssl", False),
        warehouse=cfg.get("warehouse", ""),
    )
    if result.ok:
        return True, result.message or "Connected"
    return False, result.error or "Connection failed"
