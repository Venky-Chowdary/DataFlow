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

from services.connector_capability_registry import (
    classify_payload,
    recommended_batch_size,
)
from services.validation_plan import build_validation_plan


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
        dest_types_by_name = {c.name: c.inferred_type for c in self.plan.destination.target_columns}
        mapping_dicts = [
            {
                "source": m.source,
                "target": m.target,
                "transform": getattr(m, "transform", ""),
                "target_type": dest_types_by_name.get(m.target),
            }
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

    def run_integrity_audit(self) -> dict[str, Any]:
        from services.data_integrity import run_integrity_audit as audit

        source_columns = [c.name for c in self.plan.source.columns]
        mapping_dicts = [
            {
                "source": m.source,
                "target": m.target,
                "confidence": m.confidence,
                "transform": m.transform,
                "requires_review": m.requires_review,
                "target_type": next(
                    (c.inferred_type for c in self.plan.destination.target_columns if c.name == m.target),
                    None,
                ),
            }
            for m in self.plan.mappings
        ]
        source_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type, "samples": c.samples}
            for c in self.plan.source.columns
        ]
        target_schemas = [
            {"name": c.name, "inferred_type": c.inferred_type}
            for c in self.plan.destination.target_columns
        ]
        mode = getattr(self.plan, "validation_mode", "strict") or "strict"
        return audit(
            source_columns=source_columns,
            mappings=mapping_dicts,
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            sample_rows=self.sample_rows,
            validation_mode=mode,
            destination_db_type=self.plan.destination.db_type,
        )


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
    requires_primary_key = sync in {"upsert", "incremental_deduped", "cdc"}

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


def is_compliance_only_block(proof_blockers: list[str]) -> bool:
    """Return True when every proof blocker is purely a PII/compliance review."""
    if not proof_blockers:
        return False
    return all(
        "PII/compliance" in b or "compliance review" in b.lower()
        for b in proof_blockers
    )


def apply_policy_gates(
    result: dict[str, Any],
    policy_gates: list[dict[str, Any]],
    validation_mode: str = "strict",
    destination_db_type: str = "postgresql",
) -> dict[str, Any]:
    proof_bundle = result.get("proof_bundle") or {}
    transfer_decision = (proof_bundle.get("transfer_decision") or {}).get("decision")
    proof_blockers = (proof_bundle.get("transfer_decision") or {}).get("blockers") or []

    is_strict = (validation_mode or "strict").lower() in {"strict", "maximum"}
    compliance_only = is_compliance_only_block(proof_blockers)

    # In non-strict modes, PII/compliance review is a warning, not a hard blocker.
    # Real transfers with email/name fields should be able to proceed after the user
    # sees the compliance risk. Reconciliation failures and semantic confidence issues
    # still stop the transfer.
    if is_strict:
        active_proof_blockers = list(proof_blockers)
    else:
        active_proof_blockers = [
            b for b in proof_blockers
            if "PII/compliance" not in b and "compliance review" not in b.lower()
        ]

    blockers = [
        {"id": b["id"], "message": b["message"], "details": b.get("details", {})}
        for b in result.get("blockers", [])
    ]
    blockers.extend(
        {"id": f"proof_{idx}", "message": str(message), "details": {}}
        for idx, message in enumerate(active_proof_blockers)
    )

    if policy_gates:
        gates = [*result.get("gates", []), *policy_gates]
        blockers.extend(
            {"id": g["id"], "message": g["message"], "details": g.get("details", {})}
            for g in policy_gates
            if g.get("status") == GateStatus.BLOCK.value
        )
    else:
        gates = list(result.get("gates", []))

    passed_count = sum(1 for g in gates if g.get("status") == GateStatus.PASS.value)
    total_gates = len(gates)
    has_blocks = any(g.get("status") == GateStatus.BLOCK.value for g in gates)

    proof_blocks = transfer_decision in {"block", "review"} or proof_bundle.get("passed") is False
    if proof_blocks and not is_strict:
        if active_proof_blockers:
            proof_blocks = True
        else:
            proof_blocks = False

    if proof_blocks:
        has_blocks = True

    if proof_bundle:
        proof_bundle = {**proof_bundle}
        base_decision = proof_bundle.get("transfer_decision") or {}
        if has_blocks:
            gate_blocker_messages = [b["message"] for b in blockers]
            decision_blockers = list(base_decision.get("blockers") or [])
            for msg in gate_blocker_messages:
                if msg not in decision_blockers:
                    decision_blockers.append(msg)
            proof_bundle["passed"] = False
            proof_bundle["transfer_decision"] = {
                "decision": "block",
                "blockers": decision_blockers,
                "reason": "; ".join(decision_blockers) if decision_blockers else "Preflight gates blocked the transfer",
                "warnings": [],
            }
        else:
            # No hard gate blocks; downgrade proof decision to review/approve and surface
            # compliance warnings so the UI shows the risk without disabling the transfer.
            warnings = [b for b in proof_blockers if b not in active_proof_blockers]
            decision = "review" if (transfer_decision in {"block", "review"} or compliance_only) else "approve"
            proof_bundle["passed"] = True
            proof_bundle["transfer_decision"] = {
                "decision": decision,
                "blockers": [],
                "reason": (
                    "No blocking issues detected" if not warnings
                    else "; ".join(warnings)
                ),
                "warnings": warnings,
            }

    from services.ddl_compatibility import _normalize_dest_kind
    from services.preflight_rules import enrich_blockers

    dest_kind = _normalize_dest_kind(destination_db_type)
    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    return {
        **result,
        "passed": not has_blocks,
        "passed_count": passed_count,
        "total_gates": total_gates,
        "readiness_score": round(passed_count / max(total_gates, 1) * 100, 1),
        "gates": gates,
        "blockers": enriched_blockers,
        "proof_bundle": proof_bundle,
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
    source_format: str = "",
    sync_mode: str = "append",
    sample_rows: list[dict] | None = None,
    estimated_bytes: int = 0,
    confidence_threshold: float = 0.85,
    validation_mode: str = "strict",
    destination_column_types: dict[str, str] | None = None,
    destination_table_exists: bool | None = None,
    destination_can_create: bool | None = None,
    available_staging_bytes: int | None = None,
    destination_db_type: str = "postgresql",
) -> dict[str, Any]:
    """Run 9 preflight gates for a file-based transfer."""
    if row_count <= 0 and sample_rows:
        row_count = len(sample_rows)

    # If the caller did not supply rich source types, infer them from the sample
    # rows. This keeps schemaless sources (MongoDB, DynamoDB, Redis, S3 JSON) from
    # being treated as all-VARCHAR against a typed warehouse target.
    if sample_rows and columns:
        generic_types = {"", "varchar", "text", "string"}
        if not column_types or all((column_types.get(c) or "").lower() in generic_types for c in columns):
            try:
                from services.file_parser import FileParser

                inferred = FileParser.infer_schema(sample_rows)
                if inferred:
                    column_types = {**column_types, **{c: inferred.get(c, column_types.get(c, "VARCHAR")) for c in columns}}
            except Exception:
                pass

    source_cols = [
        ColumnSchema(name=c, inferred_type=column_types.get(c, "VARCHAR").upper())
        for c in columns
    ]
    dest_types = destination_column_types or {}
    dest_cols = [
        ColumnSchema(
            name=m["target"],
            inferred_type=dest_types.get(
                m["target"],
                m.get("target_type") or column_types.get(m["source"], "VARCHAR"),
            ).upper(),
        )
        for m in mappings
    ]
    plan_mappings = [
        ColumnMapping(
            source=m["source"],
            target=m["target"],
            confidence=float(m.get("confidence", 0.0)),
            transform=m.get("transform"),
            user_override=bool(m.get("user_override", False)),
            reasoning=m.get("reasoning") or m.get("reason", ""),
            requires_review=bool(m.get("requires_review", False)),
            score_gap=float(m.get("score_gap", 1.0)),
        )
        for m in mappings
    ]

    has_samples = bool(sample_rows)
    est_bytes = estimated_bytes if estimated_bytes > 0 else max(row_count * 128, 0)
    is_file_source = source_kind == "file"

    if available_staging_bytes is None:
        available_staging_bytes = _available_staging_bytes(est_bytes)

    dest_can_create = destination_can_create if destination_can_create is not None else destination_connected
    dest_table_exists = destination_table_exists if destination_table_exists is not None else False

    from services.ddl_compatibility import (
        _normalize_dest_kind,
        evaluate_ddl_compatibility,
    )
    from services.schema_drift import detect_schema_drift

    dest_kind = _normalize_dest_kind(destination_db_type)
    schemaless = dest_kind in {"mongodb", "dynamodb", "redis"}

    target_cols = list((destination_column_types or {}).keys())
    ddl_compatible, ddl_issues = evaluate_ddl_compatibility(
        mappings=mappings,
        source_schema=column_types,
        target_schema=destination_column_types or {},
        sample_rows=sample_rows,
        table_exists=dest_table_exists,
        dest_connected=destination_connected,
        dest_db_type=destination_db_type,
        allow_create=dest_can_create,
    )

    drift = detect_schema_drift(
        source_columns=columns,
        source_schema=column_types,
        target_columns=target_cols or [m["target"] for m in mappings],
        target_schema=destination_column_types or {},
        mappings=mappings,
        destination_db_type=destination_db_type,
    )
    if drift.get("drift_detected"):
        for issue in drift.get("issues", []):
            if issue not in ddl_issues:
                ddl_issues.append(issue)
        if drift.get("severity") == "breaking":
            ddl_compatible = False

    sample_quality: dict[str, Any] = {}
    if sample_rows and columns:
        from services.sample_quality import analyze_dataset_quality

        sample_quality = analyze_dataset_quality(columns, sample_rows, schema=column_types, dest_kind=dest_kind)
        # For schemaless destinations (MongoDB, DynamoDB, Redis) missing/optional fields
        # are normal; high null rates should not be treated as DDL blockers.
        if sample_quality.get("blocks_transfer") and not schemaless:
            ddl_compatible = False
            for issue in sample_quality.get("issues", [])[:10]:
                if issue not in ddl_issues:
                    ddl_issues.append(issue)
        if schemaless:
            sample_quality["blocks_transfer"] = False

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
            db_type=dest_kind,
            connected=destination_connected,
            can_create_table=dest_can_create,
            can_write=destination_connected,
            target_columns=dest_cols,
            table_exists=dest_table_exists,
            error=destination_error,
        ),
        mappings=plan_mappings,
        dry_run_passed=False,
        ddl_compatible=ddl_compatible,
        ddl_issues=ddl_issues,
        estimated_bytes=est_bytes,
        available_staging_bytes=available_staging_bytes,
        confidence_threshold=confidence_threshold,
        validation_mode=validation_mode,
    )

    ctx = FilePreflightContext(plan, sample_rows)
    engine = PreflightEngine(fail_fast=False)
    result = engine.run(ctx)

    from services.preflight_proof_bundle import build_preflight_proof_bundle

    proof_bundle = build_preflight_proof_bundle(
        columns=columns,
        sample_rows=sample_rows or [],
        mappings=mappings,
        source_schemas=[
            {
                "name": c,
                "inferred_type": column_types.get(c, "VARCHAR").upper(),
                "samples": [str(row.get(c, "")) for row in (sample_rows or [])[:20] if row.get(c) is not None],
            }
            for c in columns
        ],
        source_records=sample_rows or [],
        target_records=[],
        validation_mode=validation_mode,
        confidence_threshold=confidence_threshold,
    )

    from services.preflight_rules import enrich_blockers

    blockers = [
        {"id": b.gate_id.value, "message": b.message, "details": b.details}
        for b in result.blockers
    ]
    enriched_blockers = enrich_blockers(
        blockers,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )

    from services.type_system import is_binary_type, is_structural_type

    has_binary = any(is_binary_type(t) for t in column_types.values())
    has_unstructured = any(is_structural_type(t) for t in column_types.values())
    _src_fmt = (source_format or source_kind).lower()
    _tgt_fmt = (destination_db_type or "").lower()
    payload_shape = classify_payload(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        has_binary=has_binary,
        has_unstructured=has_unstructured,
    )
    validation_plan = build_validation_plan(
        source_format=_src_fmt,
        target_format=_tgt_fmt,
        validation_mode=validation_mode,
        write_semantics=sync_mode,
        confidence_threshold=confidence_threshold,
    )

    out = {
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
        "blockers": enriched_blockers,
        "schema_drift": drift,
        "ddl_issues": ddl_issues,
        "sample_quality": sample_quality,
        "proof_bundle": proof_bundle,
        "payload_shape": payload_shape,
        "validation_plan": validation_plan.to_dict(),
        "recommended_batch_size": min(
            recommended_batch_size(_src_fmt),
            recommended_batch_size(_tgt_fmt) or recommended_batch_size(_src_fmt),
        ),
    }
    return out


def probe_destination(endpoint) -> tuple[bool, str]:
    """Live connectivity probe for database destinations (Gate G2)."""
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_registry import run_probe

    if endpoint.kind != "database":
        return True, "Non-database destination"

    db_type = (endpoint.format or "").lower()
    cfg = resolve_connector_config(endpoint)
    # DynamoDB uses the table name as the database identifier; ensure the
    # connectivity probe sees the intended destination table.
    if db_type == "dynamodb":
        cfg["table"] = resolve_dest_table(db_type, endpoint)
    return run_probe(db_type, cfg)


def _available_staging_bytes(estimated_bytes: int) -> int:
    """Estimate writable staging capacity from local exports volume."""
    import shutil
    from pathlib import Path

    export_dir = Path(__file__).resolve().parents[2] / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(export_dir)
        # Reserve 15% headroom; require at least 3× estimated transfer size
        usable = int(usage.free * 0.85)
        required = max(estimated_bytes * 3, 1_048_576)
        return max(usable, required) if usable >= required else usable
    except OSError:
        return max(estimated_bytes * 3, 8_388_608)


def inspect_destination_for_preflight(
    *,
    connector_id: str | None = None,
    dest_type: str | None = None,
    dest_host: str | None = None,
    dest_port: int | None = None,
    dest_database: str | None = None,
    dest_table: str | None = None,
    dest_collection: str | None = None,
    dest_schema: str | None = None,
    dest_username: str | None = None,
    dest_password: str | None = None,
    dest_connection_string: str | None = None,
    dest_warehouse: str | None = None,
    dest_auth_source: str | None = None,
    dest_auth_mode: str | None = None,
    dest_auth_role: str | None = None,
    dest_api_key: str | None = None,
    dest_service_account: str | None = None,
    dest_kind: str = "database",
) -> dict[str, Any]:
    """Introspect destination for table existence and column schema."""
    out: dict[str, Any] = {
        "connected": False,
        "table_exists": False,
        "can_create_table": False,
        "column_types": {},
        "columns": [],
        "db_type": (dest_type or "").lower(),
        "message": "",
    }
    if dest_kind == "file_export":
        out["connected"] = True
        out["can_create_table"] = True
        out["message"] = "File export destination"
        return out

    from src.transfer.adapters import _lookup_saved_connector
    from src.transfer.models import EndpointConfig

    if connector_id:
        conn = _lookup_saved_connector(connector_id)
        if not conn:
            out["message"] = f"Connector '{connector_id}' not found"
            return out
        db_type = (conn.get("type") or "mongodb").lower()
        out["db_type"] = db_type
        endpoint = EndpointConfig(
            kind="database",
            format=db_type,
            connector_id=connector_id,
            host=conn.get("host", ""),
            port=int(conn.get("port") or 0),
            database=conn.get("database", ""),
            schema=conn.get("schema", "public"),
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            username=conn.get("username", ""),
            password=conn.get("password", ""),
            connection_string=conn.get("connection_string", ""),
            warehouse=conn.get("warehouse", ""),
            ssl=conn.get("ssl", False),
            auth_source=conn.get("auth_source", ""),
            auth_mode=conn.get("auth_mode", ""),
            auth_role=conn.get("auth_role", ""),
            api_key=conn.get("api_key", ""),
            service_account=conn.get("service_account", ""),
        )
    elif dest_host or dest_connection_string:
        db_type = (dest_type or "mongodb").lower()
        out["db_type"] = db_type
        endpoint = EndpointConfig(
            kind="database",
            format=db_type,
            host=dest_host or "localhost",
            port=int(dest_port or 0),
            database=dest_database or "",
            schema=dest_schema or "public",
            table=dest_table or "",
            collection=dest_collection or dest_table or "",
            username=dest_username or "",
            password=dest_password or "",
            connection_string=dest_connection_string or "",
            warehouse=dest_warehouse or "",
            auth_source=dest_auth_source or "",
            auth_mode=dest_auth_mode or "",
            auth_role=dest_auth_role or "",
            api_key=dest_api_key or "",
            service_account=dest_service_account or "",
        )
    else:
        out["message"] = "Destination not configured"
        return out

    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(endpoint)
    out["connected"] = bool(info.get("connected"))
    out["message"] = info.get("message", "")
    schema = info.get("schema") or {}
    cols = info.get("columns") or list(schema.keys())
    out["columns"] = cols
    out["column_types"] = schema
    stream = dest_collection or dest_table or endpoint.collection or endpoint.table
    if stream and cols:
        out["table_exists"] = True
    elif stream and info.get("objects"):
        names = {o.get("name") for o in info.get("objects", []) if isinstance(o, dict)}
        out["table_exists"] = stream in names
    out["can_create_table"] = out["connected"]
    return out
