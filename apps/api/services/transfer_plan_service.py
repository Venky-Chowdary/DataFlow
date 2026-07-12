"""Transfer plan orchestration — map, preflight, run with persisted contract."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from services.audit_log import append_audit_event
from services.transfer_plan_store import (
    TransferPlanRecord,
    add_mapping_revision,
    add_preflight_run,
    approve_plan_version,
    get_plan,
    sync_ui_mappings,
    update_plan,
)

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import read_source_database
from src.transfer.models import EndpointConfig


def _preflight():
    from src.services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        inspect_destination_for_preflight,
        run_file_preflight,
        run_transfer_policy_gates,
    )
    return (
        apply_policy_gates,
        confidence_threshold_for_mode,
        inspect_destination_for_preflight,
        run_file_preflight,
        run_transfer_policy_gates,
    )


def run_plan_mapping(
    plan_id: str,
    *,
    validation_mode: str = "balanced",
    use_llm: bool = True,
    source_samples: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    from services.mapping_pipeline import run_mapping_pipeline

    _, confidence_threshold_for_mode, _, _, _ = _preflight()

    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")

    threshold = confidence_threshold_for_mode(validation_mode)
    samples = source_samples or {}
    source_schemas = [
        {
            "name": c,
            "inferred_type": plan.source_schema.get(c, "VARCHAR"),
            "samples": [str(x) for x in samples.get(c, [])[:8]],
        }
        for c in plan.source_columns
    ]
    target_schemas = [
        {"name": c, "inferred_type": plan.target_schema.get(c, "VARCHAR"), "samples": []}
        for c in plan.target_columns
    ]

    result = run_mapping_pipeline(
        plan.source_columns,
        plan.target_columns,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        confidence_threshold=threshold,
        use_llm=use_llm,
        source_samples=source_samples,
        validation_mode=validation_mode,
    )

    updated = add_mapping_revision(plan_id, result)
    append_audit_event(
        action="transfer_plan.mapped",
        resource=f"plan/{plan_id}",
        details={
            "version": updated.active_version if updated else None,
            "mapped_count": len(result.get("mappings") or []),
            "validation_passed": result.get("validation", {}).get("passed"),
        },
    )
    return {
        "plan_id": plan_id,
        "version": updated.active_version if updated else None,
        "mapping_hash": (updated.active_revision().mapping_hash if updated and updated.active_revision() else ""),
        **result,
    }


def patch_plan(plan_id: str, data: dict[str, Any]) -> TransferPlanRecord:
    plan = update_plan(plan_id, data)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    append_audit_event(
        action="transfer_plan.updated",
        resource=f"plan/{plan_id}",
        details={"status": plan.status, "target_columns": len(plan.target_columns)},
    )
    return plan


def sync_plan_mappings(plan_id: str, mappings: list[dict[str, Any]]) -> TransferPlanRecord:
    plan = sync_ui_mappings(plan_id, mappings)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    append_audit_event(
        action="transfer_plan.mappings_synced",
        resource=f"plan/{plan_id}",
        details={"version": plan.active_version, "mapping_count": len(mappings)},
    )
    return plan


def run_plan_preflight(plan_id: str) -> dict[str, Any]:
    (
        apply_policy_gates,
        confidence_threshold_for_mode,
        inspect_destination_for_preflight,
        run_file_preflight,
        run_transfer_policy_gates,
    ) = _preflight()

    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    rev = plan.active_revision()
    if not rev or not rev.mappings:
        raise ValueError("Plan has no mapping revision — run /map first")

    dest = plan.destination
    dest_meta = inspect_destination_for_preflight(
        connector_id=dest.get("connector_id"),
        dest_type=dest.get("format") or dest.get("type"),
        dest_host=dest.get("host"),
        dest_port=int(dest.get("port") or 0) or None,
        dest_database=dest.get("database"),
        dest_table=dest.get("table"),
        dest_collection=dest.get("collection"),
        dest_schema=dest.get("schema"),
        dest_username=dest.get("username"),
        dest_password=dest.get("password"),
        dest_connection_string=dest.get("connection_string"),
        dest_warehouse=dest.get("warehouse"),
        dest_kind=dest.get("kind", "database"),
    )

    live_target_schema = dest_meta.get("column_types") or plan.target_schema
    live_target_columns = list(live_target_schema.keys()) if live_target_schema else plan.target_columns

    from services.schema_drift import detect_schema_drift

    drift = detect_schema_drift(
        source_columns=plan.source_columns,
        source_schema=plan.source_schema,
        target_columns=live_target_columns,
        target_schema=live_target_schema,
        stored_source_fp=rev.source_schema_hash,
        stored_target_fp=rev.target_schema_hash,
        mappings=rev.mappings,
        destination_db_type=(dest.get("format") or dest.get("type") or "").lower(),
    )

    policies = plan.policies
    validation_mode = policies.get("validation_mode", "strict")
    threshold = confidence_threshold_for_mode(validation_mode)

    sample_rows = plan.sample_rows or None
    if not sample_rows and plan.source.get("kind") == "database" and plan.source_columns:
        try:
            source_endpoint = EndpointConfig.from_dict(plan.source.get("kind", "database"), plan.source)
            records, _headers, _schema = read_source_database(
                source_endpoint, limit=100, raise_on_truncate=False
            )
            sample_rows = records[:100] if records else None
        except Exception:
            sample_rows = None

    pf = run_file_preflight(
        columns=plan.source_columns,
        column_types=plan.source_schema,
        row_count=plan.row_count_estimate,
        mappings=rev.mappings,
        destination_connected=bool(dest_meta.get("connected")),
        destination_error=None if dest_meta.get("connected") else dest_meta.get("message"),
        source_connected=True,
        source_kind=plan.source.get("kind", "file"),
        sample_rows=sample_rows,
        confidence_threshold=threshold,
        destination_column_types=live_target_schema,
        destination_table_exists=dest_meta.get("table_exists"),
        destination_can_create=dest_meta.get("can_create_table"),
        destination_db_type=(dest_meta.get("db_type") or dest.get("format") or dest.get("type") or "postgresql").lower(),
    )
    pf = apply_policy_gates(
        pf,
        run_transfer_policy_gates(
            sync_mode=policies.get("sync_mode", "full_refresh_overwrite"),
            schema_policy=policies.get("schema_policy", "manual_review"),
            validation_mode=validation_mode,
            stream_contracts=policies.get("stream_contracts"),
            backfill_new_fields=bool(policies.get("backfill_new_fields")),
        ),
        validation_mode=validation_mode,
    )

    if drift.get("drift_detected"):
        pf["schema_drift"] = drift
        if drift.get("severity") == "breaking" and policies.get("schema_policy") == "pause_on_change":
            pf["passed"] = False
            pf.setdefault("blockers", []).append({
                "gate": "schema_drift",
                "message": drift["issues"][0] if drift.get("issues") else "Schema drift detected",
            })

    add_preflight_run(plan_id, pf)
    append_audit_event(
        action="transfer_plan.preflight",
        resource=f"plan/{plan_id}",
        level="success" if pf.get("passed") else "warn",
        details={
            "passed": pf.get("passed"),
            "readiness_score": pf.get("readiness_score"),
            "mapping_version": rev.version,
            "mapping_hash": rev.mapping_hash,
            "drift_detected": drift.get("drift_detected"),
        },
    )
    return {"plan_id": plan_id, "mapping_version": rev.version, "schema_drift": drift, **pf}


def build_run_payload(plan_id: str) -> dict[str, Any]:
    """Build transfer run payload from approved plan revision."""
    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    rev = plan.active_revision()
    if not rev:
        raise ValueError("Plan has no mappings")
    if plan.status not in {"approved", "preflight_passed", "mapped"}:
        raise ValueError(f"Plan status '{plan.status}' is not runnable — approve after preflight")

    return {
        "plan_id": plan_id,
        "mapping_version": rev.version,
        "mapping_hash": rev.mapping_hash,
        "mappings": rev.mappings,
        "source": plan.source,
        "destination": plan.destination,
        "column_types": plan.source_schema,
        "policies": plan.policies,
    }


def approve_plan(plan_id: str, version: int | None = None) -> TransferPlanRecord:
    plan = approve_plan_version(plan_id, version)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    append_audit_event(
        action="transfer_plan.approved",
        resource=f"plan/{plan_id}",
        level="success",
        details={"version": plan.active_version},
    )
    return plan
