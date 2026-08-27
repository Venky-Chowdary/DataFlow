"""Transfer plan orchestration — map, preflight, run with persisted contract."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

from services.acknowledgment_contract import (
    Acknowledgments,
    acknowledgments_from_policies,
    audit_acknowledgments,
)
from services.audit_log import append_audit_event
from services.file_parser import iter_stored_upload_rows
from services.population_fit_scan import STUDIO_FIT_SCAN_SECONDS
from services.shape_preflight import shaped_population_rows, shaped_preflight_image
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

    from services.data_profiler import source_types_are_authoritative

    policies = getattr(plan, "policies", None) or {}
    dest = plan.destination if isinstance(plan.destination, dict) else {}
    src = plan.source if isinstance(plan.source, dict) else {}
    dest_extra = dest.get("extra") if isinstance(dest.get("extra"), dict) else {}
    # Prefer nested extra (Studio SSOT) then flat root (legacy plans).
    table_exists = dest_extra.get("table_exists", dest.get("table_exists"))
    if not isinstance(table_exists, bool):
        table_exists = None
    result = run_mapping_pipeline(
        plan.source_columns,
        plan.target_columns,
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        confidence_threshold=threshold,
        use_llm=use_llm,
        source_samples=source_samples,
        validation_mode=validation_mode,
        destination_db_type=(plan.destination.get("format") or plan.destination.get("type") or "").lower(),
        schema_policy=policies.get("schema_policy", "manual_review"),
        sync_mode=str(policies.get("sync_mode") or ""),
        destination_table_exists=table_exists,
        source_types_authoritative=source_types_are_authoritative(
            str(src.get("kind") or ""),
            str(src.get("format") or ""),
        ),
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


def _plan_fit_progress(plan_id: str, rows_estimate: int):
    """Heartbeat into the in-memory job so GET /preflight is live, not a hang."""

    def _on_progress(scanned: int) -> None:
        from services.plan_preflight_job import report_plan_preflight_progress

        report_plan_preflight_progress(
            plan_id,
            rows_scanned=scanned,
            rows_estimate=rows_estimate,
            phase="scanning_population_fit",
        )

    return _on_progress


def run_plan_preflight(
    plan_id: str,
    *,
    acknowledgments: Acknowledgments | None = None,
    shape_recipe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run preflight for a persisted plan, honouring operator acknowledgments.

    ``acknowledgments`` is what the operator just attested on Validate. It is
    recorded on the plan (stamped with the mapping revision it was granted for)
    so Execute and a page reload see the same attestation, and so a remap cannot
    inherit a green from the shape it replaced.

    ``shape_recipe`` is the approved pre-load transform. Execute shapes rows on
    the read, so the gates must judge that image: without it Validate blocks on
    values — a control character, an unrounded decimal — that the writer never
    sees. Raises :class:`ShapePreflightRefused` when the recipe cannot run.
    """
    try:
        from services.plan_preflight_job import report_plan_preflight_progress

        report_plan_preflight_progress(plan_id, phase="running_gates")
    except Exception:
        pass
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
        dest_auth_source=dest.get("auth_source"),
        dest_auth_mode=dest.get("auth_mode"),
        dest_auth_role=dest.get("auth_role"),
        dest_api_key=dest.get("api_key"),
        dest_service_account=dest.get("service_account"),
        dest_kind=dest.get("kind", "database"),
        dest_extra=dest.get("extra") if isinstance(dest.get("extra"), dict) else {},
    )

    live_target_schema = dest_meta.get("column_types") or {}
    # Prefer live introspect. Never fall back to Map stamps when the table
    # exists but live schema is empty — that greens empties as VARCHAR while
    # write binds physical DATE/INT (client-deploy wipe cliff).
    table_exists = (
        dest_meta.get("table_exists")
        if isinstance(dest_meta.get("table_exists"), bool)
        else (
            dest.get("table_exists")
            if isinstance(dest.get("table_exists"), bool)
            else None
        )
    )
    if not live_target_schema and table_exists is True:
        live_target_schema = {}
    live_target_columns = list(live_target_schema.keys()) if live_target_schema else plan.target_columns

    ack = acknowledgments or Acknowledgments()
    if ack.any_claimed:
        updated = update_plan(
            plan_id,
            {"policies": ack.as_policies(mapping_version=plan.active_version)},
        )
        plan = updated or plan
        audit_acknowledgments(
            ack,
            resource=f"plan/{plan_id}",
            details={
                "mapping_version": plan.active_version,
                "destination_table": dest.get("table") or dest.get("collection") or "",
            },
        )
    else:
        ack = acknowledgments_from_policies(
            plan.policies, mapping_version=plan.active_version
        )

    policies = plan.policies
    validation_mode = policies.get("validation_mode", "balanced")
    threshold = confidence_threshold_for_mode(validation_mode)

    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT
    from services.primary_key import extract_contract_primary_key_columns

    sample_rows = list(plan.sample_rows or []) or None
    # Studio often persists a 25-row snapshot. Execute preflight samples up to
    # PREFLIGHT_SAMPLE_LIMIT — refetch when the cache is thinner so Validate
    # cannot Approve on a sample Execute will expand and then block.
    if (
        plan.source.get("kind") == "database"
        and plan.source_columns
        and (not sample_rows or len(sample_rows) < PREFLIGHT_SAMPLE_LIMIT)
    ):
        try:
            source_endpoint = EndpointConfig.from_dict(
                plan.source.get("kind", "database"), plan.source
            )
            records, _headers, _schema = read_source_database(
                source_endpoint,
                limit=PREFLIGHT_SAMPLE_LIMIT,
                raise_on_truncate=False,
            )
            fetched = records[:PREFLIGHT_SAMPLE_LIMIT] if records else None
            if fetched and (not sample_rows or len(fetched) >= len(sample_rows)):
                sample_rows = fetched
        except Exception:
            pass
    if sample_rows and len(sample_rows) > PREFLIGHT_SAMPLE_LIMIT:
        sample_rows = sample_rows[:PREFLIGHT_SAMPLE_LIMIT]

    # Previous = revision snapshot (or empty for legacy revisions without snapshot).
    prev_cols = list(rev.source_columns or [])
    prev_schema = dict(rev.source_schema or {})

    stream_contracts = list(policies.get("stream_contracts") or [])
    dest_table = str(dest.get("table") or dest.get("collection") or "")
    # Full composite — never truncate to first column (Validate≡Execute probe SSOT).
    contract_pk_cols = extract_contract_primary_key_columns(
        stream_contracts, stream_name=dest_table, fallback_first=False
    )
    if not contract_pk_cols:
        selected = [
            c for c in stream_contracts if isinstance(c, dict) and c.get("selected", True)
        ]
        if len(selected) == 1:
            contract_pk_cols = extract_contract_primary_key_columns(selected)
    contract_pk = ",".join(contract_pk_cols) if contract_pk_cols else None

    source = plan.source if isinstance(plan.source, dict) else {}
    source_connector_id = str(source.get("connector_id") or "")
    source_table = str(source.get("table") or source.get("collection") or "")
    dest_db_type = (
        dest_meta.get("db_type") or dest.get("format") or dest.get("type") or "postgresql"
    ).lower()
    source_kind = str(source.get("kind") or "file")
    source_format = str(source.get("format") or source.get("kind") or "file")
    source_file_id = str(
        source.get("file_id")
        or (source.get("extra") or {}).get("file_id")
        or ""
    ).strip()

    # run_file_preflight is the SSOT for drift + gates — do not re-detect/overwrite.
    # Source connector/table/config + stream_contracts required for uniqueness probe
    # (Studio plan Validate must not skip the probe Execute will run).
    shaped_image = shaped_preflight_image(
        shape_recipe,
        columns=plan.source_columns,
        column_types=plan.source_schema,
        sample_rows=sample_rows,
    )

    stored_population = iter_stored_upload_rows(source_file_id) if source_file_id else None
    if stored_population is not None and shaped_image.applied:
        stored_population = shaped_population_rows(
            shape_recipe,
            stored_population,
            source_columns=shaped_image.columns,
        )

    pf = run_file_preflight(
        columns=shaped_image.columns,
        column_types=shaped_image.column_types,
        row_count=plan.row_count_estimate,
        mappings=rev.mappings,
        destination_connected=bool(dest_meta.get("connected")),
        destination_error=None if dest_meta.get("connected") else dest_meta.get("message"),
        source_connected=True,
        source_kind=source_kind,
        source_format=source_format,
        sync_mode=policies.get("sync_mode", "full_refresh_overwrite"),
        sample_rows=shaped_image.sample_rows,
        source_file_id=source_file_id,
        population_rows=stored_population,
        rows_are_population=stored_population is not None,
        shape_recipe=shape_recipe,
        confidence_threshold=threshold,
        validation_mode=validation_mode,
        date_locale=policies.get("date_locale", ""),
        number_locale=policies.get("number_locale", ""),
        destination_column_types=live_target_schema,
        destination_column_nullability=dest_meta.get("column_nullability") or {},
        destination_column_defaults=dest_meta.get("column_defaults") or {},
        destination_identity_columns=dest_meta.get("identity_columns") or [],
        destination_generated_columns=dest_meta.get("generated_columns") or [],
        destination_table_exists=table_exists,
        destination_can_create=dest_meta.get("can_create_table"),
        destination_can_write=dest_meta.get("can_write"),
        privilege_probe=dest_meta.get("privilege_probe"),
        redshift_staging_probe=dest_meta.get("redshift_staging_probe"),
        destination_db_type=dest_db_type,
        source_connector_id=source_connector_id,
        source_config=dict(source) if source else None,
        source_table=source_table,
        destination_table=dest_table,
        schema_policy=policies.get("schema_policy", "manual_review"),
        backfill_new_fields=bool(policies.get("backfill_new_fields")),
        # Drift compares against the revision's declared-source fingerprint, so
        # it must see the declared source — an approved transform is not drift.
        declared_source_columns=list(plan.source_columns),
        declared_source_schema=dict(plan.source_schema or {}),
        stored_source_fp=rev.source_schema_hash or "",
        stored_target_fp=rev.target_schema_hash or "",
        previous_source_columns=prev_cols or None,
        previous_source_schema=prev_schema or None,
        contract_primary_key=contract_pk,
        destination_pk_columns=dest_meta.get("primary_key_columns") or dest_meta.get("pk_columns"),
        destination_unique_keys=dest_meta.get("unique_keys") or [],
        destination_foreign_keys=dest_meta.get("foreign_keys") or [],
        # Without the destination connection the append key-collision probe cannot
        # run, and Validate greens a batch Execute then refuses on the same key.
        destination_config=dest_meta.get("_probe_cfg") or None,
        stream_contracts=stream_contracts,
        compliance_acknowledged=ack.compliance,
        schema_drift_acknowledged=ack.schema_drift,
        fk_risk_acknowledged=ack.fk_risk,
        acknowledgment_actor=ack.actor,
        acknowledgment_reason=ack.reason,
        fit_scan_seconds=STUDIO_FIT_SCAN_SECONDS,
        on_fit_progress=_plan_fit_progress(plan_id, int(plan.row_count_estimate or 0)),
    )
    pf = apply_policy_gates(
        pf,
        run_transfer_policy_gates(
            sync_mode=policies.get("sync_mode", "full_refresh_overwrite"),
            schema_policy=policies.get("schema_policy", "manual_review"),
            validation_mode=validation_mode,
            stream_contracts=stream_contracts,
            backfill_new_fields=bool(policies.get("backfill_new_fields")),
            source_columns=shaped_image.columns,
            dest_type=dest_db_type,
            source_type=source_format,
            source_kind=source_kind,
            write_via_staging=bool(policies.get("write_via_staging")),
            source_read_mode=str(
                (source.get("source_read_mode") or (source.get("extra") or {}).get("source_read_mode") or "")
            ),
        ),
        validation_mode=validation_mode,
        destination_db_type=dest_db_type,
    )

    if pf.get("effective_mappings"):
        pf["mappings"] = pf["effective_mappings"]
    if shaped_image.applied:
        # Name the image the gates judged. A verdict on transformed rows that
        # reads as a verdict on the source is how an operator loses the thread.
        pf["transform_image"] = {
            "recipe_hash": shaped_image.recipe_hash,
            "columns": shaped_image.columns,
            "sample_rows_in": shaped_image.rows_in,
            "sample_rows_out": shaped_image.rows_out,
            "sample_rows_removed": shaped_image.rows_removed,
            "sample_rows_diverted": shaped_image.rows_diverted,
            "retyped_columns": shaped_image.retyped_columns,
        }
        note = shaped_image.note()
        if note:
            bucket = pf.setdefault("warnings", [])
            if note not in bucket:
                bucket.append(note)
    # Surface destination catalog honesty (BQ/Redshift/SF NOT ENFORCED) warn-only.
    for w in dest_meta.get("warnings") or dest_meta.get("schema_warnings") or []:
        note = str(w).strip()
        if not note:
            continue
        bucket = pf.setdefault("warnings", [])
        if note not in bucket:
            bucket.append(note)
    # Every preflight verdict must be citable: the persisted run id is the handle
    # Execute is unlocked against, so it travels back with the result instead of
    # leaving the caller to infer a run that has no identity.
    pf["run_id"] = str(pf.get("run_id") or f"pf_{uuid.uuid4().hex[:12]}")
    add_preflight_run(plan_id, pf)
    drift = pf.get("schema_drift") or {}
    append_audit_event(
        action="transfer_plan.preflight",
        resource=f"plan/{plan_id}",
        level="success" if pf.get("passed") else "warn",
        details={
            "passed": pf.get("passed"),
            "readiness_score": pf.get("readiness_score"),
            "run_id": pf["run_id"],
            "mapping_version": rev.version,
            "mapping_hash": rev.mapping_hash,
            "drift_detected": drift.get("drift_detected"),
        },
    )
    return {"plan_id": plan_id, "mapping_version": rev.version, **pf}


def build_run_payload(plan_id: str) -> dict[str, Any]:
    """Build transfer run payload from approved plan revision."""
    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")
    rev = plan.active_revision()
    if not rev:
        raise ValueError(
            "Plan has no mapping revision — open Map / Validate so mappings are "
            "synced before Execute (empty draft plans cannot run)"
        )
    if not rev.mappings:
        raise ValueError(
            "Plan mapping revision is empty — re-run Map or Validate to sync column mappings"
        )
    if plan.status not in {"approved", "preflight_passed", "mapped"}:
        raise ValueError(f"Plan status '{plan.status}' is not runnable — approve after preflight")

    from services.mapping_proof import mapping_proof_or_build

    dest = plan.destination or {}
    src = plan.source or {}
    mapping_proof = mapping_proof_or_build(
        rev.mappings,
        existing=dict(rev.mapping_proof or {}),
        target_columns=plan.target_columns,
        destination_db_type=str(dest.get("format") or dest.get("type") or ""),
        source_kind=str(src.get("kind") or ""),
        dest_kind=str(dest.get("kind") or ""),
        sync_mode=str((plan.policies or {}).get("sync_mode") or ""),
    )

    from src.transfer.contract_engine import resolve_bound_contract

    cid, require = resolve_bound_contract(policies=plan.policies)
    payload = {
        "plan_id": plan_id,
        "mapping_version": rev.version,
        "mapping_hash": rev.mapping_hash,
        "mappings": rev.mappings,
        "mapping_proof": mapping_proof,
        "source": plan.source,
        "destination": plan.destination,
        "column_types": plan.source_schema,
        "policies": plan.policies,
    }
    if cid:
        payload["contract_id"] = cid
        payload["require_signed_contract"] = require
    return payload


def merge_plan_into_run(
    plan_id: str,
    *,
    request_mappings: list[dict[str, Any]] | None = None,
    request_column_types: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load plan policies/mappings for Execute without failing a Studio race.

    Map can fire-and-forget create an empty draft plan; Validate may then pass via
    direct preflight while ``persistedPlanId`` still points at that empty draft.
    When the request already carries mappings, recover by syncing them onto the
    plan instead of hard-failing with "Plan has no mappings".
    """
    plan = get_plan(plan_id)
    if not plan:
        raise ValueError(f"Plan '{plan_id}' not found")

    rev = plan.active_revision()
    if rev and rev.mappings:
        return build_run_payload(plan_id)

    maps = list(request_mappings or [])
    if not maps:
        raise ValueError(
            "Plan has no mapping revision — open Map / Validate so mappings are "
            "synced before Execute (empty draft plans cannot run)"
        )

    # Persist Studio mappings onto the empty draft. sync → status "mapped" (runnable).
    synced = sync_plan_mappings(plan_id, maps)
    if synced and synced.active_revision() and synced.active_revision().mappings:
        return build_run_payload(plan_id)

    from services.mapping_proof import mapping_proof_or_build

    dest = plan.destination or {}
    src = plan.source or {}
    proof = mapping_proof_or_build(
        maps,
        target_columns=plan.target_columns,
        destination_db_type=str(dest.get("format") or dest.get("type") or ""),
        source_kind=str(src.get("kind") or ""),
        dest_kind=str(dest.get("kind") or ""),
        sync_mode=str((plan.policies or {}).get("sync_mode") or ""),
    )
    return {
        "plan_id": plan_id,
        "mapping_version": 0,
        "mapping_hash": "",
        "mappings": maps,
        "mapping_proof": proof,
        "source": plan.source,
        "destination": plan.destination,
        "column_types": request_column_types or plan.source_schema,
        "policies": plan.policies,
        "recovered_from_request_mappings": True,
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
