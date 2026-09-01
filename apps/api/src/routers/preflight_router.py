"""Preflight API — 8-gate validation before transfer."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.acknowledgment_contract import (
    AcknowledgmentRefused,
    audit_acknowledgments,
    resolve_acknowledgments,
)
from services.preflight_cursor_gate import resolve_read_scope
from services.shape_preflight import (
    ShapePreflightRefused,
    shaped_population_rows,
    shaped_preflight_image,
)

from ..services.preflight_service import (
    apply_policy_gates,
    confidence_threshold_for_mode,
    inspect_destination_for_preflight,
    run_file_preflight,
    run_transfer_policy_gates,
)
from ..transfer.connector_registry import run_probe

router = APIRouter(prefix="/preflight", tags=["Preflight"])


class MappingItem(BaseModel):
    source: str
    target: str
    confidence: float = 0.9
    reason: str = ""
    transform: str | None = None
    target_type: str | None = None
    source_type: str | None = None
    requires_review: bool = False
    score_gap: float = 1.0
    user_override: bool = False
    # Must survive Validate → Execute; stripping these caused create_new maps to
    # look like missing columns (or operators remapped onto incompatible DECIMAL id).
    create_new: bool = False
    assignment_strategy: str | None = None
    semantic_role: str | None = None
    # STRUCT/JSON Map choice — write path materializes flatten_top_level_keys.
    struct_policy: str | None = None
    struct_derived: bool = False
    struct_parent: str | None = None
    # Array normalize/hybrid — must survive Validate → Execute.
    structural_class: str | None = None
    child_table_spec: dict[str, Any] | None = None
    # Map Accept risk must survive /preflight/run — stripping these left G3/G4/G9
    # blocking after the operator already acknowledged lossy TEXT→INTEGER (etc.).
    fidelity: str | None = None
    type_narrowing: bool = False
    risk_acknowledged: bool = False
    intentional_omit: bool = False
    # Field Reduction Ledger (G16) evidence for an omitted column. Dropping these
    # on the wire would leave every UI-declared reduction unexplained.
    omit_reason: str | None = None
    omit_reason_text: str | None = None
    archive_reference: str | None = None
    retention_until: str | None = None
    omit_approved_by: str | None = None
    omit_approved_at: str | None = None
    # Migration Risk Contract draft/signed — Execute-approve authority.
    risk_contract: dict[str, Any] | None = None
    # G21 — opt-in independent SUM of this monetary column after write.
    # Must survive Validate → Execute; extra=ignore would drop it.
    control_total: bool | None = None


class PreflightRequest(BaseModel):
    columns: list[str]
    column_types: dict[str, str] = Field(default_factory=dict)
    row_count: int = 0
    mappings: list[MappingItem]
    connector_id: str | None = None
    source_connector_id: str | None = None
    # Ad-hoc / inline source endpoint when no saved connector — uniqueness + orphan probes.
    source_config: dict[str, Any] | None = None
    source_kind: str = "file"
    source_type: str | None = None
    source_table: str | None = None
    source_collection: str | None = None
    dest_kind: str = "database"
    dest_type: str | None = None
    dest_host: str | None = None
    dest_port: int | None = None
    dest_database: str | None = None
    dest_username: str | None = None
    dest_password: str | None = None
    dest_connection_string: str | None = None
    sample_rows: list[dict[str, Any]] | None = None
    estimated_bytes: int = 0
    sync_mode: str = "full_refresh_overwrite"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    stream_contracts: list[dict[str, Any]] = Field(default_factory=list)
    destination_column_types: dict[str, str] = Field(default_factory=dict)
    # Locale for ambiguous day/month dates: 'DMY' (European/Indian/Australian), 'MDY' (US), or ''.
    date_locale: str = ""
    # Locale for ambiguous grouping: 'US' (1,234.56), 'EU' (1.234,56), or ''.
    number_locale: str = ""
    dest_schema: str | None = None
    dest_warehouse: str | None = None
    dest_auth_source: str | None = None
    dest_auth_mode: str | None = None
    dest_auth_role: str | None = None
    dest_api_key: str | None = None
    dest_service_account: str | None = None
    dest_table: str | None = None
    dest_collection: str | None = None
    # Operator attested governance policy allows moving detected PII columns.
    compliance_acknowledged: bool = False
    # Operator acknowledged schema drift under manual_review (keep mappings / ignore new cols).
    schema_drift_acknowledged: bool = False
    # Operator acknowledged destination FK mapping risk (schema coverage only).
    fk_risk_acknowledged: bool = False
    # Module 11 — opt-in full-table population orphan scan (only path to RI proven).
    run_population_orphan_scan: bool = False
    # Optional acknowledgment trail (who / why). Timestamp is stamped server-side.
    acknowledgment_actor: str = ""
    acknowledgment_reason: str = ""
    # Pre-ingestion staging (SQL destinations only) — Validate must fail closed.
    write_via_staging: bool = False
    # Execute-applied sort + cap. Validate names the cap (G17); type-fit stays uncapped.
    priority_column: str = ""
    priority_direction: str = "desc"
    row_limit: int = 0
    # Connector-specific dest settings (Redshift staging_bucket / iam_role, etc.).
    dest_extra: dict[str, Any] | None = None
    # CDC delivery — default at_least_once; exactly_once is opt-in and fail-closed.
    delivery_guarantee: str = "at_least_once"
    # Approved pre-load transform recipe. Execute shapes rows on the read, so the
    # gates must judge the transformed image, not the raw source.
    shape_recipe: dict[str, Any] | None = None
    # Persisted upload from /connectors/upload. Studio posts a preview; when
    # this id is set, Validate scans the stored population (same bytes Execute
    # will stream). Browser-local CSV fallback has no file_id — stay sampled.
    source_file_id: str | None = None


def _schema_default(db_type: str) -> str:
    from services.dialect_profiles import default_schema_for

    return default_schema_for(db_type) or ""


def _default_port(db_type: str) -> int:
    from ..transfer.connector_capabilities import default_port

    return default_port(db_type)


def _probe_inline_destination(body: PreflightRequest) -> tuple[bool, str]:
    """Probe destination using inline connection settings when no saved connector is selected."""
    db_type = (body.dest_type or "mongodb").lower()
    cfg = {
        "host": body.dest_host or "localhost",
        "port": body.dest_port or _default_port(db_type),
        "database": body.dest_database or "",
        "username": body.dest_username or "",
        "password": body.dest_password or "",
        "connection_string": body.dest_connection_string or "",
        "schema": body.dest_schema or _schema_default(db_type),
        "ssl": False,
        "warehouse": body.dest_warehouse or "",
        "type": db_type,
        "auth_source": body.dest_auth_source or "",
        "auth_mode": body.dest_auth_mode or "",
        "auth_role": body.dest_auth_role or "",
        "api_key": body.dest_api_key or "",
        "service_account": body.dest_service_account or "",
    }
    return run_probe(db_type, cfg)


def _probe_saved_connector(connector_id: str) -> tuple[bool, str]:
    """Live connectivity probe for any saved connector type — same as Connectors Test."""
    from services.connector_probe import probe_saved_connector

    ok, msg, _cfg = probe_saved_connector(connector_id)
    return ok, msg


@router.post("/run")
async def run_preflight(body: PreflightRequest):
    """
    Run core G1–G9 preflight gates before a transfer.
    Blocks transfer if any gate fails — no mocked pass when connectors or samples are missing.
    """
    if not body.columns:
        raise HTTPException(status_code=400, detail="No columns provided for preflight")
    if not body.mappings:
        raise HTTPException(status_code=400, detail="No column mappings provided")

    # Refuse an unattributed attestation before any gate consumes it.
    try:
        ack = resolve_acknowledgments(
            compliance=body.compliance_acknowledged,
            schema_drift=body.schema_drift_acknowledged,
            fk_risk=body.fk_risk_acknowledged,
            actor=body.acknowledgment_actor,
            reason=body.acknowledgment_reason,
        )
    except AcknowledgmentRefused as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    destination_connected = False
    dest_error: str | None = None
    dest_meta: dict = {}

    dest_meta = inspect_destination_for_preflight(
        connector_id=body.connector_id,
        dest_type=body.dest_type,
        dest_host=body.dest_host,
        dest_port=body.dest_port,
        dest_database=body.dest_database,
        dest_table=body.dest_table,
        dest_collection=body.dest_collection,
        dest_schema=body.dest_schema,
        dest_username=body.dest_username,
        dest_password=body.dest_password,
        dest_connection_string=body.dest_connection_string,
        dest_warehouse=body.dest_warehouse,
        dest_auth_source=body.dest_auth_source,
        dest_auth_mode=body.dest_auth_mode,
        dest_auth_role=body.dest_auth_role,
        dest_api_key=body.dest_api_key,
        dest_service_account=body.dest_service_account,
        dest_kind=body.dest_kind,
        dest_extra=dict(body.dest_extra or {}),
    )

    if body.dest_kind == "file_export" or dest_meta.get("connected"):
        destination_connected = True
    elif body.connector_id or body.dest_host or body.dest_connection_string:
        destination_connected = False
        dest_error = dest_meta.get("message") or "Destination unreachable"
    else:
        dest_error = "Destination not configured — select a saved connector or enter connection settings"

    source_connected = True
    source_error: str | None = None
    if body.source_connector_id:
        source_connected, msg = _probe_saved_connector(body.source_connector_id)
        if not source_connected:
            source_error = msg

    # Validate must score the mappings against the schema Execute will read.
    # Live source introspection wins over the browser's Map stamps; anything it
    # cannot answer keeps the posted declaration.
    from services.source_schema_authority import (
        live_source_column_types,
        reconcile_source_types,
        restamp_mapping_source_types,
    )

    source_column_types, source_type_drift = reconcile_source_types(
        body.column_types,
        live_source_column_types(
            source_connector_id=body.source_connector_id or "",
            source_table=body.source_table or "",
            source_collection=body.source_collection or "",
        ),
    )
    # The write never sees the raw source when a recipe is approved: shape the
    # image first so a gate cannot block on a value the transform removes.
    try:
        shaped_image = shaped_preflight_image(
            body.shape_recipe,
            columns=body.columns,
            column_types=source_column_types,
            sample_rows=body.sample_rows,
        )
    except ShapePreflightRefused as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    preflight_columns = shaped_image.columns
    source_column_types = shaped_image.column_types
    preflight_sample_rows = shaped_image.sample_rows
    if int(body.row_limit or 0) > 0 or str(body.priority_column or "").strip():
        try:
            from src.transfer.engine import _apply_priority_and_limit
        except ImportError:
            from transfer.engine import _apply_priority_and_limit

        preflight_sample_rows = _apply_priority_and_limit(
            list(preflight_sample_rows or []),
            str(body.priority_column or "").strip(),
            str(body.priority_direction or "desc"),
            int(body.row_limit or 0),
        )

    from services.file_parser import iter_stored_upload_rows

    source_file_id = str(body.source_file_id or "").strip()
    stored_population = iter_stored_upload_rows(source_file_id) if source_file_id else None
    if stored_population is not None and shaped_image.applied:
        stored_population = shaped_population_rows(
            body.shape_recipe,
            stored_population,
            source_columns=preflight_columns,
        )

    preflight_mappings = restamp_mapping_source_types(
        [m.model_dump() for m in body.mappings],
        source_column_types,
    )

    dest_column_types = dest_meta.get("column_types") or {}
    # Prefer live introspect. Never fall back to Studio Map stamps when the
    # table exists but live schema is empty — that greens empties as VARCHAR.
    if not dest_column_types and dest_meta.get("table_exists") is None:
        # Unknown existence: accept Studio types only as a hint for G6 width checks
        # when non-empty; drift path still treats unknown as non-live.
        dest_column_types = body.destination_column_types or {}
    # table_exists True + empty live → keep {} (fail-closed Validate probe)

    from services.primary_key import extract_contract_primary_key_columns

    # Full composite — never truncate to first column (source probe + G9 SSOT).
    # Exact stream match; single-contract Studio may omit matching names.
    _stream_name = body.dest_table or body.dest_collection or ""
    contract_pk_cols = extract_contract_primary_key_columns(
        body.stream_contracts,
        stream_name=_stream_name,
        fallback_first=False,
    )
    if not contract_pk_cols:
        selected = [
            c
            for c in (body.stream_contracts or [])
            if isinstance(c, dict) and c.get("selected", True)
        ]
        if len(selected) == 1:
            contract_pk_cols = extract_contract_primary_key_columns(selected)
    contract_pk = ",".join(contract_pk_cols) if contract_pk_cols else None

    try:
        from services.tracing import get_correlation_id, start_span
    except Exception:
        start_span = None  # type: ignore[assignment]
        def get_correlation_id() -> str:  # type: ignore[misc]
            return ""

    span_attrs = {
        "dataflow.phase": "validate",
        "dataflow.column_count": len(body.columns or []),
        "dataflow.mapping_count": len(body.mappings or []),
        "dataflow.validation_mode": body.validation_mode or "",
        "dataflow.dest_type": (body.dest_type or ""),
        "dataflow.correlation_id": get_correlation_id(),
    }
    with (
        start_span("studio.validate", attributes=span_attrs, kind="internal")
        if start_span is not None
        else __import__("contextlib").nullcontext()
    ):
        result = run_file_preflight(
            columns=preflight_columns,
            column_types=source_column_types,
            row_count=body.row_count,
            mappings=preflight_mappings,
            destination_connected=destination_connected,
            destination_error=dest_error,
            source_connected=source_connected,
            source_error=source_error,
            source_kind=body.source_kind or ("database" if body.source_connector_id else "file"),
            source_format=body.source_type or body.source_kind,
            sync_mode=body.sync_mode,
            sample_rows=preflight_sample_rows,
            estimated_bytes=body.estimated_bytes,
            confidence_threshold=confidence_threshold_for_mode(body.validation_mode),
            destination_column_types=dest_column_types,
            destination_column_nullability=dest_meta.get("column_nullability") or {},
            destination_column_defaults=dest_meta.get("column_defaults") or {},
            destination_identity_columns=dest_meta.get("identity_columns") or [],
            destination_generated_columns=dest_meta.get("generated_columns") or [],
            destination_table_exists=dest_meta.get("table_exists"),
            destination_can_create=dest_meta.get("can_create_table"),
            destination_can_write=dest_meta.get("can_write"),
            privilege_probe=dest_meta.get("privilege_probe"),
            redshift_staging_probe=dest_meta.get("redshift_staging_probe"),
            destination_db_type=(dest_meta.get("db_type") or body.dest_type or "postgresql").lower(),
            source_connector_id=body.source_connector_id or "",
            source_config=body.source_config,
            source_table=(body.source_table or body.source_collection or ""),
            destination_table=(body.dest_table or body.dest_collection or ""),
            source_filename="",
            source_file_id=source_file_id,
            population_rows=stored_population,
            rows_are_population=stored_population is not None,
            shape_recipe=body.shape_recipe,
            schema_policy=body.schema_policy,
            backfill_new_fields=body.backfill_new_fields,
            contract_primary_key=contract_pk,
            destination_pk_columns=dest_meta.get("primary_key_columns") or dest_meta.get("pk_columns"),
            destination_unique_keys=dest_meta.get("unique_keys") or [],
            destination_foreign_keys=dest_meta.get("foreign_keys") or [],
            destination_config=dest_meta.get("_probe_cfg") or None,
            stream_contracts=list(body.stream_contracts or []),
            date_locale=body.date_locale,
            number_locale=body.number_locale,
            compliance_acknowledged=ack.compliance,
            schema_drift_acknowledged=ack.schema_drift,
            fk_risk_acknowledged=ack.fk_risk,
            run_population_orphan_scan=bool(body.run_population_orphan_scan),
            acknowledgment_actor=ack.actor,
            acknowledgment_reason=ack.reason,
        )
    try:
        audit_acknowledgments(
            ack,
            resource="preflight",
            details={
                "source_type": body.source_type,
                "dest_type": body.dest_type,
                "schema_policy": body.schema_policy,
                "validation_mode": body.validation_mode,
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Could not record acknowledgment audit event — acknowledgment not accepted",
        ) from exc
    dest_identity = dest_meta.get("destination_identity")
    if isinstance(dest_identity, dict) and dest_identity.get("database"):
        # Show the effective destination before Execute — the write must never be
        # the first place the operator learns which value won.
        result["destination_identity"] = dest_identity
        if dest_identity.get("conflict") and dest_identity.get("note"):
            bucket = result.setdefault("warnings", [])
            note = str(dest_identity["note"])
            if note not in bucket:
                bucket.append(note)
    if shaped_image.applied:
        # Say which image the gates judged. A verdict on transformed rows that
        # reads as a verdict on the source is how an operator loses the thread.
        result["transform_image"] = {
            "recipe_hash": shaped_image.recipe_hash,
            "columns": shaped_image.columns,
            "sample_rows_in": shaped_image.rows_in,
            "sample_rows_out": shaped_image.rows_out,
            "sample_rows_removed": shaped_image.rows_removed,
            "sample_rows_diverted": shaped_image.rows_diverted,
            "retyped_columns": shaped_image.retyped_columns,
        }
        note = shaped_image.note()
        bucket = result.setdefault("warnings", [])
        if note and note not in bucket:
            bucket.append(note)
    if source_type_drift:
        # The operator's Map rows disagreed with the live source. Gates already
        # ran on the live truth — say so instead of silently rescoring.
        result["source_schema_drift"] = source_type_drift
        bucket = result.setdefault("warnings", [])
        for d in source_type_drift:
            note = (
                f"Source type re-read from live connector: {d['column']} "
                f"{d['declared']} → {d['live']} (Map stamp was stale)"
            )
            if note not in bucket:
                bucket.append(note)
    # Advisory catalog honesty from destination introspect (warn-only).
    for w in dest_meta.get("warnings") or dest_meta.get("schema_warnings") or []:
        note = str(w).strip()
        if not note:
            continue
        bucket = result.setdefault("warnings", [])
        if note not in bucket:
            bucket.append(note)
    gated = apply_policy_gates(
        result,
        run_transfer_policy_gates(
            sync_mode=body.sync_mode,
            schema_policy=body.schema_policy,
            validation_mode=body.validation_mode,
            stream_contracts=body.stream_contracts,
            backfill_new_fields=body.backfill_new_fields,
            source_columns=preflight_columns,
            dest_type=body.dest_type
            or (dest_meta.get("db_type") if isinstance(dest_meta, dict) else None),
            source_type=body.source_type,
            source_kind=body.source_kind or ("database" if body.source_connector_id else "file"),
            write_via_staging=bool(body.write_via_staging),
            priority_column=str(body.priority_column or ""),
            priority_direction=str(body.priority_direction or "desc"),
            row_limit=int(body.row_limit or 0),
            source_read_mode=str(
                ((body.source_config or {}).get("source_read_mode")
                 or ((body.source_config or {}).get("extra") or {}).get("source_read_mode")
                 or "")
            ),
            # A stored watermark belongs to the column it was measured on;
            # Validate refuses a repointed cursor rather than letting Run
            # apply one column's value to another.
            read_scope=resolve_read_scope(
                sync_mode=body.sync_mode,
                stream_contracts=body.stream_contracts,
                source_format=body.source_type or "",
                source_config=body.source_config,
                source_table=(body.source_table or body.source_collection or ""),
                destination_db_type=str(
                    body.dest_type
                    or (dest_meta.get("db_type") if isinstance(dest_meta, dict) else "")
                    or ""
                ),
                destination_config=dest_meta.get("_probe_cfg") or None,
                destination_table=(body.dest_table or body.dest_collection or ""),
            ),
            delivery_guarantee=body.delivery_guarantee or "at_least_once",
            allow_append_only=bool((body.dest_extra or {}).get("allow_append_only")),
        ),
        validation_mode=body.validation_mode,
    )
    from services.preflight_run_store import save_preflight_run

    dest_label = (
        body.dest_table
        or body.dest_collection
        or body.dest_database
        or body.dest_type
        or body.dest_kind
        or "destination"
    )
    return save_preflight_run(
        gated,
        source_label=body.source_type or body.source_kind or "source",
        dest_label=str(dest_label),
        validation_mode=body.validation_mode,
        route={
            "source_kind": body.source_kind,
            "source_type": body.source_type,
            "source_connector_id": body.source_connector_id,
            "dest_kind": body.dest_kind,
            "dest_type": body.dest_type,
            "dest_connector_id": body.connector_id,
            "dest_table": body.dest_table,
            "dest_collection": body.dest_collection,
            "row_count": body.row_count,
        },
    )


@router.get("/runs")
async def list_preflight_runs(limit: int = 20):
    """List recent validation runs (IDs Datawrap Pilot / Jobs can reference)."""
    from services.preflight_run_store import list_preflight_runs as _list

    return {"runs": _list(limit=limit), "count": min(limit, 100)}


@router.get("/runs/{run_id}")
async def get_preflight_run(run_id: str):
    """Fetch a stored validation run by ID for Pilot triage and audit."""
    from services.preflight_run_store import get_preflight_run as _get

    record = _get(run_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Preflight run '{run_id}' not found")
    return record


class ExplainRequest(BaseModel):
    """A preflight result to explain (as returned by POST /preflight/run).

    Prefer ``run_id`` plus a slimed payload. The full Validate result is not
    required and must not be posted after a 1M-row scan (nginx client_temp warn).
    """

    preflight: dict[str, Any] = Field(default_factory=dict, description="Slimed preflight result dict")
    run_id: str | None = None
    dest_type: str | None = None
    validation_mode: str = "strict"
    use_llm: bool = Field(True, description="Reuse Datawrap Pilot LLM for a natural-language narrative when available")


@router.post("/explain")
async def explain_preflight(body: ExplainRequest):
    """AI-assisted 'explain & suggest fix' for a preflight/validation result.

    Returns a structured, actionable explanation — what failed, which
    column/row/value/type, why, and concrete fixes plus machine-readable
    ``suggested_actions``. Works deterministically offline; reuses the Data
    Pilot LLM only to add a friendlier narrative when a provider is configured.
    """
    from services.validation_assistant import explain_validation, slim_preflight_for_explain

    payload = body.preflight if isinstance(body.preflight, dict) else {}
    run_id = str(body.run_id or payload.get("run_id") or "").strip()
    if run_id and not payload.get("blockers") and "passed" not in payload:
        from services.preflight_run_store import get_preflight_run as _get

        record = _get(run_id)
        if isinstance(record, dict):
            payload = record.get("result") or record.get("preflight") or record
    try:
        return explain_validation(
            slim_preflight_for_explain(payload),
            dest_kind=(body.dest_type or "").lower(),
            validation_mode=body.validation_mode,
            use_llm=body.use_llm,
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e))


class SchemaDriftRequest(BaseModel):
    old_schema: dict[str, Any] = Field(default_factory=dict)
    new_schema: dict[str, Any] = Field(default_factory=dict)
    dest_db: str = ""
    schema_policy: str = "manual_review"


@router.post("/schema-drift")
async def classify_schema_drift(body: SchemaDriftRequest):
    """Classify schema evolution and stamp compatibility + pause/propagate/review."""
    from services.schema_drift import classify_schema_evolution_report

    try:
        return classify_schema_evolution_report(
            body.old_schema,
            body.new_schema,
            dest_db=body.dest_db or "",
            schema_policy=body.schema_policy or "manual_review",
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e))


class CellPreviewRequest(BaseModel):
    headers: list[str] = Field(default_factory=list)
    sample_rows: list[list[Any]] = Field(default_factory=list)
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    column_types: dict[str, str] = Field(default_factory=dict)
    sample_size: int = Field(25, ge=1, le=200)
    date_locale: str = ""
    number_locale: str = ""
    shape_recipe: dict[str, Any] | None = None


@router.post("/preview-cells")
async def preview_quarantine_cells(body: CellPreviewRequest):
    """Cell-level quarantine/coerce preview before transfer run.

    The recipe travels with the request for the same reason it travels with
    ``/preflight/run``: Execute transforms on the read, so a cell preview that
    scans raw values reports findings on values no writer will ever bind —
    ``Invalid integer: '22.43'`` on a column the approved recipe rounds to 22.
    """
    from services.transform_engine import preview_quarantine_cells as _preview

    try:
        from services.transform_engine import (
            reset_active_date_locale,
            reset_active_number_locale,
            set_active_date_locale,
            set_active_number_locale,
        )

        locale_token = set_active_date_locale(body.date_locale)
        number_token = set_active_number_locale(body.number_locale)
        try:
            rows = [[("" if c is None else str(c)) for c in row] for row in body.sample_rows]
            headers = list(body.headers)
            column_types = dict(body.column_types)
            try:
                image = shaped_preflight_image(
                    body.shape_recipe,
                    columns=headers,
                    column_types=column_types,
                    sample_rows=[dict(zip(headers, row)) for row in rows],
                )
            except ShapePreflightRefused as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            if image.applied:
                headers = image.columns
                column_types = image.column_types
                rows = [
                    [("" if row.get(h) is None else str(row.get(h))) for h in headers]
                    for row in (image.sample_rows or [])
                ]
            result = _preview(
                headers=headers,
                sample_rows=rows,
                mappings=body.mappings,
                column_types=column_types,
                sample_size=body.sample_size,
            )
            if image.applied:
                result["transform_image"] = {
                    "recipe_hash": image.recipe_hash,
                    "sample_rows_in": image.rows_in,
                    "sample_rows_out": image.rows_out,
                    "sample_rows_removed": image.rows_removed,
                    "retyped_columns": image.retyped_columns,
                }
            return result
        finally:
            reset_active_number_locale(number_token)
            reset_active_date_locale(locale_token)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e))
