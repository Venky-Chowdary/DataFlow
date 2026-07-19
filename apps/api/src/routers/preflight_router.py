"""Preflight API — 8-gate validation before transfer."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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
    requires_review: bool = False
    score_gap: float = 1.0
    user_override: bool = False


class PreflightRequest(BaseModel):
    columns: list[str]
    column_types: dict[str, str] = Field(default_factory=dict)
    row_count: int = 0
    mappings: list[MappingItem]
    connector_id: Optional[str] = None
    source_connector_id: Optional[str] = None
    source_kind: str = "file"
    source_type: Optional[str] = None
    dest_kind: str = "database"
    dest_type: Optional[str] = None
    dest_host: Optional[str] = None
    dest_port: Optional[int] = None
    dest_database: Optional[str] = None
    dest_username: Optional[str] = None
    dest_password: Optional[str] = None
    dest_connection_string: Optional[str] = None
    sample_rows: Optional[list[dict[str, Any]]] = None
    estimated_bytes: int = 0
    sync_mode: str = "full_refresh_overwrite"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    stream_contracts: list[dict[str, Any]] = Field(default_factory=list)
    destination_column_types: dict[str, str] = Field(default_factory=dict)
    dest_schema: Optional[str] = None
    dest_warehouse: Optional[str] = None
    dest_auth_source: Optional[str] = None
    dest_auth_mode: Optional[str] = None
    dest_auth_role: Optional[str] = None
    dest_api_key: Optional[str] = None
    dest_service_account: Optional[str] = None
    dest_table: Optional[str] = None
    dest_collection: Optional[str] = None


def _schema_default(db_type: str) -> str:
    return "PUBLIC" if db_type == "snowflake" else "public"


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
    """Live connectivity probe for any saved connector type."""
    from ..transfer.adapters import _lookup_saved_connector

    conn = _lookup_saved_connector(connector_id)
    if not conn:
        return False, f"Connector '{connector_id}' not found"

    db_type = (conn.get("type") or "").lower()
    return run_probe(db_type, conn)


@router.post("/run")
async def run_preflight(body: PreflightRequest):
    """
    Run all 8 preflight gates before a transfer.
    Blocks transfer if any gate fails — no mocked pass when connectors or samples are missing.
    """
    if not body.columns:
        raise HTTPException(status_code=400, detail="No columns provided for preflight")
    if not body.mappings:
        raise HTTPException(status_code=400, detail="No column mappings provided")

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
    )

    if body.dest_kind == "file_export":
        destination_connected = True
    elif dest_meta.get("connected"):
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

    dest_column_types = body.destination_column_types or dest_meta.get("column_types") or {}

    result = run_file_preflight(
        columns=body.columns,
        column_types=body.column_types,
        row_count=body.row_count,
        mappings=[m.model_dump() for m in body.mappings],
        destination_connected=destination_connected,
        destination_error=dest_error,
        source_connected=source_connected,
        source_error=source_error,
        source_kind=body.source_kind or ("database" if body.source_connector_id else "file"),
        source_format=body.source_type or body.source_kind,
        sync_mode=body.sync_mode,
        sample_rows=body.sample_rows,
        estimated_bytes=body.estimated_bytes,
        confidence_threshold=confidence_threshold_for_mode(body.validation_mode),
        destination_column_types=dest_column_types,
        destination_table_exists=dest_meta.get("table_exists"),
        destination_can_create=dest_meta.get("can_create_table"),
        destination_db_type=(dest_meta.get("db_type") or body.dest_type or "postgresql").lower(),
    )
    gated = apply_policy_gates(
        result,
        run_transfer_policy_gates(
            sync_mode=body.sync_mode,
            schema_policy=body.schema_policy,
            validation_mode=body.validation_mode,
            stream_contracts=body.stream_contracts,
            backfill_new_fields=body.backfill_new_fields,
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
    """List recent validation runs (IDs Data Pilot / Jobs can reference)."""
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
    """A preflight result to explain (as returned by POST /preflight/run)."""

    preflight: dict[str, Any] = Field(..., description="Full preflight result dict")
    dest_type: Optional[str] = None
    validation_mode: str = "strict"
    use_llm: bool = Field(True, description="Reuse Data Pilot LLM for a natural-language narrative when available")


@router.post("/explain")
async def explain_preflight(body: ExplainRequest):
    """AI-assisted 'explain & suggest fix' for a preflight/validation result.

    Returns a structured, actionable explanation — what failed, which
    column/row/value/type, why, and concrete fixes plus machine-readable
    ``suggested_actions``. Works deterministically offline; reuses the Data
    Pilot LLM only to add a friendlier narrative when a provider is configured.
    """
    from services.validation_assistant import explain_validation

    try:
        return explain_validation(
            body.preflight,
            dest_kind=(body.dest_type or "").lower(),
            validation_mode=body.validation_mode,
            use_llm=body.use_llm,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SchemaDriftRequest(BaseModel):
    old_schema: dict[str, Any] = Field(default_factory=dict)
    new_schema: dict[str, Any] = Field(default_factory=dict)


@router.post("/schema-drift")
async def classify_schema_drift(body: SchemaDriftRequest):
    """Classify schema evolution as additive vs breaking (approve/reject UX)."""
    from services.schema_drift import classify_schema_change

    try:
        return classify_schema_change(body.old_schema, body.new_schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CellPreviewRequest(BaseModel):
    headers: list[str] = Field(default_factory=list)
    sample_rows: list[list[Any]] = Field(default_factory=list)
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    column_types: dict[str, str] = Field(default_factory=dict)
    sample_size: int = Field(25, ge=1, le=200)


@router.post("/preview-cells")
async def preview_quarantine_cells(body: CellPreviewRequest):
    """Cell-level quarantine/coerce preview before transfer run."""
    from services.transform_engine import preview_quarantine_cells as _preview

    try:
        rows = [[("" if c is None else str(c)) for c in row] for row in body.sample_rows]
        return _preview(
            headers=body.headers,
            sample_rows=rows,
            mappings=body.mappings,
            column_types=body.column_types,
            sample_size=body.sample_size,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
