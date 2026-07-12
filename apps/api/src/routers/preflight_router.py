"""Preflight API — 8-gate validation before transfer."""

from __future__ import annotations

import importlib
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
    dest_table: Optional[str] = None
    dest_collection: Optional[str] = None


def _schema_default(db_type: str) -> str:
    return "PUBLIC" if db_type == "snowflake" else "public"


def _default_port(db_type: str) -> int:
    from ..transfer.connector_capabilities import default_port

    return default_port(db_type)


def _probe_inline_destination(body: PreflightRequest) -> tuple[bool, str]:
    """Probe destination using inline connection settings when no saved connector is selected."""
    from ..transfer.connector_registry import run_probe

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
    }
    return run_probe(db_type, cfg)


def _probe_saved_connector(connector_id: str) -> tuple[bool, str]:
    """Live connectivity probe for any saved connector type."""
    from ..transfer.adapters import _lookup_saved_connector, probe_mongodb

    conn = _lookup_saved_connector(connector_id)
    if not conn:
        return False, f"Connector '{connector_id}' not found"

    db_type = (conn.get("type") or "").lower()

    if db_type == "mongodb":
        return probe_mongodb(conn)

    probes = {
        "postgresql": ("connectors.postgresql", "test_postgresql"),
        "mysql": ("connectors.mysql", "test_mysql"),
        "snowflake": ("connectors.snowflake", "test_snowflake"),
        "bigquery": ("connectors.bigquery", "test_bigquery"),
        "redshift": ("connectors.redshift", "test_redshift"),
        "dynamodb": ("connectors.dynamodb", "test_dynamodb"),
        "s3": ("connectors.s3", "test_s3"),
        "gcs": ("connectors.gcs", "test_gcs"),
        "redis": ("connectors.redis_kv", "test_redis"),
        "elasticsearch": ("connectors.elasticsearch", "test_elasticsearch"),
    }
    if db_type not in probes:
        return False, f"No connectivity probe for connector type '{db_type}'"

    mod_name, fn_name = probes[db_type]
    mod = importlib.import_module(mod_name)
    probe_fn = getattr(mod, fn_name)
    result = probe_fn(
        host=conn.get("host") or "",
        port=int(conn.get("port") or _default_port(db_type)),
        database=conn.get("database") or "",
        username=conn.get("username") or "",
        password=conn.get("password") or "",
        schema=conn.get("schema") or ("PUBLIC" if db_type == "snowflake" else "dataflow" if db_type == "bigquery" else "public"),
        connection_string=conn.get("connection_string") or "",
        ssl=conn.get("ssl", False),
        warehouse=conn.get("warehouse") or "",
    )
    if result.ok:
        return True, result.message or "Connected"
    return False, result.error or "Connection failed"


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
        source_kind="database" if body.source_connector_id else "file",
        sample_rows=body.sample_rows,
        estimated_bytes=body.estimated_bytes,
        confidence_threshold=confidence_threshold_for_mode(body.validation_mode),
        destination_column_types=dest_column_types,
        destination_table_exists=dest_meta.get("table_exists"),
        destination_can_create=dest_meta.get("can_create_table"),
        destination_db_type=(dest_meta.get("db_type") or body.dest_type or "postgresql").lower(),
    )
    return apply_policy_gates(
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
