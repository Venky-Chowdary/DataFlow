"""Preflight API — 8-gate validation before transfer."""

from __future__ import annotations

import importlib
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..services.preflight_service import (
    apply_policy_gates,
    confidence_threshold_for_mode,
    run_file_preflight,
    run_transfer_policy_gates,
)

router = APIRouter(prefix="/preflight", tags=["Preflight"])


class MappingItem(BaseModel):
    source: str
    target: str
    confidence: float = 0.9
    reason: str = ""


class PreflightRequest(BaseModel):
    columns: list[str]
    column_types: dict[str, str] = Field(default_factory=dict)
    row_count: int = 0
    mappings: list[MappingItem]
    connector_id: Optional[str] = None
    source_connector_id: Optional[str] = None
    sample_rows: Optional[list[dict[str, Any]]] = None
    estimated_bytes: int = 0
    sync_mode: str = "full_refresh_overwrite"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    stream_contracts: list[dict[str, Any]] = Field(default_factory=list)


def _probe_saved_connector(connector_id: str) -> tuple[bool, str]:
    """Live connectivity probe for any saved connector type."""
    import sys
    from pathlib import Path

    api_root = Path(__file__).resolve().parents[2]
    if str(api_root) not in sys.path:
        sys.path.insert(0, str(api_root))

    from services.connector_store import get_connector

    conn = get_connector(connector_id)
    if not conn:
        return False, f"Connector '{connector_id}' not found"

    db_type = (conn.type or "").lower()

    if db_type == "mongodb":
        try:
            from pymongo import MongoClient

            conn_str = conn.connection_string or f"mongodb://{conn.host}:{conn.port or 27017}/"
            client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            return True, "MongoDB reachable"
        except Exception as exc:
            return False, str(exc)

    probes = {
        "postgresql": ("connectors.postgresql", "test_postgresql"),
        "mysql": ("connectors.mysql", "test_mysql"),
        "snowflake": ("connectors.snowflake", "test_snowflake"),
        "bigquery": ("connectors.bigquery", "test_bigquery"),
    }
    if db_type not in probes:
        return False, f"No connectivity probe for connector type '{db_type}'"

    mod_name, fn_name = probes[db_type]
    mod = importlib.import_module(mod_name)
    probe_fn = getattr(mod, fn_name)
    result = probe_fn(
        host=conn.host or "",
        port=int(conn.port or (443 if db_type in ("snowflake", "bigquery") else 5432)),
        database=conn.database or "",
        username=conn.username or "",
        password=conn.password or "",
        schema=conn.schema or ("PUBLIC" if db_type == "snowflake" else "dataflow" if db_type == "bigquery" else "public"),
        connection_string=conn.connection_string or "",
        ssl=conn.ssl,
        warehouse=conn.warehouse or "",
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

    if body.connector_id:
        destination_connected, msg = _probe_saved_connector(body.connector_id)
        if not destination_connected:
            dest_error = msg
    else:
        dest_error = "Destination connector not selected — configure one in Connectors"

    source_connected = True
    source_error: str | None = None
    if body.source_connector_id:
        source_connected, msg = _probe_saved_connector(body.source_connector_id)
        if not source_connected:
            source_error = msg

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
    )
