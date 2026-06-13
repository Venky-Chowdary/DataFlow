"""Preflight API — 8-gate validation before transfer."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Any
from ..services.preflight_service import run_file_preflight
from ..services.mongodb_service import get_mongodb_service

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
    sample_rows: Optional[list[dict[str, Any]]] = None
    estimated_bytes: int = 0


@router.post("/run")
async def run_preflight(body: PreflightRequest):
    """
    Run all 8 preflight gates before a transfer.
    Blocks transfer if any gate fails (source, destination, schema, mapping, dry-run, DDL, capacity).
    """
    destination_connected = True
    if body.connector_id:
        mongo = get_mongodb_service()
        connector = mongo.get_connector(body.connector_id)
        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")
        try:
            from pymongo import MongoClient
            host, port = connector["host"], connector["port"]
            conn_str = connector.get("connection_string") or f"mongodb://{host}:{port}/"
            client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
        except Exception as e:
            destination_connected = False

    result = run_file_preflight(
        columns=body.columns,
        column_types=body.column_types,
        row_count=body.row_count,
        mappings=[m.model_dump() for m in body.mappings],
        destination_connected=destination_connected,
        sample_rows=body.sample_rows,
        estimated_bytes=body.estimated_bytes,
    )
    return result
