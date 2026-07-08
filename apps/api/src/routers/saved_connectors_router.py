"""Saved connector profiles — file-backed CRUD used by preflight and Transfer Studio."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.connector_store import (  # noqa: E402
    create_connector,
    delete_connector,
    get_connector,
    list_connectors,
    mark_tested,
    mask_connector,
    update_connector,
)

router = APIRouter(prefix="/connectors/saved", tags=["Saved Connectors"])


class ConnectorSaveDTO(BaseModel):
    name: str
    type: str
    role: str = "both"
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = "public"
    connection_string: str = ""
    ssl: bool = False
    warehouse: str = ""


def _to_ui(c) -> dict[str, Any]:
    d = mask_connector(c)
    d["id"] = d["id"]
    d["status"] = "configured" if c.last_test_ok else ("error" if c.last_tested_at and not c.last_test_ok else "configured")
    return d


@router.get("")
def get_saved_connectors(role: str | None = None):
    return {"connectors": [_to_ui(c) for c in list_connectors(role)]}


@router.get("/{connector_id}")
def get_saved_connector(connector_id: str):
    conn = get_connector(connector_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connector not found")
    return mask_connector(conn)


@router.post("")
def save_connector(body: ConnectorSaveDTO):
    conn = create_connector(body.model_dump())
    return _to_ui(conn)


@router.put("/{connector_id}")
def update_saved_connector(connector_id: str, body: ConnectorSaveDTO):
    data = body.model_dump()
    existing = get_connector(connector_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Connector not found")
    if data.get("password") in ("", "****"):
        data["password"] = existing.password
    updated = update_connector(connector_id, data)
    if not updated:
        raise HTTPException(status_code=404, detail="Connector not found")
    return _to_ui(updated)


@router.delete("/{connector_id}")
def remove_saved_connector(connector_id: str):
    if not delete_connector(connector_id):
        raise HTTPException(status_code=404, detail="Connector not found")
    return {"ok": True}


@router.post("/{connector_id}/test")
def test_saved_connector(connector_id: str):
    conn = get_connector(connector_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connector not found")

    db_type = (conn.type or "").lower()
    ok, message = False, "Unsupported type"

    if db_type == "mongodb":
        try:
            from pymongo import MongoClient

            conn_str = conn.connection_string or f"mongodb://{conn.host}:{conn.port or 27017}/"
            if conn.username and conn.password:
                conn_str = f"mongodb://{conn.username}:{conn.password}@{conn.host}:{conn.port or 27017}/"
            client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            ok, message = True, "MongoDB reachable"
        except Exception as exc:
            ok, message = False, str(exc)
    else:
        probes = {
            "postgresql": ("connectors.postgresql", "test_postgresql"),
            "mysql": ("connectors.mysql", "test_mysql"),
            "snowflake": ("connectors.snowflake", "test_snowflake"),
            "bigquery": ("connectors.bigquery", "test_bigquery"),
            "dynamodb": ("connectors.dynamodb", "test_dynamodb"),
            "redis": ("connectors.redis_kv", "test_redis"),
            "s3": ("connectors.s3", "test_s3"),
            "elasticsearch": ("connectors.elasticsearch", "test_elasticsearch"),
        }
        if db_type in probes:
            mod_name, fn_name = probes[db_type]
            mod = importlib.import_module(mod_name)
            probe_fn = getattr(mod, fn_name)
            kwargs = dict(
                host=conn.host or "",
                port=int(conn.port or 5432),
                database=conn.database or "",
                username=conn.username or "",
                password=conn.password or "",
                schema=conn.schema or "public",
                connection_string=conn.connection_string or "",
                ssl=conn.ssl,
            )
            if db_type in ("snowflake", "bigquery", "dynamodb", "redis", "s3", "elasticsearch"):
                kwargs["warehouse"] = conn.warehouse or ""
            result = probe_fn(**kwargs)
            ok = result.ok
            message = result.message if result.ok else (result.error or "Connection failed")

    mark_tested(connector_id, ok)
    return {"success": ok, "message": message}
