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
    auth_mode: str = ""
    auth_role: str = ""
    api_key: str = ""
    service_account: str = ""
    auth_source: str = ""


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


def _sentinel_secret(value: str) -> bool:
    """Return True if the secret could not be decrypted."""
    return "[encrypted-secret-unavailable]" in value or "[decryption-failed]" in value


@router.post("/{connector_id}/test")
def test_saved_connector(connector_id: str):
    conn = get_connector(connector_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connector not found")

    if _sentinel_secret(conn.password or "") or _sentinel_secret(conn.connection_string or ""):
        mark_tested(connector_id, False)
        return {
            "success": False,
            "message": (
                "Saved credentials cannot be decrypted. Install `cryptography` "
                "(`pip install cryptography`) and ensure the same DATAFLOW_SECRETS_KEY "
                "is set, then re-enter the password or connection string."
            ),
        }

    from ..transfer.connector_registry import run_probe

    cfg = {
        "host": conn.host or "",
        "port": int(conn.port or 0),
        "database": conn.database or "",
        "username": conn.username or "",
        "password": conn.password or "",
        "schema": conn.schema or "",
        "connection_string": conn.connection_string or "",
        "warehouse": conn.warehouse or "",
        "ssl": conn.ssl,
        "auth_mode": conn.auth_mode or "",
        "auth_role": conn.auth_role or "",
        "role": conn.auth_role or "",
        "api_key": conn.api_key or "",
        "service_account": conn.service_account or "",
    }
    ok, message = run_probe(conn.type or "", cfg)

    mark_tested(connector_id, ok)
    return {"success": ok, "message": message}
