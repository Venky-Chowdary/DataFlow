"""Saved connector profiles — file-backed CRUD used by preflight and Transfer Studio.

All endpoints accept an optional ``X-Workspace-Id`` header.  When provided, the
requesting user or API key must be a member of that workspace (owner, editor, or
viewer for read; owner or editor for write).  Connectors created with a
workspace id are only visible inside that workspace (plus the global workspace
id ``""`` for shared templates).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

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
from services.team_store import can_read_workspace, can_write_workspace  # noqa: E402

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
    private_key: str = ""
    endpoint_url: str = ""
    path_style: bool = False
    auth_source: str = ""
    # Optional: persist an in-form Test result so the list matches on first save.
    last_test_ok: bool | None = None


def _actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def _resolve_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    """Return the workspace id after verifying the actor can read it."""
    workspace_id = (x_workspace_id or "").strip()
    if workspace_id and not can_read_workspace(workspace_id, _actor_email(request)):
        raise HTTPException(status_code=403, detail="Access to workspace denied")
    return workspace_id


def _require_write_workspace(
    request: Request,
    x_workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> str:
    """Return the workspace id after verifying the actor can write to it."""
    workspace_id = (x_workspace_id or "").strip()
    if workspace_id and not can_write_workspace(workspace_id, _actor_email(request)):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")
    return workspace_id


def _to_ui(c) -> dict[str, Any]:
    d = mask_connector(c)
    d["id"] = d["id"]
    if c.last_test_ok is True:
        d["status"] = "configured"
    elif c.last_test_ok is False and c.last_tested_at:
        d["status"] = "error"
    else:
        d["status"] = "configured"
    return d


def _can_access_connector(request: Request, conn: Any) -> bool:
    """True if the actor may see or mutate this connector."""
    if not conn.workspace_id:
        return True
    return can_read_workspace(conn.workspace_id, _actor_email(request))


@router.get("")
def get_saved_connectors(
    role: str | None = None,
    request: Request = None,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _resolve_workspace(request, workspace_id)
    return {"connectors": [_to_ui(c) for c in list_connectors(role, workspace_id=workspace_id)]}


@router.get("/{connector_id}")
def get_saved_connector(
    connector_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _resolve_workspace(request, workspace_id)
    conn = get_connector(connector_id, workspace_id=workspace_id)
    if not conn or not _can_access_connector(request, conn):
        raise HTTPException(status_code=404, detail="Connector not found")
    return mask_connector(conn)


def _persist_form_test_status(connector_id: str, last_test_ok: bool | None) -> None:
    """Apply in-form Test result without re-probing credentials."""
    if last_test_ok is True:
        mark_tested(connector_id, True)
    elif last_test_ok is False:
        mark_tested(connector_id, False)


@router.post("")
def save_connector(
    body: ConnectorSaveDTO,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _require_write_workspace(request, workspace_id)
    data = body.model_dump()
    form_test = data.pop("last_test_ok", None)
    data["workspace_id"] = workspace_id
    conn = create_connector(data)
    _persist_form_test_status(conn.id, form_test)
    refreshed = get_connector(conn.id, workspace_id=workspace_id) or conn
    return _to_ui(refreshed)


@router.put("/{connector_id}")
def update_saved_connector(
    connector_id: str,
    body: ConnectorSaveDTO,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _require_write_workspace(request, workspace_id)
    data = body.model_dump()
    form_test = data.pop("last_test_ok", None)
    existing = get_connector(connector_id, workspace_id=workspace_id)
    if not existing or not _can_access_connector(request, existing):
        raise HTTPException(status_code=404, detail="Connector not found")
    if data.get("password") in ("", "****"):
        data["password"] = existing.password
    if data.get("private_key") in ("", "****"):
        data["private_key"] = existing.private_key
    data["workspace_id"] = existing.workspace_id or workspace_id
    updated = update_connector(connector_id, data, workspace_id=workspace_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Connector not found")
    _persist_form_test_status(connector_id, form_test)
    refreshed = get_connector(connector_id, workspace_id=workspace_id) or updated
    return _to_ui(refreshed)


@router.delete("/{connector_id}")
def remove_saved_connector(
    connector_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _require_write_workspace(request, workspace_id)
    conn = get_connector(connector_id, workspace_id=workspace_id)
    if not conn or not _can_access_connector(request, conn):
        raise HTTPException(status_code=404, detail="Connector not found")
    if not delete_connector(connector_id, workspace_id=workspace_id):
        raise HTTPException(status_code=404, detail="Connector not found")
    return {"ok": True}


@router.post("/{connector_id}/test")
def test_saved_connector(
    connector_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _resolve_workspace(request, workspace_id)
    conn = get_connector(connector_id, workspace_id=workspace_id)
    if not conn or not _can_access_connector(request, conn):
        raise HTTPException(status_code=404, detail="Connector not found")

    from services.connector_probe import probe_saved_connector

    ok, message, cfg = probe_saved_connector(connector_id, workspace_id=workspace_id)

    # Persist any auto-resolved auth fields (e.g., MongoDB authSource) so the
    # saved connector works end-to-end without re-entering the connection string.
    if ok and cfg.get("auth_source") and cfg.get("auth_source") != (conn.auth_source or ""):
        update_connector(connector_id, {"auth_source": cfg.get("auth_source", "")}, workspace_id=workspace_id)

    mark_tested(connector_id, ok)
    return {"success": ok, "message": message, "auth_source": cfg.get("auth_source", "")}
