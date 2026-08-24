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
from pydantic import BaseModel, ConfigDict, Field

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.connector_store import (
    create_connector,
    delete_connector,
    get_connector,
    list_connectors,
    mark_tested,
    mask_connector,
    update_connector,
)
from services.team_store import can_read_workspace, can_write_workspace

router = APIRouter(prefix="/connectors/saved", tags=["Saved Connectors"])


class ConnectorSaveDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    type: str
    role: str = "both"
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    db_schema: str = Field(default="public", alias="schema")
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


_Secret_fields = ("password", "api_key", "service_account", "private_key")


def _is_masked_value(value: Any) -> bool:
    """Detect values that the UI sends back as placeholders for unchanged secrets."""
    if value is None:
        return True
    if isinstance(value, str):
        v = value.strip()
        if not v or v == "****":
            return True
        if "****" in v or "<redacted>" in v.lower():
            return True
    return False


def _preserve_masked_secrets(data: dict[str, Any], existing: Any) -> dict[str, Any]:
    """Copy saved secrets back into an update payload that only carried placeholders.

    This is the server-side backstop for the common UI pattern: the connector list
    is masked, the form is re-submitted with the masked values, and without this
    guard the saved connection string / password / API key would be overwritten by
    ``****`` and destination introspection would fail with authentication errors.
    """
    out = dict(data)
    for field in _Secret_fields:
        if _is_masked_value(out.get(field)) and getattr(existing, field, None) is not None:
            out[field] = getattr(existing, field)
    if _is_masked_value(out.get("connection_string")) and getattr(existing, "connection_string", None):
        out["connection_string"] = existing.connection_string
    return out


def _can_access_connector(request: Request, conn: Any) -> bool:
    """True if the actor may see or mutate this connector (workspace + optional ACL)."""
    actor = _actor_email(request)
    if conn.workspace_id and not can_read_workspace(conn.workspace_id, actor):
        return False
    try:
        from services.resource_acl import assert_resource_acl

        user = getattr(request.state, "user", None) or {}
        is_admin = str(user.get("role") or "").lower() == "admin"
        assert_resource_acl(
            tenant_id=conn.workspace_id or "",
            resource_type="connector",
            resource_id=str(getattr(conn, "id", "") or ""),
            principal=actor,
            min_role="viewer",
            is_admin=is_admin,
        )
    except PermissionError:
        return False
    except Exception:
        # Fail closed: ACL infrastructure errors must not open restricted resources.
        return False
    return True


@router.get("")
def get_saved_connectors(
    role: str | None = None,
    request: Request = None,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    workspace_id = _resolve_workspace(request, workspace_id)
    return {
        "connectors": [
            _to_ui(c)
            for c in list_connectors(role, workspace_id=workspace_id)
            if _can_access_connector(request, c)
        ]
    }


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
    data = body.model_dump(by_alias=True)
    form_test = data.pop("last_test_ok", None)
    data["workspace_id"] = workspace_id
    # If the form re-submits a masked copy of an existing connector, preserve secrets
    # before create_connector deletes the old record and re-creates it.
    existing = None
    for c in list_connectors(role=data.get("role"), workspace_id=workspace_id):
        if c.name == data.get("name") and c.type == data.get("type"):
            existing = c
            break
    if existing is not None:
        data = _preserve_masked_secrets(data, existing)
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
    data = body.model_dump(by_alias=True)
    form_test = data.pop("last_test_ok", None)
    existing = get_connector(connector_id, workspace_id=workspace_id)
    if not existing or not _can_access_connector(request, existing):
        raise HTTPException(status_code=404, detail="Connector not found")
    data = _preserve_masked_secrets(data, existing)
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


class RotateSecretsBody(BaseModel):
    """Replace one or more connector secrets and invalidate pooled engines."""

    password: str | None = None
    api_key: str | None = None
    connection_string: str | None = None
    private_key: str | None = None
    service_account: str | None = None


@router.post("/{connector_id}/rotate-secrets")
def rotate_saved_connector_secrets(
    connector_id: str,
    body: RotateSecretsBody,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Rotate connector credentials with audit + pool invalidation.

    Masked placeholders are rejected — callers must send the new secret value.
    Does not claim short-lived DB token issuance (separate enterprise feature).
    """
    from datetime import datetime, timezone

    from services.audit_log import actor_from_request, append_audit_event

    workspace_id = _require_write_workspace(request, workspace_id)
    existing = get_connector(connector_id, workspace_id=workspace_id)
    if not existing or not _can_access_connector(request, existing):
        raise HTTPException(status_code=404, detail="Connector not found")

    payload = body.model_dump(exclude_none=True)
    if not payload:
        raise HTTPException(status_code=400, detail="Provide at least one new secret field")
    for key, value in payload.items():
        if _is_masked_value(value):
            raise HTTPException(
                status_code=400,
                detail=f"{key} looks masked; send the new secret value, not ****",
            )

    rotated_at = datetime.now(timezone.utc).isoformat()
    payload["credentials_rotated_at"] = rotated_at
    # Invalidate pooled engines keyed by the *pre-rotation* credentials first —
    # cache keys include password/connection_string, so post-update invalidate misses.
    pool_invalidated = False
    try:
        from services.engine_pool import invalidate

        pool_invalidated = bool(invalidate(existing.to_dict()))
    except Exception:
        pool_invalidated = False

    updated = update_connector(connector_id, payload, workspace_id=workspace_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        from services.engine_pool import invalidate

        pool_invalidated = bool(invalidate(updated.to_dict())) or pool_invalidated
    except Exception:
        pass

    try:
        append_audit_event(
            action="connector.rotate_secrets",
            resource=f"connector:{connector_id}",
            actor=actor_from_request(request),
            level="info",
            details={
                "fields": sorted(k for k in payload if k != "credentials_rotated_at"),
                "pool_invalidated": pool_invalidated,
                "credentials_rotated_at": rotated_at,
            },
        )
    except Exception:
        pass

    ui = _to_ui(updated)
    ui["credentials_rotated_at"] = rotated_at
    ui["pool_invalidated"] = pool_invalidated
    ui["honesty"] = (
        "Secrets rotated and re-encrypted at rest. Pooled SQL engines for this "
        "config were invalidated when present. Not a short-lived token workflow."
    )
    return ui


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
    # Echo last_test_ok so the UI can patch the list immediately — never leave a
    # sticky "Test failed" badge after a green probe (Test-all vs individual drift).
    return {
        "success": ok,
        "message": message,
        "last_test_ok": ok,
        "auth_source": cfg.get("auth_source", ""),
    }
