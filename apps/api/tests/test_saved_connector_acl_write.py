"""A viewer-level ACL grant lets a member read a saved connector, not change it."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from services import resource_acl, team_store

OWNER = "owner@example.com"
EDITOR = "editor@example.com"


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    ws = team_store.create_workspace(name="Globex", created_by=OWNER)
    team_store.add_workspace_member(workspace_id=ws.id, email=EDITOR, role="editor", added_by=OWNER)
    return ws.id


def _request(email: str) -> Request:
    req = Request({"type": "http", "method": "PUT", "path": "/", "headers": []})
    req.state.user_email = email
    req.state.user = {"email": email, "role": "member"}
    return req


def test_viewer_acl_grant_blocks_writes_but_not_reads(workspace):
    module = importlib.import_module("src.routers.saved_connectors_router")
    conn = SimpleNamespace(id="ACL_CONN", workspace_id=workspace)
    resource_acl.upsert_grant(
        tenant_id=workspace,
        resource_type="connector",
        resource_id=conn.id,
        principal=EDITOR,
        role="viewer",
        created_by=OWNER,
    )
    req = _request(EDITOR)
    assert module._can_access_connector(req, conn) is True
    assert module._can_access_connector(req, conn, write=True) is False
    with pytest.raises(HTTPException) as exc:
        module._writable_connector_or_404(req, conn)
    assert exc.value.status_code == 404


def test_workspace_editor_without_an_acl_still_writes(workspace):
    module = importlib.import_module("src.routers.saved_connectors_router")
    conn = SimpleNamespace(id="OPEN_CONN", workspace_id=workspace)
    assert module._writable_connector_or_404(_request(EDITOR), conn) is conn
