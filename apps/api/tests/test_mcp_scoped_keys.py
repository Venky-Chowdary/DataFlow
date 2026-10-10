"""Scoped workspace API keys remain scoped across MCP and Pilot dispatch."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.rbac import Permission
from src.ai.copilot.tool_permissions import (
    allowed_tools,
    bind_current_context,
    caller_role,
    can_confirm_kind,
    denial_message,
    is_permission_denial,
    is_tool_allowed,
)


@pytest.fixture
def scoped_client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)

    from services import auth_service, integrations_store, team_store

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "api_keys.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_database", lambda: None)

    from src.main import app

    scoped = integrations_store.create_api_key(
        "job reader",
        "scoped@example.test",
        role="editor",
        scopes=["job.read"],
    )
    unscoped = integrations_store.create_api_key(
        "full editor",
        "editor@example.test",
        role="editor",
    )
    with TestClient(app) as client:
        yield client, scoped["key"], unscoped["key"]


def _stream_call(client, token: str, name: str, arguments: dict):
    response = client.post(
        "/api/v1/mcp",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    if "error" in body:
        return str(body["error"].get("message") or body["error"])
    result = body.get("result") or {}
    return str((result.get("content") or [{}])[0].get("text") or result)


def _rest_call(client, token: str, name: str, arguments: dict):
    response = client.post(
        "/api/v1/mcp/tools/call",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": name, "arguments": arguments},
    )
    body = response.json()
    if response.status_code >= 400:
        detail = body.get("detail", body)
        if isinstance(detail, dict):
            return str(detail.get("error") or detail.get("detail") or detail)
        return str(detail)
    return str(body.get("output") or body)


def test_scoped_editor_key_is_limited_on_both_mcp_endpoints(scoped_client):
    client, token, _unscoped = scoped_client
    for call in (_stream_call, _rest_call):
        for name, arguments in (
            ("get_job", {"job_id": "missing"}),
            ("list_jobs", {}),
        ):
            result = call(client, token, name, arguments)
            assert not is_permission_denial(result), (name, result)

        for name, arguments in (
            ("start_transfer", {}),
            ("confirm_action", {"ack_id": "unavailable-ack"}),
        ):
            result = call(client, token, name, arguments)
            assert is_permission_denial(result), (name, result)


def test_unscoped_editor_keeps_role_permissions(scoped_client):
    client, _scoped, unscoped = scoped_client
    stream_error = _stream_call(client, unscoped, "start_transfer", {})
    rest_error = _rest_call(client, unscoped, "start_transfer", {})
    assert not is_permission_denial(stream_error)
    assert not is_permission_denial(rest_error)


def test_permission_binding_narrows_tools_and_confirmation_and_reaches_workers():
    from src.ai.copilot.tool_permissions import current_caller_permissions

    granted = {Permission.JOB_READ}
    with caller_role("editor", permissions=granted):
        assert current_caller_permissions() == frozenset(granted)
        assert "get_job" in allowed_tools("editor")
        assert "start_transfer" not in allowed_tools("editor")
        assert is_tool_allowed("editor", "get_job")
        assert not is_tool_allowed("editor", "start_transfer")
        assert not can_confirm_kind("editor", "start_transfer")
        assert "view jobs" in denial_message("editor", "start_transfer")
        with ThreadPoolExecutor(max_workers=1) as pool:
            scoped_result = pool.submit(
                bind_current_context(
                    lambda: (
                        current_caller_permissions(),
                        is_tool_allowed("editor", "start_transfer"),
                    )
                )
            ).result()
        assert scoped_result == (frozenset(granted), False)
    assert current_caller_permissions() is None


def test_ack_kind_outside_the_bound_scope_cannot_be_confirmed():
    with caller_role("editor", permissions={Permission.WORKSPACE_READ}):
        assert can_confirm_kind("editor", "create_connector") is False


def test_empty_role_keeps_open_deployment_behavior():
    with caller_role("", permissions={Permission.JOB_READ}):
        assert is_tool_allowed("", "start_transfer") is True
        assert can_confirm_kind("", "start_transfer") is True
