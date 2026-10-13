"""Behavioral coverage for organization-wide MCP policy enforcement."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def policy_client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")

    from services import integrations_store, mcp_invocation_log, team_store
    from src.main import app
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_database", lambda: None)
    monkeypatch.setattr(mcp_invocation_log, "STORE_PATH", tmp_path / "mcp_invocations.jsonl")
    audit_events = []
    monkeypatch.setattr(
        "services.audit_log.append_audit_event",
        lambda **event: audit_events.append(event),
    )
    monkeypatch.setattr(
        mcp_invocation_log,
        "append_audit_event",
        lambda **event: audit_events.append(event),
    )

    viewer = integrations_store.create_api_key(
        "viewer",
        "viewer@example.test",
        role="viewer",
    )
    scoped = integrations_store.create_api_key(
        "scoped editor",
        "scoped@example.test",
        role="editor",
        scopes=["job.read"],
    )
    admin = integrations_store.create_api_key(
        "admin",
        "admin@example.test",
        role="admin",
    )

    with TestClient(app) as client:
        yield {
            "client": client,
            "tokens": {
                "viewer": viewer["key"],
                "scoped": scoped["key"],
                "admin": admin["key"],
            },
            "integrations_store": integrations_store,
            "audit_events": audit_events,
        }


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _native_call(client: TestClient, token: str | None, name: str):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return client.post(
        "/api/v1/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": {}},
        },
    )


def _rest_call(client: TestClient, token: str, name: str):
    return client.post(
        "/api/v1/mcp/tools/call",
        headers=_headers(token),
        json={"name": name, "arguments": {}},
    )


def _patch_tool_executor(monkeypatch):
    from src.ai.copilot import tools

    calls: list[tuple[str, dict]] = []

    def execute(name, arguments):
        calls.append((name, arguments))
        return SimpleNamespace(
            success=True,
            name=name,
            output="tool executed",
            error=None,
        )

    monkeypatch.setattr(tools, "get_pilot_tools", lambda: SimpleNamespace(execute=execute))
    return calls


def test_policy_routes_require_workspace_manage(policy_client):
    client = policy_client["client"]
    tokens = policy_client["tokens"]

    for role in ("viewer", "scoped"):
        headers = _headers(tokens[role])
        assert client.get("/api/v1/mcp/policy", headers=headers).status_code == 403
        assert (
            client.put(
                "/api/v1/mcp/policy",
                headers=headers,
                json={"enabled": True, "allowed_tools": None},
            ).status_code
            == 403
        )

    admin_headers = _headers(tokens["admin"])
    assert client.get("/api/v1/mcp/policy", headers=admin_headers).status_code == 200
    assert (
        client.put(
            "/api/v1/mcp/policy",
            headers=admin_headers,
            json={"enabled": True, "allowed_tools": None},
        ).status_code
        == 200
    )


def test_policy_put_rejects_unknown_tools_and_audits_actor_and_values(policy_client):
    client = policy_client["client"]
    admin_headers = _headers(policy_client["tokens"]["admin"])
    bad = client.put(
        "/api/v1/mcp/policy",
        headers=admin_headers,
        json={"enabled": True, "allowed_tools": ["not_a_real_tool"]},
    )
    assert bad.status_code == 422

    updated = client.put(
        "/api/v1/mcp/policy",
        headers=admin_headers,
        json={"enabled": False, "allowed_tools": ["list_connectors"]},
    )
    assert updated.status_code == 200
    event = next(
        event
        for event in policy_client["audit_events"]
        if event.get("action") == "mcp.policy.updated"
    )
    assert event["actor"] == "admin@example.test"
    assert event["details"]["old"] == {"enabled": True, "allowed_tools": None}
    assert event["details"]["new"] == updated.json()


def test_policy_persists_after_store_reload(policy_client):
    store = policy_client["integrations_store"]
    expected = {"enabled": True, "allowed_tools": ["list_connectors", "list_jobs"]}

    assert store.set_mcp_policy(expected) == expected
    assert store._load_raw()["mcp_policy"] == expected
    assert store.get_mcp_policy() == expected


def test_disabled_policy_denies_both_doors_without_executing_and_logs(policy_client, monkeypatch):
    from services import mcp_invocation_log, mcp_rate_limit

    calls = _patch_tool_executor(monkeypatch)
    policy_client["integrations_store"].set_mcp_policy(
        {"enabled": False, "allowed_tools": None}
    )
    monkeypatch.setattr(
        mcp_rate_limit,
        "check_mcp_rate_limit",
        lambda _actor: pytest.fail("policy denial must precede rate limiting"),
    )

    client = policy_client["client"]
    token = policy_client["tokens"]["admin"]
    native = _native_call(client, token, "list_connectors")
    rest = _rest_call(client, token, "list_connectors")

    assert native.status_code == 200
    assert native.json()["error"]["code"] == -32003
    assert rest.status_code == 403
    assert calls == []
    rows = mcp_invocation_log.list_mcp_invocations()
    assert len(rows) == 2
    assert all(row["error_kind"] == "policy_denied" for row in rows)


def test_allow_list_denies_both_doors_without_executing(policy_client, monkeypatch):
    calls = _patch_tool_executor(monkeypatch)
    policy_client["integrations_store"].set_mcp_policy(
        {"enabled": True, "allowed_tools": ["get_job"]}
    )
    client = policy_client["client"]
    token = policy_client["tokens"]["admin"]

    native = _native_call(client, token, "list_connectors")
    rest = _rest_call(client, token, "list_connectors")

    assert native.json()["error"]["code"] == -32003
    assert rest.status_code == 403
    assert calls == []


def test_confirm_action_is_not_implicitly_allowed_with_start_transfer(policy_client, monkeypatch):
    calls = _patch_tool_executor(monkeypatch)
    policy_client["integrations_store"].set_mcp_policy(
        {"enabled": True, "allowed_tools": ["start_transfer"]}
    )
    client = policy_client["client"]
    token = policy_client["tokens"]["admin"]

    native = _native_call(client, token, "confirm_action")
    rest = _rest_call(client, token, "confirm_action")

    assert native.json()["error"]["code"] == -32003
    assert rest.status_code == 403
    assert calls == []


def test_all_tool_lists_apply_the_allow_list(policy_client):
    store = policy_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "allowed_tools": ["list_connectors"]})
    client = policy_client["client"]
    headers = _headers(policy_client["tokens"]["admin"])

    native = client.post(
        "/api/v1/mcp",
        headers=headers,
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    )
    rest = client.get("/api/v1/mcp/tools", headers=headers)
    manifest = client.get("/api/v1/mcp/manifest", headers=headers)

    assert [tool["name"] for tool in native.json()["result"]["tools"]] == ["list_connectors"]
    assert [tool["name"] for tool in rest.json()["tools"]] == ["list_connectors"]
    assert [tool["name"] for tool in manifest.json()["tools"]] == ["list_connectors"]


def test_unauthenticated_call_is_rejected_before_disabled_policy(policy_client):
    policy_client["integrations_store"].set_mcp_policy(
        {"enabled": False, "allowed_tools": None}
    )
    response = _native_call(policy_client["client"], None, "list_connectors")
    assert response.json()["error"]["code"] == -32001
