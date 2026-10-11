"""Regression coverage for the native MCP Streamable HTTP boundary."""

from __future__ import annotations

import logging
import re
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def mcp_api(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))

    from services import auth_service, connector_store, integrations_store, team_store
    from services import mcp_invocation_log

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "api_keys.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_database", lambda: None)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(mcp_invocation_log, "STORE_PATH", tmp_path / "mcp_invocations.jsonl")
    monkeypatch.setattr(mcp_invocation_log, "append_audit_event", lambda **_kwargs: None)

    from src.main import app

    editor = integrations_store.create_api_key(
        "MCP hardening editor",
        "editor@example.test",
        role="editor",
    )
    viewer = integrations_store.create_api_key(
        "MCP hardening viewer",
        "viewer@example.test",
        role="viewer",
    )
    with TestClient(app) as client:
        yield client, editor["key"], viewer["key"]


def _rpc(
    client: TestClient,
    method: str,
    *,
    token: str | None = None,
    params: dict | None = None,
    headers: dict[str, str] | None = None,
):
    request_headers = {"Accept": "application/json"}
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    if headers:
        request_headers.update(headers)
    return client.post(
        "/api/v1/mcp",
        headers=request_headers,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
    )


def _tools_with_result(*, success: bool = True, output=None, error: str | None = None):
    def execute(name, _arguments):
        return SimpleNamespace(
            name=name,
            success=success,
            output={"ok": True} if output is None else output,
            error=error,
        )

    return SimpleNamespace(execute=execute)


def test_mcp_call_context_is_frozen_and_defaults_to_streamable_metadata(monkeypatch):
    from services.mcp_protocol import McpCallContext, handle_jsonrpc
    from services import mcp_invocation_log

    context = McpCallContext(actor="actor", client="client", correlation_id=None)
    with pytest.raises(FrozenInstanceError):
        context.actor = "changed"

    captured = []
    monkeypatch.setattr(
        mcp_invocation_log,
        "log_mcp_invocation",
        lambda **kwargs: captured.append(kwargs) or {"id": "test"},
    )
    monkeypatch.setattr(
        "services.mcp_rate_limit.check_mcp_rate_limit",
        lambda _actor: {"allowed": True},
    )
    monkeypatch.setattr("src.ai.copilot.tools.get_pilot_tools", lambda: _tools_with_result())
    result = handle_jsonrpc(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_connectors", "arguments": {}},
        },
        authenticated=True,
    )

    assert result["result"]["isError"] is False
    assert captured[0]["client"] == "mcp-streamable"
    assert captured[0]["actor"] == "mcp-streamable"
    assert captured[0]["correlation_id"] is None


def test_streamable_rate_limit_blocks_tools_call_but_not_protocol_methods(mcp_api, monkeypatch):
    client, editor_token, _viewer_token = mcp_api
    checks = []
    tools = _tools_with_result()
    monkeypatch.setattr(
        "services.mcp_rate_limit.check_mcp_rate_limit",
        lambda actor: checks.append(actor)
        or {"allowed": False, "retry_after_sec": 7},
    )
    monkeypatch.setattr("src.ai.copilot.tools.get_pilot_tools", lambda: tools)

    denied = _rpc(
        client,
        "tools/call",
        token=editor_token,
        params={"name": "list_connectors", "arguments": {}},
    )
    assert denied.status_code == 200
    assert denied.json()["error"]["code"] == -32029
    assert denied.json()["error"]["message"] == "MCP rate limit exceeded"
    assert denied.json()["error"]["data"]["retry_after_sec"] == 7

    assert _rpc(client, "ping").json()["result"] == {}
    assert _rpc(client, "initialize").status_code == 200
    assert _rpc(client, "tools/list").status_code == 200
    assert checks == ["editor@example.test"]


def test_streamable_invocation_log_has_request_attribution(mcp_api, monkeypatch):
    client, editor_token, _viewer_token = mcp_api
    from services import mcp_invocation_log

    captured = []
    monkeypatch.setattr(
        mcp_invocation_log,
        "log_mcp_invocation",
        lambda **kwargs: captured.append(kwargs) or {"id": "test"},
    )
    monkeypatch.setattr("src.ai.copilot.tools.get_pilot_tools", lambda: _tools_with_result())

    response = _rpc(
        client,
        "tools/call",
        token=editor_token,
        params={"name": "list_connectors", "arguments": {}},
        headers={
            "X-MCP-Client": "qa-bot",
            "X-Correlation-ID": "mcp-correlation-123",
        },
    )

    assert response.json()["result"]["isError"] is False
    assert len(captured) == 1
    assert captured[0]["client"] == "qa-bot"
    assert captured[0]["actor"] == "editor@example.test"
    assert captured[0]["correlation_id"] == "mcp-correlation-123"


def test_streamable_tool_exception_redacts_client_and_invocation_log(
    mcp_api, monkeypatch, caplog
):
    client, editor_token, _viewer_token = mcp_api
    from services import mcp_invocation_log

    captured = []
    monkeypatch.setattr(
        mcp_invocation_log,
        "log_mcp_invocation",
        lambda **kwargs: captured.append(kwargs) or {"id": "test"},
    )

    def fail(_name, _arguments):
        raise RuntimeError(
            "connect failed: postgresql://u:s3cret@h/db?access_token=query-secret"
        )

    monkeypatch.setattr(
        "src.ai.copilot.tools.get_pilot_tools",
        lambda: SimpleNamespace(execute=fail),
    )
    with caplog.at_level(logging.ERROR, logger="services.mcp_protocol"):
        response = _rpc(
            client,
            "tools/call",
            token=editor_token,
            params={"name": "list_connectors", "arguments": {}},
            headers={
                "X-MCP-Client": "qa-bot",
                "X-Correlation-ID": "mcp-exception-123",
            },
        )

    text = response.json()["result"]["content"][0]["text"]
    assert text.startswith("Tool error: RuntimeError:")
    assert "s3cret" not in text
    assert "query-secret" not in text
    assert "postgresql://u:****@h/db" in text
    assert "MCP tool list_connectors raised" in caplog.text
    assert len(captured) == 1
    assert "s3cret" not in captured[0]["error"]
    assert "query-secret" not in captured[0]["error"]
    assert captured[0]["client"] == "qa-bot"
    assert captured[0]["actor"] == "editor@example.test"
    assert captured[0]["correlation_id"] == "mcp-exception-123"


def test_streamable_tool_result_error_is_redacted(mcp_api, monkeypatch):
    client, editor_token, _viewer_token = mcp_api
    from services import mcp_invocation_log

    captured = []
    monkeypatch.setattr(
        mcp_invocation_log,
        "log_mcp_invocation",
        lambda **kwargs: captured.append(kwargs) or {"id": "test"},
    )
    monkeypatch.setattr(
        "src.ai.copilot.tools.get_pilot_tools",
        lambda: _tools_with_result(
            success=False,
            error="failed: postgresql://u:s3cret@h/db?token=query-secret",
        ),
    )

    response = _rpc(
        client,
        "tools/call",
        token=editor_token,
        params={"name": "list_connectors", "arguments": {}},
        headers={"X-Correlation-ID": "mcp-result-123"},
    )
    text = response.json()["result"]["content"][0]["text"]
    assert response.json()["result"]["isError"] is True
    assert "s3cret" not in text
    assert "query-secret" not in text
    assert "s3cret" not in captured[0]["error"]
    assert "query-secret" not in captured[0]["error"]
    assert captured[0]["actor"] == "editor@example.test"
    assert captured[0]["correlation_id"] == "mcp-result-123"


def test_rest_tool_exception_redacts_detail_and_invocation_log(mcp_api, monkeypatch):
    client, editor_token, _viewer_token = mcp_api
    from services import mcp_invocation_log

    captured = []
    monkeypatch.setattr(
        mcp_invocation_log,
        "log_mcp_invocation",
        lambda **kwargs: captured.append(kwargs) or {"id": "test"},
    )

    def fail(_name, _arguments):
        raise RuntimeError("connect failed: postgresql://u:s3cret@h/db")

    monkeypatch.setattr(
        "src.ai.copilot.tools.get_pilot_tools",
        lambda: SimpleNamespace(execute=fail),
    )
    response = client.post(
        "/api/v1/mcp/tools/call",
        headers={
            "Authorization": f"Bearer {editor_token}",
            "X-MCP-Client": "qa-bot",
            "X-Correlation-ID": "rest-mcp-123",
        },
        json={"name": "list_connectors", "arguments": {}},
    )

    assert response.status_code == 500
    assert "s3cret" not in response.text
    assert "postgresql://u:****@h/db" in response.text
    assert "s3cret" not in captured[0]["error"]
    assert captured[0]["actor"] == "editor@example.test"
    assert captured[0]["correlation_id"] == "rest-mcp-123"


def test_parse_error_is_generic_and_logged_as_warning(mcp_api, caplog):
    client, _editor_token, _viewer_token = mcp_api

    with caplog.at_level(logging.WARNING, logger="src.routers.mcp_router"):
        response = client.post(
            "/api/v1/mcp",
            headers={"Content-Type": "application/json"},
            content=b'{"jsonrpc":',
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == -32700
    assert response.json()["error"]["message"] == "Parse error"
    assert any("MCP JSON-RPC parse error" in record.message for record in caplog.records)


def _call_http_method(client: TestClient, method: str, headers: dict[str, str]):
    if method == "GET":
        return client.get("/api/v1/mcp", headers=headers)
    if method == "DELETE":
        return client.delete("/api/v1/mcp", headers=headers)
    return _rpc(client, "ping", headers=headers)


def _cors_middleware(client: TestClient):
    from services.cors_policy import TenantAwareCORSMiddleware
    from src.main import app

    stack = app.middleware_stack
    while stack is not None:
        if isinstance(stack, TenantAwareCORSMiddleware):
            return stack
        stack = getattr(stack, "app", None)
    raise AssertionError("TenantAwareCORSMiddleware is not installed")


@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_disallowed_origin_is_rejected_for_all_streamable_methods(mcp_api, method):
    client, _editor_token, _viewer_token = mcp_api
    response = _call_http_method(
        client,
        method,
        {"Origin": "https://untrusted.example"},
    )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == -32600
    assert response.json()["error"]["message"] == "Origin not allowed"


@pytest.mark.parametrize(
    ("origin", "policy"),
    [
        ("https://mcp-allowed.example", "origins"),
        ("https://mcp-regex.example", "regex"),
    ],
)
def test_configured_origin_passes_using_existing_cors_policy(
    mcp_api, monkeypatch, origin, policy
):
    client, _editor_token, _viewer_token = mcp_api
    cors = _cors_middleware(client)
    if policy == "origins":
        monkeypatch.setattr(cors, "allow_origins", [*cors.allow_origins, origin])
    else:
        monkeypatch.setattr(
            cors,
            "allow_origin_regex",
            re.compile(r"https://mcp-regex\.example"),
        )

    response = _call_http_method(client, "POST", {"Origin": origin})
    assert response.status_code == 200
    assert response.json()["result"] == {}


@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_streamable_requests_without_origin_are_allowed(mcp_api, method):
    client, _editor_token, _viewer_token = mcp_api
    response = _call_http_method(client, method, {})
    assert response.status_code == {"GET": 200, "POST": 200, "DELETE": 204}[method]


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("2025-06-18", "2025-06-18"),
        ("2024-11-05", "2024-11-05"),
        ("unsupported-version", "2025-06-18"),
    ],
)
def test_initialize_negotiates_supported_protocol_versions(
    mcp_api, requested, expected
):
    client, _editor_token, _viewer_token = mcp_api
    response = _rpc(
        client,
        "initialize",
        params={"protocolVersion": requested, "capabilities": {}, "clientInfo": {}},
    )
    assert response.status_code == 200
    assert response.json()["result"]["protocolVersion"] == expected


def test_tools_list_includes_effect_annotations(mcp_api):
    client, _editor_token, _viewer_token = mcp_api
    from src.ai.copilot.tool_permissions import MUTATE, PLAN, READ, tool_requirement

    response = _rpc(client, "tools/list")
    assert response.status_code == 200
    descriptors = {tool["name"]: tool for tool in response.json()["result"]["tools"]}
    for name in descriptors:
        effect = tool_requirement(name)[1]
        read_only = effect in (READ, PLAN) and name != "confirm_action"
        annotations = descriptors[name]["annotations"]
        assert annotations == {
            "readOnlyHint": read_only,
            "destructiveHint": not read_only,
        }
        if effect == MUTATE:
            assert annotations["destructiveHint"] is True
    assert descriptors["get_job"]["annotations"]["readOnlyHint"] is True
    assert descriptors["confirm_action"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": True,
    }


def test_auth_on_streamable_door_keeps_tools_public_and_enforces_tool_roles(mcp_api):
    client, editor_token, viewer_token = mcp_api
    expected_tools = {
        "start_transfer",
        "create_connector",
        "confirm_action",
        "get_job",
        "plan_transfer",
        "start_dataset_transfer",
        "test_connector",
        "get_preflight_run",
        "cancel_job",
        "replay_quarantine",
        "list_jobs",
        "sample_connector_object",
        "run_query",
        "resume_job",
        "create_schedule",
        "get_schedule",
        "list_schedules",
        "delete_schedule",
        "list_connector_objects",
        "get_transfer_capabilities",
        "introspect_connector_schema",
        "diff_schemas",
        "prepare_cdc_source",
        "delete_connector",
        "update_connector",
    }
    editor_headers = {"Authorization": f"Bearer {editor_token}"}
    initialized = client.post(
        "/api/v1/mcp",
        headers=editor_headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-06-18"},
        },
    )
    assert initialized.status_code == 200

    listed = client.post(
        "/api/v1/mcp",
        headers=editor_headers,
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    )
    assert listed.status_code == 200
    listed_names = {tool["name"] for tool in listed.json()["result"]["tools"]}
    assert expected_tools <= listed_names

    anonymous_list = _rpc(client, "tools/list")
    assert {tool["name"] for tool in anonymous_list.json()["result"]["tools"]} == listed_names

    editor_call = _rpc(
        client,
        "tools/call",
        token=editor_token,
        params={"name": "list_connectors", "arguments": {}},
    )
    assert editor_call.status_code == 200
    assert editor_call.json()["result"]["isError"] is False

    viewer_call = _rpc(
        client,
        "tools/call",
        token=viewer_token,
        params={"name": "start_transfer", "arguments": {}},
    )
    assert viewer_call.status_code == 200
    assert viewer_call.json()["result"]["isError"] is True
    assert viewer_call.json()["result"]["content"][0]["text"].startswith("Your role")

    unauthenticated_call = _rpc(
        client,
        "tools/call",
        params={"name": "list_connectors", "arguments": {}},
    )
    assert unauthenticated_call.status_code == 401
    assert unauthenticated_call.json()["error"]["code"] == -32001


def test_free_text_and_connector_store_mask_uri_and_query_secrets():
    from services.connector_store import _mask_conn_str
    from services.secret_config import mask_secrets_in_text

    raw = (
        "failed for postgresql://u:s3cret@db.example.test/warehouse"
        "?access_token=query-secret&client_secret=client-secret"
    )
    masked = mask_secrets_in_text(raw)
    assert "s3cret" not in masked
    assert "query-secret" not in masked
    assert "client-secret" not in masked
    assert "postgresql://u:****@db.example.test/warehouse" in masked
    query = parse_qs(urlsplit(masked.split(" for ", 1)[1]).query)
    assert query["access_token"] == ["***"]
    assert query["client_secret"] == ["***"]
    assert _mask_conn_str(raw) == masked


@pytest.mark.parametrize("door", ["native", "rest"])
def test_mcp_invocation_log_persists_actor_for_authenticated_door(
    mcp_api, monkeypatch, door
):
    client, editor_token, _viewer_token = mcp_api
    monkeypatch.setattr(
        "src.ai.copilot.tools.get_pilot_tools", lambda: _tools_with_result()
    )
    correlation_id = f"actor-attribution-{door}"
    headers = {
        "Authorization": f"Bearer {editor_token}",
        "X-Correlation-ID": correlation_id,
    }

    if door == "native":
        response = _rpc(
            client,
            "tools/call",
            token=editor_token,
            params={"name": "list_connectors", "arguments": {}},
            headers={"X-Correlation-ID": correlation_id},
        )
        assert response.status_code == 200
        assert response.json()["result"]["isError"] is False
    else:
        response = client.post(
            "/api/v1/mcp/tools/call",
            headers=headers,
            json={"name": "list_connectors", "arguments": {}},
        )
        assert response.status_code == 200
        assert response.json()["success"] is True

    logs = client.get(
        "/api/v1/mcp/logs",
        headers={"Authorization": f"Bearer {editor_token}"},
    )
    assert logs.status_code == 200
    row = next(
        row
        for row in logs.json()["logs"]
        if row.get("correlation_id") == correlation_id
    )
    assert row.get("actor") == "editor@example.test"


def test_mcp_invocation_log_has_actor_for_unauthenticated_native_call(
    mcp_api, monkeypatch
):
    client, editor_token, _viewer_token = mcp_api
    monkeypatch.setattr(
        "src.ai.copilot.tools.get_pilot_tools", lambda: _tools_with_result()
    )
    correlation_id = "actor-attribution-unauthenticated"
    response = _rpc(
        client,
        "tools/call",
        params={"name": "list_connectors", "arguments": {}},
        headers={
            "X-MCP-Client": "anonymous-qa-bot",
            "X-Correlation-ID": correlation_id,
        },
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == -32001
    logs = client.get(
        "/api/v1/mcp/logs",
        headers={"Authorization": f"Bearer {editor_token}"},
    )
    assert logs.status_code == 200
    row = next(
        row
        for row in logs.json()["logs"]
        if row.get("correlation_id") == correlation_id
    )
    assert row.get("actor")
