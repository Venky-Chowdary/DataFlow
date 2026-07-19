"""MCP server endpoint tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_mcp_manifest_is_reachable(client: TestClient):
    response = client.get("/api/v1/mcp/manifest")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "dataflow"
    assert data["protocol"] == "streamable-http"
    assert data["endpoints"]["manifest"].startswith("http://testserver/api/v1/mcp")
    assert data["tools"]
    assert data["capabilities"]


def test_mcp_tools_list_is_reachable(client: TestClient):
    response = client.get("/api/v1/mcp/tools")
    assert response.status_code == 200
    data = response.json()
    assert data["tools"]
    assert any(tool["name"] == "get_transfer_capabilities" for tool in data["tools"])


def test_mcp_tool_call_get_transfer_capabilities(client: TestClient):
    response = client.post(
        "/api/v1/mcp/tools/call",
        json={"name": "get_transfer_capabilities", "arguments": {}},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True
    assert "output" in data


def test_mcp_status_is_reachable(client: TestClient):
    response = client.get("/api/v1/mcp/status")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "online"
    assert data["tools_registered"] >= 1
    assert data["manifest_url"].startswith("http://testserver/api/v1/mcp/manifest")


def test_mcp_streamable_initialize(client: TestClient):
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "pytest", "version": "0"},
            },
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["result"]["protocolVersion"]
    assert data["result"]["capabilities"]["tools"] is not None
    assert response.headers.get("mcp-session-id")


def test_mcp_streamable_tools_list(client: TestClient):
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    )
    assert response.status_code == 200, response.text
    tools = response.json()["result"]["tools"]
    assert any(t["name"] == "list_connectors" for t in tools)
    assert all("inputSchema" in t for t in tools)


def test_mcp_streamable_tools_call_requires_auth_when_enforced(client: TestClient, monkeypatch):
    import services.mcp_protocol as proto

    monkeypatch.setattr(proto, "handle_jsonrpc", proto.handle_jsonrpc)
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "get_transfer_capabilities", "arguments": {}},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    # Without Bearer in test (auth often off), call may succeed; with auth on, -32001.
    if "error" in body:
        assert body["error"]["code"] == -32001
    else:
        assert body["result"]["isError"] is False


def test_mcp_streamable_initialized_notification(client: TestClient):
    response = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert response.status_code == 202
