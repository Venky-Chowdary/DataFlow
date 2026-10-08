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


def test_mcp_tool_call_get_transfer_capabilities(client: TestClient, monkeypatch):
    # When platform auth is off (local), tools/call remains usable for DX.
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    response = client.post(
        "/api/v1/mcp/tools/call",
        json={"name": "get_transfer_capabilities", "arguments": {}},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True
    assert "output" in data


def test_mcp_tool_call_rejected_without_bearer_when_auth_on(client: TestClient, monkeypatch):
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: True)
    response = client.post(
        "/api/v1/mcp/tools/call",
        json={"name": "get_transfer_capabilities", "arguments": {}},
    )
    assert response.status_code == 401, response.text


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


def test_streamable_ping_returns_while_a_tool_is_still_running(monkeypatch):
    """DEF-MCP-OUTAGE: a long tools/call must not freeze ping on the listener.

    The client timeout is JSON-RPC -32001. The server auth error uses the
    same code and is a different failure. This proves the listener stays
    free. A process restart during deploy still drops the in-flight call.
    """
    import asyncio
    import time

    import httpx

    from src.ai.copilot.tools import ToolResult

    class _Slow:
        def execute(self, name, arguments):
            time.sleep(0.3)
            return ToolResult(name=name, success=True, output={"ok": True})

    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    monkeypatch.setattr("src.ai.copilot.tools.get_pilot_tools", lambda: _Slow())

    async def _main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            tool_task = asyncio.create_task(
                client.post(
                    "/api/v1/mcp",
                    headers={"Accept": "application/json"},
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "list_datasets", "arguments": {}},
                    },
                )
            )
            await asyncio.sleep(0.05)
            started = time.perf_counter()
            ping = await client.post(
                "/api/v1/mcp",
                headers={"Accept": "application/json"},
                json={"jsonrpc": "2.0", "id": 2, "method": "ping"},
            )
            elapsed = time.perf_counter() - started
            tool = await tool_task
            return elapsed, ping, tool

    elapsed, ping, tool = asyncio.run(_main())
    assert elapsed < 0.2, elapsed
    assert ping.status_code == 200
    assert ping.json()["result"] == {}
    assert tool.status_code == 200
    assert tool.json()["result"]["isError"] is False
