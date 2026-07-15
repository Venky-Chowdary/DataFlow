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
    assert data["name"] == "datatransfer"
    assert data["protocol"] == "rest-bridge"
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
