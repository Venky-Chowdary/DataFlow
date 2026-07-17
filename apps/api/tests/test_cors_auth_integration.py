"""Integration tests that CORS and auth play nicely on the SSE stream endpoint."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def client(monkeypatch):
    # Force auth so the protected routes require a token, and use the default
    # localhost CORS origin so we do not depend on the Railway regex/env.
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(auth_mod, "_REAUTH_SECRET", "cors-test-secret-value")

    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


def test_stream_without_token_returns_401_with_cors_headers(client):
    resp = client.get(
        "/api/v1/connectors/jobs/000000000000000000000000/stream",
        headers={"Origin": "http://localhost:5173"},
    )
    assert resp.status_code == 401
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
    assert resp.headers.get("access-control-allow-credentials") == "true"


def test_stream_with_query_token_is_allowed(client, monkeypatch):
    import src.services.auth_service as auth_mod

    email = "stream@test.local"
    token, _ = auth_mod.create_token(email)

    # The job id is fake; the stream endpoint will 404, but auth must pass first.
    resp = client.get(
        f"/api/v1/connectors/jobs/000000000000000000000000/stream?token={token}",
        headers={"Origin": "http://localhost:5173"},
    )
    # Auth passed; job not found -> 404 with CORS headers.
    assert resp.status_code == 404, resp.text
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"


def test_cors_preflight_for_stream_is_allowed(client):
    resp = client.options(
        "/api/v1/connectors/jobs/000000000000000000000000/stream",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert resp.status_code == 200
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
    assert "GET" in (resp.headers.get("access-control-allow-methods") or "")
    assert resp.headers.get("access-control-allow-credentials") == "true"
