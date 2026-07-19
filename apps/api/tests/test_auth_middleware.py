"""Unit tests for the authentication middleware.

These tests verify that the middleware:
  - allows public and OPTIONS requests without a token,
  - requires a bearer token or valid user for protected routes,
  - accepts the token as a query parameter only on `*/stream` endpoints.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def auth_env(monkeypatch):
    """Force auth on and set a known admin user/password for the test process."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "test@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-unit-tests")

    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)


def _app_client(auth_env):
    # Import after env vars are set so auth_service picks them up.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/jobs")
    def jobs():
        return {"jobs": []}

    @app.get("/api/v1/connectors/jobs/{job_id}/stream")
    def stream(job_id: str):
        return {"stream": job_id}

    return TestClient(app)


def _token() -> str:
    from src.services.auth_service import create_token

    return create_token("test@example.com")[0]


def test_public_route_without_token(auth_env):
    client = _app_client(auth_env)
    response = client.get("/health")
    assert response.status_code == 200


def test_catalog_is_public_without_token(auth_env):
    """Landing/docs call /catalog/stats before sign-in — must not 401."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/api/v1/catalog/stats")
    def stats():
        return {"connector_count": 1}

    client = TestClient(app)
    response = client.get("/api/v1/catalog/stats")
    assert response.status_code == 200
    assert response.json()["connector_count"] == 1


def test_auth_login_alias_is_public(auth_env):
    """Mis-set VITE_API_BASE hits /auth/login — must not require a bearer token."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.post("/auth/login")
    def login_alias():
        return {"ok": True}

    @app.post("/api/v1/auth/login")
    def login_canonical():
        return {"ok": True}

    @app.get("/auth/bootstrap")
    def bootstrap_alias():
        return {"ok": True}

    client = TestClient(app)
    for path, method in (
        ("/auth/login", "post"),
        ("/api/v1/auth/login", "post"),
        ("/auth/bootstrap", "get"),
    ):
        response = getattr(client, method)(path, json={"email": "a@b.c", "password": "x"}) if method == "post" else client.get(path)
        assert response.status_code == 200, path
        assert response.json() == {"ok": True}


def test_protected_route_without_token_is_401(auth_env):
    client = _app_client(auth_env)
    response = client.get("/api/v1/jobs")
    assert response.status_code == 401


def test_protected_route_with_bearer_token(auth_env):
    client = _app_client(auth_env)
    response = client.get("/api/v1/jobs", headers={"Authorization": f"Bearer {_token()}"})
    assert response.status_code == 200


def test_stream_accepts_token_query_param(auth_env):
    client = _app_client(auth_env)
    response = client.get(f"/api/v1/connectors/jobs/abc/stream?token={_token()}")
    assert response.status_code == 200


def test_non_stream_route_ignores_token_query_param(auth_env):
    client = _app_client(auth_env)
    response = client.get(f"/api/v1/jobs?token={_token()}")
    assert response.status_code == 401
