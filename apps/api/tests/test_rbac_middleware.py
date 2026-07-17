"""Unit tests for the RBAC middleware."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def rbac_env(monkeypatch):
    """Force auth + RBAC on with a known admin user."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-rbac")

    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)

    import services.rbac as rbac_mod

    # Force re-evaluation of auth_required in the rbac module.
    monkeypatch.setattr(rbac_mod, "auth_required", auth_mod.auth_required)


def _token(email: str) -> str:
    from src.services.auth_service import create_token

    return create_token(email)[0]


def _client(rbac_env, user: dict | None = None):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from src.services.rbac import RBACMiddleware

    app = FastAPI()
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/jobs")
    def list_jobs():
        return {"jobs": []}

    @app.post("/api/v1/transfer/run")
    def run_transfer():
        return {"job_id": "123"}

    @app.post("/api/v1/connectors")
    def create_connector():
        return {"id": "c1"}

    @app.get("/api/v1/connectors")
    def list_connectors():
        return {"connectors": []}

    return TestClient(app)


def test_public_route_bypasses_rbac(rbac_env):
    client = _client(rbac_env)
    assert client.get("/health").status_code == 200


def test_missing_token_is_401(rbac_env):
    client = _client(rbac_env)
    assert client.get("/api/v1/jobs").status_code == 401


def test_admin_can_run_transfer(rbac_env, monkeypatch):
    client = _client(rbac_env)
    token = _token("admin@example.com")
    resp = client.post("/api/v1/transfer/run", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


def test_viewer_cannot_run_transfer(rbac_env, monkeypatch):
    from services.rbac import _ROLE_PERMISSIONS, normalize_role

    # Patch viewer so admin token is treated as viewer for the permission check.
    import services.rbac as rbac_mod

    original = rbac_mod.normalize_role
    rbac_mod.normalize_role = lambda role: "viewer"
    try:
        client = _client(rbac_env)
        token = _token("admin@example.com")
        resp = client.post("/api/v1/transfer/run", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 403
        assert "Permission denied" in resp.json()["detail"]
    finally:
        rbac_mod.normalize_role = original


def test_editor_can_write_connector(rbac_env, monkeypatch):
    import services.rbac as rbac_mod

    original = rbac_mod.normalize_role
    rbac_mod.normalize_role = lambda role: "editor"
    try:
        client = _client(rbac_env)
        token = _token("admin@example.com")
        resp = client.post("/api/v1/connectors", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200
    finally:
        rbac_mod.normalize_role = original
