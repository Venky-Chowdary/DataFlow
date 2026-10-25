"""P0-6 — DATAWRAP_SIGNUP_ENABLED gates every signup route (default off).

Flag off: the new routes answer 404 — byte-for-byte what an unknown route did
before this feature existed. Flag on: the gated surface opens. The check runs
inside the router dependency so both mounts (/api/v1 and the root alias)
behave identically.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _client(monkeypatch: pytest.MonkeyPatch, *, enabled: bool, require_auth: bool) -> TestClient:
    if enabled:
        monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    else:
        monkeypatch.delenv("DATAWRAP_SIGNUP_ENABLED", raising=False)
        monkeypatch.delenv("DATAFLOW_SIGNUP_ENABLED", raising=False)
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1" if require_auth else "0")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "admin-password-123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-unit-tests")

    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router
    from src.routers.signup_router import router as signup_router

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(auth_router)
    app.include_router(signup_router, prefix="/api/v1")
    app.include_router(signup_router)
    return TestClient(app)


def test_signup_routes_404_when_flag_off_unauthenticated_prod(monkeypatch):
    """Production posture: auth required. Flag off -> defined routes 404."""
    client = _client(monkeypatch, enabled=False, require_auth=True)
    # Signup data routes must 404, not 401/405. The capability probe
    # (signup/config) is the one route that answers flag-off by design.
    assert client.post("/api/v1/auth/signup", json={}).status_code == 404
    assert client.get("/auth/form-token").status_code == 404
    # Paths no router defines are 404 anyway — flag state is invisible.
    assert client.post("/api/v1/auth/signup", json={}).status_code == 404
    assert client.post("/api/v1/auth/verify-email", json={}).status_code == 404
    assert client.post("/api/v1/auth/resend", json={}).status_code == 404


def test_signup_routes_404_when_flag_off_dev_mode(monkeypatch):
    client = _client(monkeypatch, enabled=False, require_auth=False)
    assert client.post("/api/v1/auth/verify-email", json={}).status_code == 404
    assert client.post("/api/v1/auth/signup", json={}).status_code == 404


def test_flag_off_get_vs_post_on_defined_route():
    """Documenting the gate: 404 comes from the dependency, per-method.

    An unmounted method (POST on a GET-only route) still answers Starlette's
    405 even with the flag off — that is route-table truth, not feature
    surface, and matches how every other router behaves.
    """
    client = _client(pytest.MonkeyPatch(), enabled=False, require_auth=True)
    assert client.get("/api/v1/auth/form-token").status_code == 404
    assert client.post("/api/v1/auth/form-token", json={}).status_code == 405


def test_signup_config_reachable_when_flag_on(monkeypatch):
    client = _client(monkeypatch, enabled=True, require_auth=True)
    resp = client.get("/api/v1/auth/signup/config")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert "turnstile_site_key" in body


def test_signup_config_root_alias_when_flag_on(monkeypatch):
    client = _client(monkeypatch, enabled=True, require_auth=True)
    resp = client.get("/auth/signup/config")
    assert resp.status_code == 200
    assert resp.json()["enabled"] is True


def test_signup_config_reachable_when_flag_off(monkeypatch):
    """Capability probe must answer (enabled:false) with the flag off so
    the login page can feature-detect without minting a form token."""
    client = _client(monkeypatch, enabled=False, require_auth=True)
    for path in ("/api/v1/auth/signup/config", "/auth/signup/config"):
        resp = client.get(path)
        assert resp.status_code == 200
        body = resp.json()
        assert body["enabled"] is False
        assert body["turnstile_site_key"] == ""
