"""ITEM 4 — /auth/login must 429 under burst; lockout after failed attempts.

Live router + AuthMiddleware (not a stubbed limiter). Proves programmatic
callers cannot brute-force without hitting per-IP / per-account throttles.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def login_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "item4-login-rate-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "3")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "0.01")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "3")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_SEC", "120")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")

    import src.services.auth_service as auth_mod
    from services.auth_rate_limit import reset_auth_rate_limits

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    reset_auth_rate_limits()


def _client(login_env):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(auth_router)
    return TestClient(app)


def test_login_burst_returns_429_with_retry_after(login_env):
    client = _client(login_env)
    body = {"email": "admin@example.com", "password": "wrong-password-xxx"}
    codes = []
    for _ in range(5):
        response = client.post("/api/v1/auth/login", json=body)
        codes.append(response.status_code)
        if response.status_code == 429:
            assert "Retry-After" in response.headers
            assert int(response.headers["Retry-After"]) >= 1
            assert "Too many login attempts" in response.json()["detail"]
            break
    assert 429 in codes, codes
    # At least one attempt was evaluated before throttle (401), proving login ran.
    assert 401 in codes, codes


def test_login_lockout_returns_429_after_failed_attempts(login_env, monkeypatch):
    # Generous burst so lockout (not token bucket) is the denier.
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "50")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "10")
    from services.auth_rate_limit import reset_auth_rate_limits

    reset_auth_rate_limits()

    client = _client(login_env)
    body = {"email": "admin@example.com", "password": "wrong-password-xxx"}
    for _ in range(3):
        response = client.post("/api/v1/auth/login", json=body)
        assert response.status_code == 401, response.text
    locked = client.post("/api/v1/auth/login", json=body)
    assert locked.status_code == 429, locked.text
    assert "Retry-After" in locked.headers
