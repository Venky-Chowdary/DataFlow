"""D117 — a missing DATAWRAP_OTP_PEPPER only refuses traffic when signup is on.

The pepper is used by self-serve signup alone (OTP codes, password-reset
tokens). Requiring it unconditionally made ProductionConfigMiddleware answer
503 on every route of a deployment that never enabled signup.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services import platform_config
from src.middleware.production_config_middleware import ProductionConfigMiddleware


@pytest.fixture
def production_env(monkeypatch):
    """A production config that is complete except for the OTP pepper."""
    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        for name in ("OTP_PEPPER", "ALLOW_DEV_USER", "AUTH_USERS", "SIGNUP_ENABLED"):
            monkeypatch.delenv(prefix + name, raising=False)
    for name in ("RAILWAY_ENVIRONMENT", "RAILWAY_SERVICE_ID", "RAILWAY_TOKEN",
                 "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_DEPLOYMENT_ID", "RAILWAY_REPLICA_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    monkeypatch.setenv("DATAWRAP_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAWRAP_AUTH_SECRET", "x" * 40)
    monkeypatch.setenv("DATAWRAP_SECRETS_KEY", "y" * 40)
    monkeypatch.setenv("DATAWRAP_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAWRAP_ADMIN_PASSWORD", "admin-password-long")
    monkeypatch.setenv("MONGODB_URI", "mongodb+srv://db.example.com/dataflow")
    assert platform_config.is_production() is True


def _client() -> TestClient:
    app = FastAPI()
    app.add_middleware(ProductionConfigMiddleware)
    app.state.config_errors = platform_config.validate_production_config()

    @app.get("/api/v1/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/connectors")
    def connectors():
        return []

    return TestClient(app)


def test_signup_off_without_pepper_serves_traffic(production_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "0")
    assert platform_config.validate_production_config() == []
    c = _client()
    assert c.get("/api/v1/health").status_code == 200
    assert c.get("/api/v1/connectors").status_code == 200


def test_signup_on_without_pepper_is_still_refused(production_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    errors = platform_config.validate_production_config()
    assert any("OTP_PEPPER" in e for e in errors)
    c = _client()
    assert c.get("/api/v1/health").status_code == 200  # liveness stays open
    assert c.get("/api/v1/connectors").status_code == 503


def test_signup_on_with_pepper_serves_traffic(production_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "p" * 32)
    assert platform_config.validate_production_config() == []
    assert _client().get("/api/v1/connectors").status_code == 200
