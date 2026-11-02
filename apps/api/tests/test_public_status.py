"""Public status endpoint: unauthenticated, summary-only, cached."""

from __future__ import annotations

from fastapi.testclient import TestClient

from services import rbac
from src.main import app
from src.middleware import auth_middleware
from src.routers import status_router


def _reset_cache():
    status_router._cache.update(at=0.0, payload=None)


def test_status_is_public_in_both_gates():
    assert rbac._is_public_path("/api/v1/status")
    assert any("/api/v1/status".startswith(p) for p in auth_middleware._PUBLIC_PREFIXES)


def test_status_publishes_only_component_states(monkeypatch):
    _reset_cache()
    monkeypatch.setattr(
        "services.health_service.aggregate_health",
        lambda: {
            "status": "degraded",
            "services": {"api": "up", "mongodb": "up", "storage": "down", "connectors": "provisioning"},
            "mongodb": {"status": "up", "error": "secret-host:27017 auth detail"},
        },
    )
    body = TestClient(app).get("/api/v1/status").json()
    states = {c["id"]: c["status"] for c in body["components"]}
    assert states == {"api": "operational", "mongodb": "operational", "storage": "outage", "connectors": "degraded"}
    assert body["status"] == "outage"
    assert "secret-host" not in str(body)
    assert set(body) == {"status", "components", "checked_at", "cache_seconds"}


def test_all_operational_and_cached(monkeypatch):
    _reset_cache()
    calls = []

    def health():
        calls.append(1)
        return {"services": {"api": "up", "mongodb": "up", "storage": "up", "connectors": "ready"}}

    monkeypatch.setattr("services.health_service.aggregate_health", health)
    client = TestClient(app)
    assert client.get("/api/v1/status").json()["status"] == "operational"
    client.get("/api/v1/status")
    assert len(calls) == 1


def test_probe_failure_reports_outage_not_500(monkeypatch):
    _reset_cache()

    def boom():
        raise RuntimeError("mongo exploded")

    monkeypatch.setattr("services.health_service.aggregate_health", boom)
    resp = TestClient(app).get("/api/v1/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "outage"
    assert {c["id"]: c["status"] for c in body["components"]}["api"] == "operational"
    assert "exploded" not in str(body)
