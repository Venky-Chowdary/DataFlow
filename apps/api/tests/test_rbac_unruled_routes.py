from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def rbac_client(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from services import rbac

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-unruled-routes")
    monkeypatch.setattr(rbac._auth_service, "_REQUIRE_AUTH", True)

    app = FastAPI()
    app.add_middleware(rbac.RBACMiddleware)
    app.add_middleware(AuthMiddleware)

    @app.get("/unruled-probe")
    def get_unruled_probe():
        return {"ok": True}

    @app.post("/unruled-probe")
    def post_unruled_probe():
        return {"ok": True}

    @app.post("/api/v1/transfer/execute")
    def execute_transfer():
        return {"ok": True}

    @app.get("/api/v1/usage/summary")
    def usage_summary():
        return {"ok": True}

    return TestClient(app)


def _token() -> str:
    from src.services.auth_service import create_token

    return create_token("admin@example.com")[0]


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}"}


def _clear_denials(monkeypatch) -> None:
    from services import audit_coverage

    with audit_coverage._DENIAL_LOCK:
        audit_coverage._DENIALS.clear()


def test_deny_audits_no_rule_and_deduplicates(rbac_client, monkeypatch):
    from services import audit_coverage, rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "deny", raising=False)
    _clear_denials(monkeypatch)
    monkeypatch.setattr(audit_coverage, "_DENIAL_WINDOW_SECONDS", 100)
    events = []
    monkeypatch.setattr(audit_coverage.audit_log, "append_audit_event", lambda **event: events.append(event))

    for _ in range(5):
        response = rbac_client.get("/unruled-probe", headers=_headers())
        assert response.status_code == 403
        assert response.json()["reason"] == "no_rule"
    assert len(events) == 1
    assert events[0]["action"] == "authz.denied"
    assert events[0]["details"]["reason"] == "no_rule"
    assert events[0]["details"]["method"] == "GET"
    assert events[0]["details"]["path"] == "/unruled-probe"
    assert events[0]["actor"] == "admin@example.com"

    monkeypatch.setattr(audit_coverage, "_DENIAL_WINDOW_SECONDS", 0)
    response = rbac_client.get("/unruled-probe", headers=_headers())
    assert response.status_code == 403
    assert len(events) == 2
    assert events[1]["details"]["suppressed_since_last"] == 4


def test_unknown_path_is_denied_as_no_rule(rbac_client, monkeypatch):
    from services import audit_coverage, rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "deny", raising=False)
    monkeypatch.setattr(audit_coverage.audit_log, "append_audit_event", lambda **_event: None)
    response = rbac_client.get("/not-a-real-route", headers=_headers())
    assert response.status_code == 403
    assert response.json()["reason"] == "no_rule"


@pytest.mark.parametrize(
    ("method", "path"),
    (
        ("HEAD", "/health"),
        ("GET", "/api/v1/catalog/not-registered"),
    ),
)
def test_public_paths_bypass_real_route_matching(
    rbac_client, monkeypatch, method, path
):
    from services import rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "deny", raising=False)
    response = rbac_client.request(method, path, headers=_headers())
    assert response.status_code != 403


def test_head_on_get_route_uses_get_permission(rbac_client, monkeypatch):
    from services import rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "deny", raising=False)
    monkeypatch.setattr(rbac, "normalize_role", lambda _role: "viewer")
    assert rbac._required_permission("HEAD", "/api/v1/usage/summary") == "job.read"
    response = rbac_client.head("/api/v1/usage/summary", headers=_headers())
    assert response.status_code != 403


def test_allow_and_log_uses_legacy_permissions_and_warns_each_hit(
    rbac_client, monkeypatch, caplog
):
    from services import audit_coverage, rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "allow_and_log", raising=False)
    _clear_denials(monkeypatch)
    monkeypatch.setattr(audit_coverage.audit_log, "append_audit_event", lambda **_event: None)
    monkeypatch.setattr(
        rbac, "normalize_role", lambda _role: "viewer"
    )
    with caplog.at_level(logging.WARNING, logger="services.rbac"):
        assert rbac_client.get("/unruled-probe", headers=_headers()).status_code == 200
        assert rbac_client.get("/unruled-probe", headers=_headers()).status_code == 200
        denied_write = rbac_client.post("/unruled-probe", headers=_headers())

    assert denied_write.status_code == 403
    assert denied_write.json()["required_permission"] == "connector.write"
    warnings = [record for record in caplog.records if "Unruled RBAC route" in record.message]
    assert len(warnings) == 3
    assert all("/unruled-probe" in warning.message for warning in warnings)


def test_startup_allow_and_log_warning_and_audit_once(monkeypatch, caplog):
    from services import audit_log, rbac

    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "allow_and_log", raising=False)
    monkeypatch.setattr(rbac, "_UNRULED_STARTUP_RECORDED", False, raising=False)
    events = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **event: events.append(event))

    with caplog.at_level(logging.WARNING, logger="services.rbac"):
        rbac.record_unruled_routes_allowed_startup()
        rbac.record_unruled_routes_allowed_startup()

    assert len(events) == 1
    assert events[0]["action"] == "authz.config.unruled_routes_allowed"
    assert sum("allow_and_log" in record.message for record in caplog.records) == 1


def test_auth_disabled_skips_rbac_and_unruled_logging(rbac_client, monkeypatch, caplog):
    from services import rbac

    monkeypatch.setattr(rbac._auth_service, "auth_required", lambda: False)
    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", "deny", raising=False)
    with caplog.at_level(logging.WARNING, logger="services.rbac"):
        assert rbac_client.get("/unruled-probe").status_code == 200
        assert rbac_client.post("/unruled-probe").status_code == 200
    assert not any("Unruled RBAC route" in record.message for record in caplog.records)


def test_invalid_configuration_fails_closed_and_names_variable(
    rbac_client, monkeypatch, caplog
):
    from services import audit_coverage, rbac

    with caplog.at_level(logging.ERROR, logger="services.rbac"):
        with pytest.raises(rbac.RBACConfigError, match="DATAFLOW_RBAC_UNRULED_ROUTES"):
            rbac._parse_unruled_route_mode("permit")
        mode = rbac._load_unruled_route_mode("permit")

    assert mode == "deny"
    assert any("DATAFLOW_RBAC_UNRULED_ROUTES" in record.message for record in caplog.records)
    monkeypatch.setattr(rbac, "UNRULED_ROUTE_MODE", mode, raising=False)
    monkeypatch.setattr(audit_coverage.audit_log, "append_audit_event", lambda **_event: None)
    response = rbac_client.get("/unruled-probe", headers=_headers())
    assert response.status_code == 403
    assert response.json()["reason"] == "no_rule"


def test_new_explicit_rules_preserve_fallback_permissions(rbac_client, monkeypatch):
    from services import audit_coverage, rbac

    monkeypatch.setattr(audit_coverage.audit_log, "append_audit_event", lambda **_event: None)
    monkeypatch.setattr(rbac, "normalize_role", lambda _role: "viewer")
    assert rbac._required_permission("POST", "/api/v1/transfer/execute") == "connector.write"
    assert rbac._required_permission("GET", "/api/v1/usage/summary") == "job.read"
    assert rbac_client.get("/api/v1/usage/summary", headers=_headers()).status_code == 200
    assert rbac_client.post("/api/v1/transfer/execute", headers=_headers()).status_code == 403

    monkeypatch.setattr(rbac, "normalize_role", lambda _role: "editor")
    assert rbac_client.post("/api/v1/transfer/execute", headers=_headers()).status_code == 200
