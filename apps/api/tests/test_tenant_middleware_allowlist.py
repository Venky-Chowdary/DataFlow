"""Tenant IP allowlist: public-path matching must not switch the allowlist off."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.tenant_store import Tenant
from src.middleware import tenant_middleware as tm

ALLOWED_IP = "203.0.113.10"
BLOCKED_IP = "198.51.100.7"


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/connectors",
        "/api/v1/transfer/jobs",
        "/api/v1/auth/me",
        "/healthz",
        "/health-admin",
        "/api/v1/healthcheck-debug",
        "/auth/login-as",
        "/api/v1/auth/loginx",
        "/api/v1/mcp/tools/call",
        "/api/v1/mcp/logs",
        "/api/v1/mcp/statusx",
        "//api/v1/connectors",
    ],
)
def test_protected_paths_are_not_public(path):
    assert tm._is_public_path(path) is False


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/health",
        "/health/ready",
        "/api/v1/health",
        "/api/v1/auth/login",
        "/api/v1/auth/bootstrap",
        "/auth/login",
        "/api/v1/auth/sso/providers",
        "/api/v1/mcp",
        "/api/v1/mcp/",
        "/api/v1/mcp/manifest",
        "/api/v1/mcp/status",
        "/api/v1/mcp/tools",
    ],
)
def test_public_paths_stay_public(path):
    assert tm._is_public_path(path) is True


@pytest.fixture
def client(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    tenant = Tenant(
        id="t-acme",
        workspace_id="ws-acme",
        custom_domain="acme.example.com",
        ip_allowlist=[ALLOWED_IP],
    )
    # Client-IP resolution has its own trust-boundary tests; pin it here so
    # this suite only exercises public-path matching and the allowlist.
    monkeypatch.setattr(
        tm, "_client_ip", lambda request: request.headers.get("x-test-client-ip", "")
    )
    monkeypatch.setattr(
        tm,
        "resolve_tenant_from_host",
        lambda host: tenant if host == "acme.example.com" else None,
    )

    app = FastAPI()
    app.add_middleware(tm.TenantMiddleware)

    @app.get("/")
    def root():
        return {"ok": True}

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/connectors")
    def connectors():
        return {"ok": True}

    return TestClient(app)


def _get(client, path: str, ip: str, host: str = "acme.example.com"):
    return client.get(path, headers={"host": host, "x-test-client-ip": ip})


def test_allowlist_blocks_disallowed_ip_on_api_path(client, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", "1")
    resp = _get(client, "/api/v1/connectors", BLOCKED_IP)
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Access denied by tenant IP allowlist"


def test_allowlist_admits_allowed_ip_on_api_path(client):
    assert _get(client, "/api/v1/connectors", ALLOWED_IP).status_code == 200


@pytest.mark.parametrize("path", ["/", "/health"])
def test_public_paths_skip_allowlist(client, path):
    assert _get(client, path, BLOCKED_IP).status_code == 200


def test_non_tenant_host_is_not_filtered(client):
    assert (
        _get(
            client, "/api/v1/connectors", BLOCKED_IP, host="other.example.com"
        ).status_code
        == 200
    )


def test_every_unauthenticated_route_stays_reachable_on_allowlisted_tenant():
    """Drift guard: whatever auth_middleware serves without a session (signup,
    verify-email, Resend webhook, contact-sales, ...) must not be allowlisted
    away on a tenant custom domain."""
    from src.middleware import auth_middleware as am

    missing = [
        p for p in am._PUBLIC_PREFIXES
        if p.strip("/") and not tm._is_public_path(p)
        and p not in ("/docs", "/redoc", "/openapi.json")
    ]
    assert missing == []


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/auth/resend-webhook",
        "/api/v1/public/email-events",
        "/api/v1/auth/signup",
        "/api/v1/auth/verify-email",
        "/api/v1/contact-sales",
        "/api/v1/contact-sales/config",
        "/api/v1/auth/sso/okta/callback",
        "/api/v1/auth/password/reset/inspect",
        "/api/v1/auth/signup/config",
    ],
)
def test_webhooks_signup_and_verify_skip_allowlist(client, path):
    assert tm._is_public_path(path) is True


def test_enforcement_is_off_by_default_and_audits_would_block(client, monkeypatch):
    """Default = production behaviour before the fix: nothing is blocked, but
    the request that the allowlist would have blocked leaves an audit entry."""
    monkeypatch.delenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    monkeypatch.delenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    events = []
    monkeypatch.setattr(
        tm, "record_security_event", lambda event, **kw: events.append((event, kw))
    )

    assert _get(client, "/api/v1/connectors", BLOCKED_IP).status_code == 200
    assert _get(client, "/api/v1/connectors", ALLOWED_IP).status_code == 200
    assert _get(client, "/health", BLOCKED_IP).status_code == 200

    assert [e for e, _ in events] == ["tenant.ip_allowlist.would_block"]
    kw = events[0][1]
    assert kw["details"]["tenant_id"] == "t-acme"
    assert kw["details"]["path"] == "/api/v1/connectors"
    assert kw["details"]["client_ip"] == BLOCKED_IP


def test_would_block_event_is_registered():
    from services.audit_coverage import SECURITY_EVENTS

    assert "tenant.ip_allowlist.would_block" in SECURITY_EVENTS


def test_enforce_flag_off_values_do_not_block(client, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", "0")
    monkeypatch.setattr(tm, "record_security_event", lambda *a, **k: None)
    assert _get(client, "/api/v1/connectors", BLOCKED_IP).status_code == 200
