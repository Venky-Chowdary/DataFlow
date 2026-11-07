"""Workspace IP allowlist: public paths, kill switch and break-glass at the middleware."""

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
    """The real allowlist middleware behind a stub auth layer.

    Since QA round 11 the list is enforced after auth by
    WorkspaceIpAllowlistMiddleware (every host, sessions and API keys);
    tests/test_r11_security_enforcement_e2e.py drives it through src.main.
    Here the workspace resolution and client address are pinned so this
    suite only exercises public paths, the kill switch and break-glass.
    """
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.middleware.base import BaseHTTPMiddleware

    from services import ip_allowlist_policy as policy
    from src.middleware import workspace_ip_allowlist_middleware as wm
    from src.services import auth_service

    tenant = Tenant(id="t-acme", workspace_id="ws-acme", ip_allowlist=[ALLOWED_IP])
    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    monkeypatch.setattr(policy, "client_ip_from_request",
                        lambda request: request.headers.get("x-test-client-ip", ""))
    monkeypatch.setattr(policy, "_request_workspaces",
                        lambda request: ({"ws-acme"}, request.headers.get("x-test-platform-admin") == "1"))
    monkeypatch.setattr("services.tenant_store.tenants_for_workspaces", lambda ids: [tenant])
    events: list = []
    monkeypatch.setattr(wm, "record_security_event", lambda event, **kw: events.append((event, kw)))

    class _StubAuth(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            if request.headers.get("x-test-user"):
                request.state.user_email = request.headers["x-test-user"]
                request.state.user = {"email": request.state.user_email, "role": "editor"}
            return await call_next(request)

    app = FastAPI()
    app.add_middleware(wm.WorkspaceIpAllowlistMiddleware)
    app.add_middleware(_StubAuth)

    @app.get("/")
    def root():
        return {"ok": True}

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/connectors")
    def connectors():
        return {"ok": True}

    c = TestClient(app)
    c.events = events
    return c


def _get(client, path: str, ip: str, **extra):
    return client.get(path, headers={"x-test-user": "u@example.com", "x-test-client-ip": ip, **extra})


@pytest.fixture
def enforce_default(monkeypatch):
    monkeypatch.delenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    monkeypatch.delenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)


def test_allowlist_blocks_disallowed_ip_by_default(client, enforce_default):
    resp = _get(client, "/api/v1/connectors", BLOCKED_IP)
    assert resp.status_code == 403
    assert resp.json()["reason"] == "ip_not_in_workspace_allowlist"
    assert [e for e, _ in client.events] == ["workspace.ip_allowlist.blocked"]
    assert client.events[0][1]["workspace_id"] == "ws-acme"


def test_allowlist_admits_allowed_ip_on_api_path(client, enforce_default):
    assert _get(client, "/api/v1/connectors", ALLOWED_IP).status_code == 200
    assert client.events == []


@pytest.mark.parametrize("path", ["/", "/health"])
def test_public_paths_skip_allowlist(client, enforce_default, path):
    assert _get(client, path, BLOCKED_IP).status_code == 200


def test_unauthenticated_requests_are_left_to_auth(client, enforce_default):
    res = client.get("/api/v1/connectors", headers={"x-test-client-ip": BLOCKED_IP})
    assert res.status_code == 200  # the stub has no auth; the real app 401s first


def test_break_glass_needs_a_platform_admin_and_a_reason(client, enforce_default):
    glass = {"X-Datawrap-Break-Glass": "INC-1 restore customer access"}
    assert _get(client, "/api/v1/connectors", BLOCKED_IP, **glass).status_code == 403
    assert _get(client, "/api/v1/connectors", BLOCKED_IP, **{"x-test-platform-admin": "1",
                "X-Datawrap-Break-Glass": "short"}).status_code == 403
    ok = _get(client, "/api/v1/connectors", BLOCKED_IP, **{"x-test-platform-admin": "1", **glass})
    assert ok.status_code == 200
    assert [e for e, _ in client.events][-1] == "workspace.ip_allowlist.break_glass"


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


@pytest.mark.parametrize("off", ["0", "false", "off", "no"])
def test_kill_switch_off_admits_and_audits_would_block(client, monkeypatch, off):
    """The platform kill switch: nothing is blocked, but the request that the
    allowlist would have blocked leaves an audit entry."""
    monkeypatch.delenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    monkeypatch.setenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", off)

    assert _get(client, "/api/v1/connectors", BLOCKED_IP).status_code == 200
    assert _get(client, "/api/v1/connectors", ALLOWED_IP).status_code == 200
    assert _get(client, "/health", BLOCKED_IP).status_code == 200

    assert [e for e, _ in client.events] == ["tenant.ip_allowlist.would_block"]
    kw = client.events[0][1]
    assert kw["details"]["tenant_id"] == "t-acme"
    assert kw["details"]["path"] == "/api/v1/connectors"
    assert kw["details"]["client_ip"] == BLOCKED_IP


def test_allowlist_events_are_registered():
    from services.audit_coverage import SECURITY_EVENTS

    for name in ("tenant.ip_allowlist.would_block", "workspace.ip_allowlist.blocked",
                 "workspace.ip_allowlist.break_glass", "workspace.ip_allowlist.lockout_refused"):
        assert name in SECURITY_EVENTS
