"""QA round 11: public origin allow-list (MCP manifest, SAML) and session durability helpers.

The REST paths are covered in test_r11_security_enforcement_e2e.py; these pin
the two helpers whose callers are hard to reach without an IdP.
"""

from __future__ import annotations

import time

import pytest
from starlette.requests import Request


def _request(headers: dict[str, str], *, host: str = "dataflow-api-production-722b.up.railway.app",
             scheme: str = "http") -> Request:
    raw = [(b"host", host.encode())] + [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "scheme": scheme, "server": (host, 80), "path": "/",
                    "headers": raw, "query_string": b"", "client": ("10.0.0.2", 1)})


@pytest.fixture
def prod(monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAWRAP_WEB_DOMAIN", "app.datawrap.example")
    monkeypatch.setattr("services.tenant_store.list_tenants", lambda: [])


def test_saml_base_url_ignores_a_forwarded_host_off_the_allow_list(prod):
    from src.routers.auth_router import _saml_base_url

    req = _request({"X-Forwarded-Host": "attacker.example", "X-Forwarded-Proto": "http",
                    "X-Forwarded-Port": "8443"})
    assert _saml_base_url(req) == "https://app.datawrap.example"


def test_saml_base_url_honours_an_allow_listed_host_over_https(prod, monkeypatch):
    from src.routers.auth_router import _saml_base_url

    monkeypatch.setenv("DATAWRAP_PUBLIC_ORIGIN_ALLOWED_HOSTS", "sso.customer.example")
    req = _request({"X-Forwarded-Host": "sso.customer.example", "X-Forwarded-Proto": "http"})
    assert _saml_base_url(req) == "https://sso.customer.example"


def test_public_api_origin_wins_over_the_web_domain(prod, monkeypatch):
    from services.public_origin import public_origin

    monkeypatch.setenv("DATAWRAP_PUBLIC_API_ORIGIN", "http://api.datawrap.example/")
    assert public_origin(_request({})) == "https://api.datawrap.example"


def test_development_without_configuration_uses_the_request(monkeypatch):
    from services.public_origin import public_origin

    for name in ("DATAWRAP_WEB_DOMAIN", "DATAFLOW_WEB_DOMAIN", "DATAWRAP_WEB_URL", "DATAFLOW_WEB_URL",
                 "DATAWRAP_PUBLIC_API_ORIGIN", "DATAFLOW_PUBLIC_API_ORIGIN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("services.tenant_store.list_tenants", lambda: [])
    assert public_origin(_request({}, host="localhost:8000")) == "http://localhost:8000"


def test_a_session_without_activity_on_record_is_recorded_as_seen_now(tmp_path, monkeypatch):
    """The idle check must start, not be skipped, when no activity is recorded."""
    from services import auth_sessions
    from src.middleware import auth_middleware

    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(auth_sessions, "_collection", lambda: None)
    # A token whose row is gone from the store this process reads (e.g. the
    # file lost on redeploy while the row lived elsewhere): no activity known.
    jti = "lost-row-jti"
    assert auth_sessions.session_last_seen(jti) is None

    monkeypatch.setattr("src.services.auth_service.token_jti", lambda token: jti)
    assert auth_middleware._session_idled_out("tok", "idle@example.com") is False
    seen = auth_sessions.session_last_seen(jti)
    assert seen is not None and abs(seen - time.time()) < 5


def test_revocation_in_either_store_wins(tmp_path, monkeypatch):
    from services import auth_sessions

    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(auth_sessions, "_collection", lambda: None)
    jti = auth_sessions.create_session("rev@example.com", expires_at=int(time.time()) + 3600)
    assert auth_sessions.session_active(jti, "rev@example.com")
    assert auth_sessions.revoke_session(jti)
    assert not auth_sessions.session_active(jti, "rev@example.com")
    assert auth_sessions.revoke_all_for_email("rev@example.com") == 0
