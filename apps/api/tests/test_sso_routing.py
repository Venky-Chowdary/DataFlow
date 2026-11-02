"""P4-6 — SSO domain routing: admin-claimed domains route to the IdP."""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services import integrations_store, sso_routing


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    store = tmp_path / "integrations.json"
    monkeypatch.setattr(integrations_store, "STORE_PATH", store)
    yield


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


# An enabled provider must be complete (R8: enabling with required fields
# blank is refused), so claims are made on a usable configuration.
_COMPLETE = {
    "saml": {"entity_id": "urn:sp", "sso_url": "https://idp.test/saml", "x509_cert": "MIIC" + "A" * 40},
    "oidc": {"issuer": "https://idp.test", "client_id": "c", "client_secret": "s",
             "redirect_uri": "https://app.test/cb"},
    "azure_ad": {"tenant_id": "t", "client_id": "c", "client_secret": "s",
                 "redirect_uri": "https://app.test/cb"},
}


def _claim(sso_type: str, domains: list[str], enabled: bool = True):
    integrations_store.update_sso_config(
        sso_type, {**_COMPLETE[sso_type], "enabled": enabled, "email_domains": domains})


def test_no_claim_returns_null():
    c = _client()
    resp = c.post("/api/v1/auth/sso/route", json={"email": "a@acme.com"})
    assert resp.status_code == 200
    assert resp.json() == {"sso": None}


def test_claimed_domain_routes_to_provider():
    _claim("oidc", ["acme.com"])
    c = _client()
    resp = c.post("/api/v1/auth/sso/route", json={"email": "a@acme.com"})
    assert resp.status_code == 200
    assert resp.json() == {"sso": "oidc",
                           "start_url": "/api/v1/auth/sso/oidc/start"}
    # a different domain still gets the password flow
    resp = c.post("/api/v1/auth/sso/route", json={"email": "b@other.com"})
    assert resp.json() == {"sso": None}


def test_disabled_provider_never_claims():
    _claim("saml", ["acme.com"], enabled=False)
    assert sso_routing.provider_for_email("a@acme.com") is None


def test_domain_matching_is_case_insensitive():
    _claim("azure_ad", ["Acme.COM"])
    assert sso_routing.provider_for_email("A@ACME.com") == "azure_ad"


def test_email_domains_persist_on_config():
    _claim("oidc", ["acme.com", "acme.io"])
    cfg = integrations_store.get_sso_configs()["oidc"]
    assert cfg["email_domains"] == ["acme.com", "acme.io"]
    # Domains are normalized to lower case on save (R8).


def test_route_is_ip_throttled_but_shape_never_changes(monkeypatch):
    from services import auth_state
    auth_state.reset_auth_state()
    _claim("oidc", ["acme.com"])
    c = _client()
    # exhaust the 30/min bucket
    for _ in range(31):
        auth_state.get_auth_state().rate_limit_hit(
            "sso-route:ip:testclient", limit=30, window_sec=60)
    resp = c.post("/api/v1/auth/sso/route", json={"email": "a@acme.com"})
    # throttled answers are the same {sso: null} shape as an unclaimed domain
    assert resp.status_code == 200
    assert resp.json() == {"sso": None}
