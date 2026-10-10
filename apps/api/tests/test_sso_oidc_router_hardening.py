"""Regression coverage for OIDC router state and callback handling."""

from __future__ import annotations

import os
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def oidc_client_fixture(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(os.path.dirname(os.path.dirname(__file__)))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_ENABLE_DOCS", "0")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")

    from services import integrations_store, sso_state
    from src.main import app
    from src.routers import auth_router

    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(sso_state, "STATE_PATH", tmp_path / "sso_state.json")
    monkeypatch.setattr(sso_state, "data_dir", lambda: tmp_path)

    config = {
        "issuer": "https://issuer.example",
        "client_id": "client-123",
        "client_secret": "secret-never-in-state",
        "redirect_uri": "https://testserver/api/v1/auth/sso/oidc/callback",
        "scopes": "openid email profile",
    }
    monkeypatch.setattr(
        integrations_store,
        "get_sso_config_raw",
        lambda _sso_type: dict(config),
    )
    monkeypatch.setattr(
        integrations_store,
        "validate_sso_config",
        lambda _sso_type: {"ready": True, "message": "Configuration complete"},
    )
    client = TestClient(app, base_url="https://testserver")
    return client, config, auth_router, sso_state


def _metadata():
    from services.oidc_client import OidcProviderMetadata

    return OidcProviderMetadata(
        issuer="https://issuer.example",
        authorization_endpoint="https://issuer.example/authorize-custom",
        token_endpoint="https://issuer.example/token-custom",
        jwks_uri="https://issuer.example/jwks",
        id_token_signing_alg_values_supported=("RS256",),
    )


def test_oidc_start_uses_discovery_and_does_not_store_secret(oidc_client_fixture, monkeypatch):
    client, _config, auth_router, sso_state = oidc_client_fixture
    issuer_calls = []
    monkeypatch.setattr(
        auth_router.oidc_client,
        "discover",
        lambda issuer: (issuer_calls.append(issuer) or _metadata()),
    )

    response = client.get("/api/v1/auth/sso/oidc/start", follow_redirects=False)

    assert response.status_code == 302
    location = urlsplit(response.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == (
        "https://issuer.example/authorize-custom"
    )
    params = parse_qs(location.query)
    assert issuer_calls == ["https://issuer.example"]
    stored = sso_state.get_state(params["state"][0], "oidc")
    assert set(stored["extra"]) == {
        "code_verifier",
        "nonce",
        "issuer",
        "redirect_uri",
        "client_id",
    }
    assert "secret-never-in-state" not in repr(stored)
    assert "client_secret" not in stored["extra"]


def test_azure_multitenant_start_is_rejected(oidc_client_fixture, monkeypatch):
    client, config, auth_router, _sso_state = oidc_client_fixture
    from services import integrations_store

    config["tenant_id"] = "common"
    monkeypatch.setattr(
        integrations_store,
        "get_sso_config_raw",
        lambda _sso_type: dict(config),
    )
    response = client.get("/api/v1/auth/sso/azure_ad/start", follow_redirects=False)
    assert response.status_code == 400
    assert "multi_tenant_not_supported" in response.json()["detail"]


def _seed_callback_state(sso_state):
    sso_state.set_state(
        "callback-state",
        "oidc",
        {
            "code_verifier": "verifier",
            "nonce": "nonce",
            "issuer": "https://issuer.example",
            "redirect_uri": "https://testserver/api/v1/auth/sso/oidc/callback",
            "client_id": "client-123",
        },
    )


def _mock_http(auth_router, monkeypatch, handler):
    monkeypatch.setattr(
        auth_router.oidc_client,
        "_http_client",
        lambda: httpx.Client(
            transport=httpx.MockTransport(handler),
            timeout=10.0,
        ),
    )


def test_bad_id_token_fails_without_userinfo_request(
    oidc_client_fixture,
    monkeypatch,
):
    client, _config, auth_router, sso_state = oidc_client_fixture
    _seed_callback_state(sso_state)
    monkeypatch.setattr(auth_router.oidc_client, "discover", lambda _issuer: _metadata())
    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path == "/token-custom":
            return httpx.Response(200, json={"id_token": "not-a-jwt"})
        return httpx.Response(404)

    _mock_http(auth_router, monkeypatch, handler)
    response = client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": "authorization-code", "state": "callback-state"},
    )

    assert response.status_code == 401
    assert "/userinfo" not in requests
    assert requests == ["/token-custom"]
    assert "not-a-jwt" not in response.text


def test_token_endpoint_500_returns_safe_503(
    oidc_client_fixture,
    monkeypatch,
):
    client, _config, auth_router, sso_state = oidc_client_fixture
    _seed_callback_state(sso_state)
    monkeypatch.setattr(auth_router.oidc_client, "discover", lambda _issuer: _metadata())
    _mock_http(
        auth_router,
        monkeypatch,
        lambda _request: httpx.Response(
            500,
            text="upstream secret diagnostic",
        ),
    )

    response = client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"code": "authorization-code", "state": "callback-state"},
    )

    assert response.status_code == 503
    assert "upstream secret diagnostic" not in response.text
    assert response.json()["detail"] == "SSO login failed (token_exchange_failed)"


def test_provider_error_is_not_reflected_and_consumes_state(oidc_client_fixture):
    client, _config, _auth_router, sso_state = oidc_client_fixture
    _seed_callback_state(sso_state)

    response = client.get(
        "/api/v1/auth/sso/oidc/callback",
        params={"error": "provider-secret-error", "state": "callback-state"},
    )

    assert response.status_code == 400
    assert "provider-secret-error" not in response.text
    assert response.json()["detail"] == (
        "SSO login was cancelled or rejected by the identity provider"
    )
    assert sso_state.get_state("callback-state", "oidc") is None


def test_oidc_state_replay_is_rejected(oidc_client_fixture, monkeypatch):
    client, _config, auth_router, sso_state = oidc_client_fixture
    _seed_callback_state(sso_state)
    monkeypatch.setattr(auth_router.oidc_client, "discover", lambda _issuer: _metadata())
    _mock_http(
        auth_router,
        monkeypatch,
        lambda _request: httpx.Response(200, json={"id_token": "invalid"}),
    )
    params = {"code": "authorization-code", "state": "callback-state"}

    first = client.get("/api/v1/auth/sso/oidc/callback", params=params)
    second = client.get("/api/v1/auth/sso/oidc/callback", params=params)

    assert first.status_code == 401
    assert second.status_code == 400
