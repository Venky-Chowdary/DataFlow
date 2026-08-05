"""SSO/SAML/OIDC router tests."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def _install_fake_onelogin(mock_auth_cls: MagicMock) -> None:
    """python3-saml is optional — inject a stub package so the router can import."""
    onelogin = types.ModuleType("onelogin")
    saml2 = types.ModuleType("onelogin.saml2")
    auth = types.ModuleType("onelogin.saml2.auth")
    auth.OneLogin_Saml2_Auth = mock_auth_cls
    sys.modules["onelogin"] = onelogin
    sys.modules["onelogin.saml2"] = saml2
    sys.modules["onelogin.saml2.auth"] = auth


@pytest.fixture
def client(monkeypatch, tmp_path):
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from src.main import app
    from src.routers import auth_router
    from services import integrations_store

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_ENABLE_DOCS", "0")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(auth_router, "get_and_pop", lambda state, sso_type: bool(state))

    return TestClient(app, base_url="https://testserver")


def _saml_cfg():
    return {
        "sso": {
            "saml": {
                "enabled": True,
                "entity_id": "https://idp.example.com",
                "sso_url": "https://idp.example.com/saml/sso",
                "x509_cert": "MIIB...",
                "email_attribute": "email",
            },
            "oidc": {"enabled": False},
            "azure_ad": {"enabled": False},
        },
        "ai_providers": {},
        "api_keys": [],
    }


def test_sso_saml_start_redirect(client, monkeypatch, tmp_path):
    from services import integrations_store

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))

    mock_auth_cls = MagicMock()
    mock_auth = MagicMock()
    mock_auth.login.return_value = "https://idp.example.com/saml/sso?SAMLRequest=xyz&RelayState=state-token"
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls)

    response = client.get("/api/v1/auth/sso/saml/start", follow_redirects=False)

    assert response.status_code in (302, 307)
    assert "idp.example.com" in response.headers["location"]


def test_sso_saml_callback_creates_token(client, monkeypatch, tmp_path):
    from services import integrations_store

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))

    mock_auth_cls = MagicMock()
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = []
    mock_auth.is_authenticated.return_value = True
    mock_auth.get_nameid.return_value = "saml-user@example.com"
    mock_auth.get_attributes.return_value = {}
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls)

    response = client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": "base64-saml-response", "RelayState": "state-token"},
        follow_redirects=False,
    )

    from urllib.parse import unquote

    assert response.status_code in (302, 307)
    location = unquote(response.headers["location"])
    assert "sso_token=" in location
    assert "saml-user@example.com" in location


def test_sso_saml_callback_rejects_invalid_response(client, monkeypatch, tmp_path):
    from services import integrations_store

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))

    mock_auth_cls = MagicMock()
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = ["signature_invalid"]
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls)

    response = client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": "bad", "RelayState": "state-token"},
    )

    assert response.status_code == 401


def test_oidc_callback_requires_code_and_state(client):
    response = client.get("/api/v1/auth/sso/oidc/callback?code=&state=")
    # Missing state pops false; returns invalid SSO state before code check.
    assert response.status_code == 400
