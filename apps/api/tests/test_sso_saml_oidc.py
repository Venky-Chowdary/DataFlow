"""SSO/SAML/OIDC router tests."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


def _install_fake_onelogin(mock_auth_cls: MagicMock, monkeypatch) -> None:
    """python3-saml is optional — inject a stub package so the router can import."""
    onelogin = types.ModuleType("onelogin")
    saml2 = types.ModuleType("onelogin.saml2")
    auth = types.ModuleType("onelogin.saml2.auth")
    auth.OneLogin_Saml2_Auth = mock_auth_cls
    monkeypatch.setitem(sys.modules, "onelogin", onelogin)
    monkeypatch.setitem(sys.modules, "onelogin.saml2", saml2)
    monkeypatch.setitem(sys.modules, "onelogin.saml2.auth", auth)


@pytest.fixture
def client(monkeypatch, tmp_path):
    import os

    monkeypatch.syspath_prepend(os.path.dirname(os.path.dirname(__file__)))

    from src.main import app
    from services import sso_state
    from services import integrations_store

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_ENABLE_DOCS", "0")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    monkeypatch.setattr(sso_state, "STATE_PATH", tmp_path / "sso_state.json")
    monkeypatch.setattr(sso_state, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")

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
    mock_auth.get_last_request_id.return_value = "request-id"
    mock_auth.login.side_effect = lambda return_to: (
        "https://idp.example.com/saml/sso?SAMLRequest=xyz&RelayState=" + return_to
    )
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls, monkeypatch)

    response = client.get("/api/v1/auth/sso/saml/start", follow_redirects=False)

    assert response.status_code in (302, 307)
    assert "idp.example.com" in response.headers["location"]
    relay = mock_auth.login.call_args.kwargs["return_to"]
    from services.sso_state import get_state

    assert get_state(relay, "saml")["extra"] == {"request_id": "request-id"}
    request_dict = mock_auth_cls.call_args.args[0]
    assert request_dict["http_host"] == "testserver"
    assert request_dict["server_port"] == 443
    assert request_dict["script_name"] == "/api/v1/auth/sso/saml/callback"
    assert request_dict["request_uri"] == "/api/v1/auth/sso/saml/callback"
    security = mock_auth_cls.call_args.args[1]["security"]
    assert security["relaxDestinationValidation"] is False
    assert security["destinationStrictlyMatches"] is True
    assert security["rejectUnsolicitedResponsesWithInResponseTo"] is True
    assert security["rejectDeprecatedAlgorithm"] is True


def test_sso_saml_callback_creates_token(client, monkeypatch, tmp_path):
    from services import integrations_store
    from services import sso_state

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))
    sso_state.set_state("state-token", "saml", {"request_id": "request-id"})

    mock_auth_cls = MagicMock()
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = []
    mock_auth.is_authenticated.return_value = True
    mock_auth.get_nameid.return_value = "saml-user@example.com"
    mock_auth.get_attributes.return_value = {}
    mock_auth.get_last_assertion_id.return_value = "assertion-id"
    mock_auth.get_last_assertion_not_on_or_after.return_value = None
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls, monkeypatch)

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
    mock_auth.process_response.assert_called_once_with(request_id="request-id")


def test_sso_saml_callback_rejects_invalid_response(client, monkeypatch, tmp_path):
    from services import integrations_store
    from services import sso_state

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))
    sso_state.set_state("state-token", "saml", {"request_id": "request-id"})

    mock_auth_cls = MagicMock()
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = ["signature_invalid"]
    mock_auth.get_last_error_reason.return_value = "signature validation failed"
    mock_auth_cls.return_value = mock_auth
    _install_fake_onelogin(mock_auth_cls, monkeypatch)

    response = client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": "bad", "RelayState": "state-token"},
    )

    assert response.status_code == 401
    assert "signature validation failed" not in response.text
    mock_auth.process_response.assert_called_once_with(request_id="request-id")


def test_sso_saml_callback_rejects_state_without_request_id(client, monkeypatch):
    from services import integrations_store, sso_state

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))
    sso_state.set_state("state-no-request", "saml", {})
    response = client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": "base64-saml-response", "RelayState": "state-no-request"},
    )
    assert response.status_code == 400


def test_sso_saml_callback_requires_assertion_id(client, monkeypatch):
    from services import integrations_store, sso_state

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))
    sso_state.set_state("state-no-assertion", "saml", {"request_id": "request-id"})
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = []
    mock_auth.get_last_assertion_id.return_value = None
    mock_auth_cls = MagicMock(return_value=mock_auth)
    _install_fake_onelogin(mock_auth_cls, monkeypatch)
    response = client.post(
        "/api/v1/auth/sso/saml/callback",
        data={"SAMLResponse": "response", "RelayState": "state-no-assertion"},
    )
    assert response.status_code == 401
    assert "assertion_id_missing" in response.json()["detail"]
    mock_auth.process_response.assert_called_once_with(request_id="request-id")


def test_sso_saml_callback_rejects_replayed_assertion(client, monkeypatch):
    from services import integrations_store, sso_state

    integrations_store.STORE_PATH.write_text(integrations_store.json.dumps(_saml_cfg()))
    mock_auth = MagicMock()
    mock_auth.get_errors.return_value = []
    mock_auth.is_authenticated.return_value = True
    mock_auth.get_nameid.return_value = "saml-user@example.com"
    mock_auth.get_attributes.return_value = {}
    mock_auth.get_last_assertion_id.return_value = "same-assertion"
    mock_auth.get_last_assertion_not_on_or_after.return_value = None
    mock_auth_cls = MagicMock(return_value=mock_auth)
    _install_fake_onelogin(mock_auth_cls, monkeypatch)

    statuses = []
    for relay, request_id in (
        ("first-relay", "first-request"),
        ("second-relay", "second-request"),
    ):
        sso_state.set_state(relay, "saml", {"request_id": request_id})
        response = client.post(
            "/api/v1/auth/sso/saml/callback",
            data={"SAMLResponse": "same-response", "RelayState": relay},
            follow_redirects=False,
        )
        statuses.append(response.status_code)

    assert statuses == [302, 401]
    assert "assertion_replay" in response.json()["detail"]


def test_oidc_callback_requires_code_and_state(client):
    response = client.get("/api/v1/auth/sso/oidc/callback?code=&state=")
    # Missing state pops false; returns invalid SSO state before code check.
    assert response.status_code == 400
