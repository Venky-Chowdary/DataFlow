from __future__ import annotations

import base64
import logging
from unittest.mock import Mock

import pytest
import requests

import connectors.sdk.declarative.auth as auth_module
from connectors.sdk.declarative.auth import build_auth
from connectors.sdk.declarative.errors import ConnectorAuthError
from connectors.sdk.declarative.manifest import AuthSpec
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_static_auth_modes_build_headers_and_query_parameters() -> None:
    header = build_auth(
        AuthSpec(type="api_key", name="X-API-Key"),
        {"credentials": {"api_key": "header-secret"}},
    )
    query = build_auth(
        AuthSpec(type="api_key", location="query", name="api_key"),
        {"credentials": {"api_key": "query-secret"}},
    )
    bearer = build_auth(
        AuthSpec(type="bearer"),
        {"credentials": {"access_token": "bearer-secret"}},
    )
    basic = build_auth(
        AuthSpec(type="basic"),
        {"credentials": {"username": "user", "password": "pass"}},
    )

    assert header.headers == {"X-API-Key": "header-secret"}
    assert query.params == {"api_key": "query-secret"}
    assert bearer.headers == {"Authorization": "Bearer bearer-secret"}
    expected = base64.b64encode(b"user:pass").decode("ascii")
    assert basic.headers == {"Authorization": f"Basic {expected}"}


def test_auth_secrets_are_not_in_representations() -> None:
    secret = "do-not-show"
    spec = AuthSpec(type="api_key", value=secret)
    bundle = build_auth(spec)

    assert secret not in repr(spec)
    assert secret not in repr(bundle)


def test_oauth_refresh_and_client_credentials_refresh_tokens_on_demand() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            responses=[
                FixtureResponse(body={"access_token": "refresh-1", "expires_in": 3600}),
                FixtureResponse(body={"access_token": "refresh-2", "expires_in": 3600}),
                FixtureResponse(body={"access_token": "client-1", "expires_in": 3600}),
                FixtureResponse(body={"access_token": "client-2", "expires_in": 3600}),
            ],
            method="POST",
        )
        refresh = build_auth(
            AuthSpec(
                type="oauth2_refresh",
                token_url=f"{fixture.base_url}/token",
                client_id="client",
                client_secret="secret",
                refresh_token="refresh",
            )
        )
        assert refresh.headers == {"Authorization": "Bearer refresh-1"}
        assert refresh.refresh_auth is not None
        assert refresh.refresh_auth() == {"Authorization": "Bearer refresh-2"}

        client_credentials = build_auth(
            AuthSpec(
                type="oauth2_client_credentials",
                token_url=f"{fixture.base_url}/token",
                client_id="client",
                client_secret="secret",
            )
        )
        assert client_credentials.headers == {"Authorization": "Bearer client-1"}
        assert client_credentials.refresh_auth is not None
        assert client_credentials.refresh_auth() == {
            "Authorization": "Bearer client-2"
        }


def test_oauth_auth_error_includes_safe_cause_without_leaking_secret(
    monkeypatch, caplog
) -> None:
    secret = "planted-oauth-response-secret"
    response = requests.Response()
    response.status_code = 502
    response.headers["X-Request-ID"] = "oauth-failure-42"
    failure = requests.HTTPError(secret, response=response)
    monkeypatch.setattr(auth_module, "ensure_access_token", Mock(side_effect=failure))

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ConnectorAuthError) as raised:
            build_auth(
                AuthSpec(
                    type="oauth2_refresh",
                    token_url="https://auth.example.test/token",
                    client_id="client",
                    client_secret="client-secret",
                    refresh_token="refresh-token",
                )
            )

    message = str(raised.value)
    assert "HTTPError" in message
    assert "502" in message
    assert "oauth-failure-42" in message
    assert secret not in message
    assert secret not in caplog.text
