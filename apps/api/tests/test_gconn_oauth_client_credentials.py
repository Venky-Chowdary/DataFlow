from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from urllib.parse import parse_qs

from connectors.sdk import oauth
from connectors.sdk.oauth import OAuth2Spec, fetch_client_credentials_token
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_client_credentials_post_sends_form_and_caches_until_expiry(
    monkeypatch,
) -> None:
    now = [1_000.0]
    monkeypatch.setattr(oauth, "time", SimpleNamespace(time=lambda: now[0]))
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            responses=[
                FixtureResponse(
                    body={
                        "access_token": "first-token",
                        "token_type": "Bearer",
                        "expires_in": 300,
                    }
                ),
                FixtureResponse(
                    body={
                        "access_token": "second-token",
                        "token_type": "Bearer",
                        "expires_in": 300,
                    }
                ),
            ],
            method="POST",
        )
        spec = OAuth2Spec(
            token_url=f"{fixture.base_url}/token",
            client_id="client-one",
            client_secret="secret-one",
            scopes=["read", "write"],
            client_auth_method="client_secret_post",
        )

        first = fetch_client_credentials_token(spec)
        cached = fetch_client_credentials_token(spec)
        now[0] = 1_241.0
        refreshed = fetch_client_credentials_token(spec)

        assert first["access_token"] == cached["access_token"] == "first-token"
        assert refreshed["access_token"] == "second-token"
        assert len(fixture.request_log) == 2
        fields = parse_qs(fixture.request_log[0].body.decode())
        assert fields["grant_type"] == ["client_credentials"]
        assert fields["client_id"] == ["client-one"]
        assert fields["client_secret"] == ["secret-one"]
        assert fields["scope"] == ["read write"]


def test_client_credentials_basic_sends_credentials_in_authorization_header() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            FixtureResponse(body={"access_token": "basic-token", "expires_in": 3600}),
            method="POST",
        )
        spec = OAuth2Spec(
            token_url=f"{fixture.base_url}/token",
            client_id="basic client",
            client_secret="basic:secret",
            scopes=["records"],
            client_auth_method="client_secret_basic",
        )

        token = fetch_client_credentials_token(spec)

        entry = fixture.request_log[0]
        expected = base64.b64encode(b"basic+client:basic%3Asecret").decode()
        fields = parse_qs(entry.body.decode())
        assert token["access_token"] == "basic-token"
        assert entry.headers["Authorization"] == f"Basic {expected}"
        assert fields["grant_type"] == ["client_credentials"]
        assert fields["scope"] == ["records"]
        assert "client_secret" not in fields


def test_client_credentials_supports_forced_refresh() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            responses=[
                FixtureResponse(body={"access_token": "old-token", "expires_in": 3600}),
                FixtureResponse(body={"access_token": "new-token", "expires_in": 3600}),
            ],
            method="POST",
        )
        spec = OAuth2Spec(
            token_url=f"{fixture.base_url}/token",
            client_id="refresh-client",
            client_secret="refresh-secret",
        )

        first = fetch_client_credentials_token(spec)
        refreshed = fetch_client_credentials_token(spec, force_refresh=True)

        assert first["access_token"] == "old-token"
        assert refreshed["access_token"] == "new-token"
        assert len(fixture.request_log) == 2


def test_concurrent_client_credentials_requests_share_one_token() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/token",
            FixtureResponse(body={"access_token": "shared-token", "expires_in": 3600}),
            method="POST",
        )
        spec = OAuth2Spec(
            token_url=f"{fixture.base_url}/token",
            client_id="parallel-client",
            client_secret="parallel-secret",
        )

        with ThreadPoolExecutor(max_workers=8) as pool:
            tokens = list(pool.map(lambda _: fetch_client_credentials_token(spec), range(8)))

        assert {token["access_token"] for token in tokens} == {"shared-token"}
        assert len(fixture.request_log) == 1
