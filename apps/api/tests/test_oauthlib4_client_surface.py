"""oauthlib 4 is the floor for the default API install.

databricks-sql-connector still declares oauthlib<4. Its client only calls
WebApplicationClient methods that oauthlib 4.0.0 still provides. This test
locks that surface so a later connector release can return to the default
install without rediscovering a client break. It does not start an OAuth
server and does not call the revocation endpoint fixed in PYSEC-2026-4113.
"""

from __future__ import annotations

import importlib.metadata

from oauthlib.oauth2 import WebApplicationClient


def test_default_install_resolves_oauthlib_4() -> None:
    version = importlib.metadata.version("oauthlib")
    major = int(version.split(".", 1)[0])
    assert major >= 4, version


def test_web_application_client_matches_the_databricks_call_shape() -> None:
    client = WebApplicationClient("client-id")
    uri, _headers, _body = client.prepare_authorization_request(
        "https://accounts.example/authorize",
        redirect_url="http://localhost:8020",
        scope=["sql"],
        state="state-1",
        code_challenge="challenge",
        code_challenge_method="S256",
    )
    assert uri.startswith("https://accounts.example/authorize?")
    assert "code_challenge=challenge" in uri
    assert "code_challenge_method=S256" in uri
    assert "state=state-1" in uri

    token_body = client.prepare_request_body(
        code="auth-code",
        redirect_uri="http://localhost:8020",
    )
    assert "grant_type=authorization_code" in token_body
    assert "code=auth-code" in token_body

    refresh = client.prepare_refresh_body(
        refresh_token="refresh-1",
        client_id="client-id",
    )
    assert "grant_type=refresh_token" in refresh
    assert "refresh_token=refresh-1" in refresh

    parsed = client.parse_request_uri_response(
        "https://localhost:8020/callback?code=auth-code&state=state-1",
        state="state-1",
    )
    assert parsed["code"] == "auth-code"
    assert parsed["state"] == "state-1"
