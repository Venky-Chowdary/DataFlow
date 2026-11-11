"""CORS on the hosted cloud: exact origins only, no *.up.railway.app pattern.

The old default accepted any ``https://<name>.up.railway.app`` origin with
credentials whenever the API ran on Railway. Anyone can host a site there, so
a stranger's page could make signed-in calls to the API (same class as D337).
The allowlist is now exact: the hosted app's public web origins plus what an
operator names (CORS_ORIGINS / WEB_DOMAIN / CORS_EXTRA_ORIGINS); a pattern
only applies when an operator sets CORS_ORIGIN_REGEX deliberately.
"""

from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

STRANGER = "https://someone-elses-app.up.railway.app"


@pytest.fixture
def hosted(monkeypatch):
    for name in ("CORS_ORIGINS", "CORS_ORIGIN_REGEX", "CORS_EXTRA_ORIGINS",
                 "DATAWRAP_WEB_DOMAIN", "DATAFLOW_WEB_DOMAIN", "RAILWAY_PUBLIC_DOMAIN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "api-service.example")
    return monkeypatch


def _client() -> TestClient:
    """The CORS layer exactly as src/main.py configures it."""
    from services.cors_policy import TenantAwareCORSMiddleware
    from services.platform_config import cors_origin_regex, cors_origins

    app = Starlette(routes=[Route("/ping", lambda request: PlainTextResponse("ok"))])
    app.add_middleware(
        TenantAwareCORSMiddleware,
        allow_origins=cors_origins(),
        allow_origin_regex=cors_origin_regex(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["authorization", "content-type"],
    )
    return TestClient(app)


def _preflight(client: TestClient, origin: str):
    return client.options(
        "/ping",
        headers={"Origin": origin, "Access-Control-Request-Method": "POST"},
    )


def test_a_random_railway_app_gets_no_allow_origin(hosted):
    from services.platform_config import cors_origin_regex

    assert cors_origin_regex() is None
    client = _client()
    for response in (_preflight(client, STRANGER), client.get("/ping", headers={"Origin": STRANGER})):
        assert "access-control-allow-origin" not in response.headers, dict(response.headers)
        assert "access-control-allow-credentials" not in response.headers


@pytest.mark.parametrize("origin", ["https://datawrap.io", "https://www.datawrap.io"])
def test_the_hosted_app_origins_are_allowed_with_credentials(hosted, origin):
    response = _client().get("/ping", headers={"Origin": origin})
    assert response.headers.get("access-control-allow-origin") == origin
    assert response.headers.get("access-control-allow-credentials") == "true"


def test_an_operator_names_an_extra_frontend_exactly(hosted):
    frontend = "https://staging-web.up.railway.app"
    hosted.setenv("CORS_EXTRA_ORIGINS", frontend)
    client = _client()
    assert client.get("/ping", headers={"Origin": frontend}).headers.get(
        "access-control-allow-origin"
    ) == frontend
    # Naming one Railway host does not open the others.
    assert "access-control-allow-origin" not in client.get(
        "/ping", headers={"Origin": STRANGER}
    ).headers


def test_an_operator_set_pattern_still_applies(hosted):
    hosted.setenv("CORS_ORIGIN_REGEX", r"https://[a-z]+\.corp\.example$")
    client = _client()
    assert client.get("/ping", headers={"Origin": "https://tools.corp.example"}).headers.get(
        "access-control-allow-origin"
    ) == "https://tools.corp.example"
    assert "access-control-allow-origin" not in client.get(
        "/ping", headers={"Origin": STRANGER}
    ).headers


def test_lookalike_hosts_never_match(hosted):
    client = _client()
    for origin in ("https://datawrap.io.attacker.example", "http://datawrap.io",
                   "https://evil.up.railway.app.attacker.example"):
        assert "access-control-allow-origin" not in client.get(
            "/ping", headers={"Origin": origin}
        ).headers, origin
