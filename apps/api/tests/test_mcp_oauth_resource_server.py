"""OAuth resource-server coverage for the MCP control surface."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

ISSUER = "https://issuer.example.test"
AUDIENCE = "https://api.example.test/api/v1/mcp"
JWKS_URI = f"{ISSUER}/jwks"
METADATA_URL = (
    "https://api.example.test/.well-known/"
    "oauth-protected-resource/api/v1/mcp"
)


@pytest.fixture
def mcp_oauth_client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "x" * 64)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)
    monkeypatch.setenv("DATAFLOW_PUBLIC_URL", "https://api.example.test")
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")

    from services import integrations_store, oidc_client, sso_authorization, user_store
    from src.middleware import auth_middleware
    from src.main import app
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(user_store, "get_user", lambda _email: None)
    fake_user_lookup = lambda email: {"email": email, "role": "editor"}
    monkeypatch.setattr(auth_middleware, "lookup_user", fake_user_lookup)
    monkeypatch.setattr(sso_authorization, "lookup_user", fake_user_lookup)
    monkeypatch.setattr(
        integrations_store,
        "get_sso_configs",
        lambda: {
            "oidc": {
                "enabled": True,
                "issuer": ISSUER,
            }
        },
    )

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk.update({"kid": "mcp-key", "use": "sig", "alg": "RS256"})
    discovery = {
        "issuer": ISSUER,
        "authorization_endpoint": f"{ISSUER}/authorize",
        "token_endpoint": f"{ISSUER}/token",
        "jwks_uri": JWKS_URI,
        "id_token_signing_alg_values_supported": ["RS256"],
    }

    def handle_provider_request(request):
        if request.url.path == "/.well-known/openid-configuration":
            return httpx.Response(200, json=discovery)
        if request.url.path == "/jwks":
            return httpx.Response(200, json={"keys": [jwk]})
        return httpx.Response(404)

    monkeypatch.setattr(
        oidc_client,
        "_http_client",
        lambda: httpx.Client(
            transport=httpx.MockTransport(handle_provider_request),
            timeout=10.0,
        ),
    )
    oidc_client.reset_caches()

    def token(**overrides):
        algorithm = overrides.pop("_algorithm", "RS256")
        kid = overrides.pop("_kid", "mcp-key")
        signed = overrides.pop("_signed", True)
        now = datetime.now(timezone.utc)
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "subject-1",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(minutes=5)).timestamp()),
            "email": "oauth@example.test",
            "email_verified": True,
        }
        claims.update(overrides)
        return jwt.encode(
            claims,
            private_key if signed else "",
            algorithm=algorithm,
            headers={"kid": kid},
        )

    with TestClient(app, base_url="https://api.example.test") as client:
        yield client, token, private_key

    oidc_client.reset_caches()


def _validate_access_token(token: str):
    from services import oidc_client

    metadata = oidc_client.discover(ISSUER)
    return oidc_client.validate_access_token(
        token,
        metadata=metadata,
        audience=AUDIENCE,
        algorithms=oidc_client.allowed_algorithms("oidc", metadata),
        leeway=0,
    )


def test_valid_rs256_access_token_uses_fake_idp_jwks(mcp_oauth_client):
    _client, make_token, _private_key = mcp_oauth_client

    claims = _validate_access_token(make_token())

    assert claims["sub"] == "subject-1"
    assert claims["email"] == "oauth@example.test"


@pytest.mark.parametrize(
    ("claims", "algorithm", "kid", "signed", "reason"),
    [
        ({"aud": "https://other.example/mcp"}, "RS256", "mcp-key", True, "bad_audience"),
        (
            {
                "exp": int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp())
            },
            "RS256",
            "mcp-key",
            True,
            "expired",
        ),
        ({"iss": "https://other.example"}, "RS256", "mcp-key", True, "bad_issuer"),
        ({}, "none", "mcp-key", False, "alg_not_allowed"),
        ({}, "RS256", "unknown-key", True, "unknown_kid"),
    ],
)
def test_invalid_access_tokens_are_rejected(
    mcp_oauth_client,
    claims,
    algorithm,
    kid,
    signed,
    reason,
):
    _client, make_token, _private_key = mcp_oauth_client
    token = make_token(
        **claims,
        _algorithm=algorithm,
        _kid=kid,
        _signed=signed,
    )

    from services import oidc_client

    with pytest.raises(oidc_client.OidcTokenInvalid) as exc:
        _validate_access_token(token)
    assert exc.value.reason == reason


def test_oauth_scopes_restrict_tool_permissions(mcp_oauth_client):
    client, make_token, _private_key = mcp_oauth_client
    response = client.post(
        "/api/v1/mcp",
        headers={"Authorization": f"Bearer {make_token(scope='connector.read')}"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "start_transfer", "arguments": {}},
        },
    )

    assert response.status_code == 200
    result = response.json()["result"]
    assert result["isError"] is True
    assert result["content"][0]["text"].startswith("Your role")


def test_oauth_without_dataflow_permission_scopes_has_no_tool_permissions(mcp_oauth_client):
    client, make_token, _private_key = mcp_oauth_client
    response = client.post(
        "/api/v1/mcp",
        headers={"Authorization": f"Bearer {make_token(scope='openid email')}"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_connectors", "arguments": {}},
        },
    )

    assert response.status_code == 200
    assert response.json()["result"]["isError"] is True
    assert response.json()["result"]["content"][0]["text"].startswith("Your role")


def test_oauth_dataflow_scopes_grant_only_the_named_permission(mcp_oauth_client):
    client, make_token, _private_key = mcp_oauth_client
    headers = {"Authorization": f"Bearer {make_token(scope='datawrap:job.read')}"}
    read = client.post(
        "/api/v1/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_jobs", "arguments": {}},
        },
    )
    mutate = client.post(
        "/api/v1/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "start_transfer", "arguments": {}},
        },
    )

    assert read.status_code == 200
    assert read.json()["result"]["isError"] is False
    assert mutate.status_code == 200
    assert mutate.json()["result"]["isError"] is True
    assert mutate.json()["result"]["content"][0]["text"].startswith("Your role")


def test_oauth_refuses_users_not_authorized_for_sso(mcp_oauth_client, monkeypatch):
    from src.middleware import auth_middleware
    from services import sso_authorization

    client, make_token, _private_key = mcp_oauth_client
    monkeypatch.setattr(auth_middleware, "lookup_user", lambda _email: None)
    monkeypatch.setattr(sso_authorization, "lookup_user", lambda _email: None)
    response = client.post(
        "/api/v1/mcp",
        headers={"Authorization": f"Bearer {make_token(scope='job.read')}"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_jobs", "arguments": {}},
        },
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == (
        f'Bearer resource_metadata="{METADATA_URL}"'
    )


def test_rejected_access_token_is_not_logged(mcp_oauth_client, monkeypatch, caplog):
    import logging

    from services import mcp_oauth, oidc_client

    token = "highly-sensitive-access-token"
    monkeypatch.setattr(
        mcp_oauth,
        "get_mcp_oauth_config",
        lambda: mcp_oauth.McpOAuthConfig(
            issuer=ISSUER,
            audience=AUDIENCE,
            metadata_url=METADATA_URL,
        ),
    )
    monkeypatch.setattr(oidc_client, "discover", lambda _issuer: {})
    monkeypatch.setattr(oidc_client, "allowed_algorithms", lambda *_args: ["RS256"])

    def reject_token(*_args, **_kwargs):
        raise oidc_client.OidcTokenInvalid("expired")

    monkeypatch.setattr(oidc_client, "validate_access_token", reject_token)
    caplog.set_level(logging.INFO, logger="services.mcp_oauth")

    assert mcp_oauth.principal_from_access_token(token) is None
    assert "reason=expired" in caplog.text
    assert token not in caplog.text


def test_oauth_token_is_not_accepted_outside_mcp(mcp_oauth_client):
    client, make_token, _private_key = mcp_oauth_client

    response = client.get(
        "/api/v1/workspace/settings",
        headers={"Authorization": f"Bearer {make_token()}"},
    )

    assert response.status_code == 401


def test_oauth_disabled_user_is_rejected(mcp_oauth_client, monkeypatch):
    client, make_token, _private_key = mcp_oauth_client
    from services import user_store

    monkeypatch.setattr(
        user_store,
        "get_user",
        lambda email: {"email": email, "status": "disabled"},
    )
    response = client.post(
        "/api/v1/mcp",
        headers={"Authorization": f"Bearer {make_token()}"},
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_connectors", "arguments": {}},
        },
    )

    assert response.status_code == 401


def test_protected_resource_metadata_shape(mcp_oauth_client):
    client, _make_token, _private_key = mcp_oauth_client
    from services.rbac import all_permissions

    expected = {
        "resource": AUDIENCE,
        "authorization_servers": [ISSUER],
        "bearer_methods_supported": ["header"],
        "scopes_supported": sorted(all_permissions()),
    }
    for path in (
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/api/v1/mcp",
    ):
        response = client.get(path)
        assert response.status_code == 200
        assert response.json() == expected


def test_protected_resource_metadata_is_hidden_when_oauth_is_off(
    mcp_oauth_client,
    monkeypatch,
):
    client, _make_token, _private_key = mcp_oauth_client
    from services import integrations_store

    monkeypatch.setattr(
        integrations_store,
        "get_sso_configs",
        lambda: {"oidc": {"enabled": False, "issuer": ""}},
    )
    for path in (
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/api/v1/mcp",
    ):
        assert client.get(path).status_code == 404


def test_unauthenticated_tools_call_includes_bearer_challenge(mcp_oauth_client):
    client, _make_token, _private_key = mcp_oauth_client

    response = client.post(
        "/api/v1/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_connectors", "arguments": {}},
        },
    )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == -32001
    assert response.headers["www-authenticate"] == (
        f'Bearer resource_metadata="{METADATA_URL}"'
    )


def test_unauthenticated_rest_tools_call_includes_bearer_challenge(mcp_oauth_client):
    client, _make_token, _private_key = mcp_oauth_client

    response = client.post(
        "/api/v1/mcp/tools/call",
        json={"name": "list_connectors", "arguments": {}},
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == (
        f'Bearer resource_metadata="{METADATA_URL}"'
    )
