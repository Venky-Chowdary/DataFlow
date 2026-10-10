"""MCP OAuth resource-server configuration and challenge helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services import oidc_client
from services.brand_env import getenv_brand
from services import integrations_store
from services.platform_config import public_url
from services.rbac import all_permissions


@dataclass(frozen=True)
class McpOAuthConfig:
    issuer: str
    audience: str
    metadata_url: str

    @property
    def enabled(self) -> bool:
        return bool(self.issuer)


def get_mcp_oauth_config() -> McpOAuthConfig:
    """Resolve the MCP protected-resource issuer and resource audience."""
    issuer = (getenv_brand("MCP_OAUTH_ISSUER", "") or "").strip()
    if not issuer:
        oidc = integrations_store.get_sso_configs().get("oidc") or {}
        if oidc.get("enabled"):
            issuer = str(oidc.get("issuer") or "").strip()

    base_url = public_url().rstrip("/")
    audience = (getenv_brand("MCP_OAUTH_AUDIENCE", "") or "").strip()
    if not audience:
        audience = f"{base_url}/api/v1/mcp"
    metadata_url = f"{base_url}/.well-known/oauth-protected-resource/api/v1/mcp"
    return McpOAuthConfig(
        issuer=issuer.rstrip("/"),
        audience=audience,
        metadata_url=metadata_url,
    )


def bearer_challenge() -> str:
    """Build the RFC 9728 resource-metadata challenge value."""
    return f'Bearer resource_metadata="{get_mcp_oauth_config().metadata_url}"'


def principal_from_access_token(
    token: str,
    *,
    user_lookup: Any = None,
) -> dict[str, Any] | None:
    """Validate an MCP access token and build a scope-bound principal."""
    config = get_mcp_oauth_config()
    if not config.enabled:
        return None
    try:
        metadata = oidc_client.discover(config.issuer)
        claims = oidc_client.validate_access_token(
            token,
            metadata=metadata,
            audience=config.audience,
            algorithms=oidc_client.allowed_algorithms("oidc", metadata),
            leeway=oidc_client.clock_skew_leeway(),
        )
        email = oidc_client.email_from_claims(claims, sso_type="oidc")
    except oidc_client.OidcError:
        return None
    from services import user_store

    stored = user_store.get_user(email)
    if stored and stored.get("status") == "disabled":
        return None
    from fastapi import HTTPException
    from src.routers.auth_router import _require_sso_authorization

    try:
        _require_sso_authorization(email)
    except HTTPException:
        return None
    if user_lookup is None:
        from src.services import auth_service

        user_lookup = auth_service.lookup_user
    user = user_lookup(email) or {"email": email, "role": "viewer"}
    raw = claims.get("scope")
    scopes = raw.split() if isinstance(raw, str) else raw if isinstance(raw, list) else []
    known = set(all_permissions())
    recognized = sorted(
        {
            scope.removeprefix("datawrap:")
            for scope in scopes
            if isinstance(scope, str) and scope.removeprefix("datawrap:") in known
        }
    )
    principal = dict(user)
    principal.update({"email": email, "auth_kind": "oauth", "scopes": recognized})
    return principal
