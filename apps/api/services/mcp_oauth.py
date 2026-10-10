"""MCP OAuth resource-server configuration and challenge helpers."""

from __future__ import annotations

from dataclasses import dataclass

from services.brand_env import getenv_brand
from services import integrations_store
from services.platform_config import public_url


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
