from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Callable

from connectors.sdk.declarative.errors import (
    ConnectorAuthError,
    safe_exception_context,
)
from connectors.sdk.declarative.manifest import AuthSpec
from connectors.sdk.oauth import (
    OAuth2Spec,
    ensure_access_token,
    fetch_client_credentials_token,
    refresh_oauth2_token,
)


@dataclass(frozen=True)
class AuthBundle:
    headers: dict[str, str] = field(repr=False)
    params: dict[str, str] = field(repr=False)
    refresh_auth: Callable[[], Mapping[str, str]] | None = None


def build_auth(spec: AuthSpec, config: Mapping[str, Any] | None = None) -> AuthBundle:
    config = config or {}
    credentials = config.get("credentials")
    credentials = credentials if isinstance(credentials, Mapping) else {}

    def value(key: str, manifest_value: Any = "") -> Any:
        return credentials.get(key) or config.get(key) or manifest_value

    if spec.type == "none":
        return AuthBundle({}, {})
    if spec.type == "api_key":
        secret = value("api_key", spec.value)
        if not secret:
            raise ConnectorAuthError("API key is not configured")
        if spec.location == "query":
            return AuthBundle({}, {spec.name: str(secret)})
        return AuthBundle({spec.name: str(secret)}, {})
    if spec.type == "bearer":
        secret = value("access_token", spec.token) or value("api_key")
        if not secret:
            raise ConnectorAuthError("Bearer token is not configured")
        return AuthBundle({"Authorization": f"Bearer {secret}"}, {})
    if spec.type == "basic":
        username = value("username", spec.username)
        password = value("password", spec.password)
        if not username or not password:
            raise ConnectorAuthError("Basic credentials are not configured")
        encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return AuthBundle({"Authorization": f"Basic {encoded}"}, {})

    oauth_spec = OAuth2Spec(
        token_url=spec.token_url,
        client_id=str(value("client_id", spec.client_id)),
        client_secret=str(value("client_secret", spec.client_secret)),
        refresh_token=str(value("refresh_token", spec.refresh_token)),
        access_token=str(value("access_token", spec.access_token)),
        scopes=list(spec.scopes),
        extra_token_params=dict(spec.extra_token_params),
        client_auth_method=spec.client_auth_method,
    )
    token_cache: dict[str, str] = {}
    if spec.type == "oauth2_refresh":
        credential_config = {
            "access_token": oauth_spec.access_token,
            "credentials": {
                "access_token": oauth_spec.access_token,
                "refresh_token": oauth_spec.refresh_token,
                "expires_at": value("expires_at", spec.expires_at),
            },
        }
        try:
            token, _ = ensure_access_token(
                credential_config,
                build_spec=lambda _config: oauth_spec,
            )
        except Exception as exc:
            raise ConnectorAuthError(
                "OAuth2 refresh authentication failed"
                f"{safe_exception_context(exc)}"
            ) from None
        token_cache["access_token"] = token

        def refresh_auth() -> Mapping[str, str]:
            try:
                updated = refresh_oauth2_token(oauth_spec)
            except Exception as exc:
                raise ConnectorAuthError(
                    "OAuth2 refresh authentication failed"
                    f"{safe_exception_context(exc)}"
                ) from None
            oauth_spec.refresh_token = updated.refresh_token
            token_cache["access_token"] = updated.access_token
            return {"Authorization": f"Bearer {updated.access_token}"}

        return AuthBundle(
            {"Authorization": f"Bearer {token_cache['access_token']}"},
            {},
            refresh_auth,
        )

    if spec.type == "oauth2_client_credentials":
        try:
            token_data = fetch_client_credentials_token(oauth_spec)
            token = str(token_data.get("access_token") or "")
            if not token:
                raise ValueError("missing access token")
        except Exception as exc:
            raise ConnectorAuthError(
                "OAuth2 client-credentials authentication failed"
                f"{safe_exception_context(exc)}"
            ) from None

        def refresh_client_credentials() -> Mapping[str, str]:
            try:
                refreshed = fetch_client_credentials_token(oauth_spec, force_refresh=True)
                access_token = str(refreshed.get("access_token") or "")
                if not access_token:
                    raise ValueError("missing access token")
            except Exception as exc:
                raise ConnectorAuthError(
                    "OAuth2 client-credentials authentication failed"
                    f"{safe_exception_context(exc)}"
                ) from None
            return {"Authorization": f"Bearer {access_token}"}

        return AuthBundle({"Authorization": f"Bearer {token}"}, {}, refresh_client_credentials)
    raise ConnectorAuthError("Unsupported authentication mode")
