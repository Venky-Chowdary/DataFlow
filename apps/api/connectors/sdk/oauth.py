"""OAuth2 helpers for DataFlow Connector CDK — token refresh with config write-back."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlencode

import requests


@dataclass
class OAuth2Tokens:
    access_token: str
    refresh_token: str = ""
    expires_at: float = 0.0  # epoch seconds; 0 = unknown/never
    token_type: str = "Bearer"
    raw: dict[str, Any] = field(default_factory=dict)

    def expired(self, *, skew_seconds: int = 60) -> bool:
        if not self.expires_at:
            return False
        return time.time() >= (self.expires_at - skew_seconds)


@dataclass
class OAuth2Spec:
    token_url: str
    client_id: str
    client_secret: str
    refresh_token: str = ""
    access_token: str = ""
    scopes: list[str] = field(default_factory=list)
    extra_token_params: dict[str, str] = field(default_factory=dict)
    access_token_path: str = "access_token"
    refresh_token_path: str = "refresh_token"
    expires_in_path: str = "expires_in"


def _dig(data: dict[str, Any], path: str) -> Any:
    cur: Any = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def refresh_oauth2_token(spec: OAuth2Spec, *, timeout: int = 30) -> OAuth2Tokens:
    """Exchange refresh_token for a new access_token (OAuth2 refresh_token grant)."""
    if not spec.refresh_token:
        raise ValueError("OAuth2 refresh requires refresh_token")
    body = {
        "grant_type": "refresh_token",
        "refresh_token": spec.refresh_token,
        "client_id": spec.client_id,
        "client_secret": spec.client_secret,
        **spec.extra_token_params,
    }
    if spec.scopes:
        body["scope"] = " ".join(spec.scopes)
    resp = requests.post(
        spec.token_url,
        data=urlencode(body),
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    access = str(_dig(data, spec.access_token_path) or "")
    if not access:
        raise ValueError("OAuth2 token response missing access_token")
    refresh = str(_dig(data, spec.refresh_token_path) or spec.refresh_token)
    expires_in = _dig(data, spec.expires_in_path)
    expires_at = 0.0
    if expires_in is not None:
        try:
            expires_at = time.time() + float(expires_in)
        except (TypeError, ValueError):
            expires_at = 0.0
    return OAuth2Tokens(
        access_token=access,
        refresh_token=refresh,
        expires_at=expires_at,
        token_type=str(data.get("token_type") or "Bearer"),
        raw=dict(data) if isinstance(data, dict) else {},
    )


def apply_tokens_to_config(config: dict[str, Any], tokens: OAuth2Tokens) -> dict[str, Any]:
    """Write refreshed tokens back into connector config (Airbyte-style token updater)."""
    out = dict(config)
    creds = dict(out.get("credentials") or {})
    creds["access_token"] = tokens.access_token
    if tokens.refresh_token:
        creds["refresh_token"] = tokens.refresh_token
    if tokens.expires_at:
        creds["expires_at"] = tokens.expires_at
    out["credentials"] = creds
    out["api_key"] = tokens.access_token
    out["access_token"] = tokens.access_token
    return out


def ensure_access_token(
    config: dict[str, Any],
    *,
    build_spec: Callable[[dict[str, Any]], OAuth2Spec | None],
) -> tuple[str, dict[str, Any]]:
    """Return (access_token, maybe_updated_config), refreshing if expired."""
    creds = config.get("credentials") or {}
    access = (
        config.get("access_token")
        or config.get("api_key")
        or creds.get("access_token")
        or ""
    )
    expires_at = float(creds.get("expires_at") or config.get("expires_at") or 0)
    tokens = OAuth2Tokens(
        access_token=str(access),
        refresh_token=str(creds.get("refresh_token") or config.get("refresh_token") or ""),
        expires_at=expires_at,
    )
    if access and not tokens.expired():
        return str(access), config
    spec = build_spec(config)
    if spec is None or not spec.refresh_token:
        if access:
            return str(access), config
        raise ValueError("No access_token and OAuth2 refresh is not configured")
    if not spec.access_token and access:
        spec.access_token = str(access)
    new_tokens = refresh_oauth2_token(spec)
    updated = apply_tokens_to_config(config, new_tokens)
    return new_tokens.access_token, updated
