"""OAuth2 helpers for Datawrap Connector CDK — token refresh with config write-back."""

from __future__ import annotations

import base64
import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import quote_plus, urlencode

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
    client_auth_method: str = "client_secret_post"


_CLIENT_CREDENTIALS_CACHE: dict[tuple[Any, ...], OAuth2Tokens] = {}
_CLIENT_CREDENTIALS_LOCKS: dict[tuple[Any, ...], threading.Lock] = {}
_CLIENT_CREDENTIALS_LOCKS_GUARD = threading.Lock()


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


def fetch_client_credentials_token(
    spec: OAuth2Spec, *, timeout: int = 30, force_refresh: bool = False
) -> dict[str, Any]:
    """Fetch or reuse a token for the OAuth2 client-credentials grant."""
    auth_method = spec.client_auth_method.strip().lower()
    if auth_method not in {"client_secret_post", "client_secret_basic"}:
        raise ValueError(
            "OAuth2 client_auth_method must be client_secret_post or client_secret_basic"
        )
    if not spec.client_id or not spec.client_secret:
        raise ValueError("OAuth2 client credentials require client_id and client_secret")

    secret_digest = hashlib.sha256(spec.client_secret.encode("utf-8")).hexdigest()
    cache_key = (
        spec.token_url,
        spec.client_id,
        secret_digest,
        auth_method,
        tuple(spec.scopes),
        tuple(sorted(spec.extra_token_params.items())),
        spec.access_token_path,
        spec.expires_in_path,
    )
    with _CLIENT_CREDENTIALS_LOCKS_GUARD:
        lock = _CLIENT_CREDENTIALS_LOCKS.setdefault(cache_key, threading.Lock())

    with lock:
        cached = _CLIENT_CREDENTIALS_CACHE.get(cache_key)
        if (
            not force_refresh
            and cached is not None
            and not cached.expired(skew_seconds=60)
        ):
            return dict(cached.raw)

        body = dict(spec.extra_token_params)
        body["grant_type"] = "client_credentials"
        if spec.scopes:
            body["scope"] = " ".join(spec.scopes)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        if auth_method == "client_secret_post":
            body["client_id"] = spec.client_id
            body["client_secret"] = spec.client_secret
        else:
            encoded_client = quote_plus(spec.client_id, safe="")
            encoded_secret = quote_plus(spec.client_secret, safe="")
            credentials = base64.b64encode(
                f"{encoded_client}:{encoded_secret}".encode("utf-8")
            ).decode("ascii")
            headers["Authorization"] = f"Basic {credentials}"

        response = requests.post(
            spec.token_url,
            data=urlencode(body),
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json() if response.content else {}
        if not isinstance(data, dict):
            raise ValueError("OAuth2 token response must be a JSON object")
        access_token = str(_dig(data, spec.access_token_path) or "")
        if not access_token:
            raise ValueError("OAuth2 token response missing access_token")
        expires_in = _dig(data, spec.expires_in_path)
        expires_at = 0.0
        if expires_in is not None:
            try:
                expires_at = time.time() + float(expires_in)
            except (TypeError, ValueError):
                expires_at = 0.0
        token_type = str(data.get("token_type") or "Bearer")
        result: dict[str, Any] = {
            "access_token": access_token,
            "token_type": token_type,
            "expires_at": expires_at,
        }
        cached_tokens = OAuth2Tokens(
            access_token=access_token,
            expires_at=expires_at,
            token_type=token_type,
            raw=result,
        )
        _CLIENT_CREDENTIALS_CACHE[cache_key] = cached_tokens
        return dict(result)


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
