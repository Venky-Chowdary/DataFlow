"""Salesforce access tokens that a schedule can keep using (CLM-13).

The connector used to take only a pasted session token. Salesforce session
tokens expire (two hours by default) and nothing refreshed them, so a
scheduled Salesforce run failed once the token aged out, while the tile
said Certified.

Two ways in now:

* **OAuth client credentials** (a Connected App with the client-credentials
  flow enabled): Username holds the Consumer Key, Password the Consumer
  Secret, and no token is pasted. Each run mints a fresh access token from
  ``{instance}/services/oauth2/token``; a token is reused for at most
  ``_TOKEN_TTL_SECONDS`` (30 minutes, well inside Salesforce's session
  timeout), so a run never starts on an expired one.
* **Pasted session token** (API key or connection string): used as is, never
  refreshed. ``session_token_schedule_warning`` says so when a schedule is
  saved on it.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from connectors.saas_common import base_url, token

DEFAULT_HOST = "login.salesforce.com"
_TOKEN_TTL_SECONDS = 30 * 60
_cache: dict[tuple[str, str], tuple[str, float]] = {}
_lock = threading.Lock()

CLIENT_CREDENTIALS = "client_credentials"
SESSION_TOKEN = "session_token"  # nosec B105 - an auth mode name, not a secret


def auth_mode(cfg: dict[str, Any]) -> str:
    """Which way this connector authenticates ('' when it has no credential)."""
    chosen = str(cfg.get("auth_mode") or "").strip().lower()
    has_pair = bool(str(cfg.get("username") or "").strip() and str(cfg.get("password") or "").strip())
    if chosen == "user_pass" and has_pair:
        return CLIENT_CREDENTIALS
    if str(cfg.get("api_key") or "").strip() or str(cfg.get("connection_string") or "").strip():
        return SESSION_TOKEN
    if str(cfg.get("username") or "").strip() and str(cfg.get("password") or "").strip():
        return CLIENT_CREDENTIALS
    if str(cfg.get("password") or "").strip():
        return SESSION_TOKEN
    return ""


def _mint(instance: str, client_id: str, client_secret: str) -> str:
    import requests

    from services.egress_guard import assert_connector_dial_allowed

    url = f"{instance}/services/oauth2/token"
    assert_connector_dial_allowed(url, point="http_client")
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Accept": "application/json", "User-Agent": "Datawrap/1.0"},
        timeout=20,
        allow_redirects=False,
    )
    if resp.status_code != 200:
        raise ValueError(
            "Salesforce refused the client-credentials token request "
            f"(HTTP {resp.status_code}). Check the Connected App's Consumer Key / "
            "Secret and that the client-credentials flow is enabled with a run-as user."
        )
    access = str((resp.json() or {}).get("access_token") or "")
    if not access:
        raise ValueError("Salesforce token response carried no access_token")
    return access


def access_token(cfg: dict[str, Any], *, force_refresh: bool = False) -> str:
    """The bearer token for this connector: minted (client credentials) or pasted."""
    mode = auth_mode(cfg)
    if mode != CLIENT_CREDENTIALS:
        return token(
            str(cfg.get("api_key") or ""),
            str(cfg.get("connection_string") or ""),
            "",
            str(cfg.get("password") or ""),
        )
    instance = base_url(str(cfg.get("host") or ""), DEFAULT_HOST)
    client_id = str(cfg.get("username") or "").strip()
    key = (instance, client_id)
    now = time.monotonic()
    with _lock:
        cached = _cache.get(key)
        if cached and not force_refresh and cached[1] > now:
            return cached[0]
    minted = _mint(instance, client_id, str(cfg.get("password") or "").strip())
    with _lock:
        _cache[key] = (minted, now + _TOKEN_TTL_SECONDS)
    return minted


def session_token_schedule_warning(cfg: dict[str, Any], *, name: str = "Salesforce") -> str:
    """The schedule-save warning for a pasted, unrefreshable session token."""
    if auth_mode(cfg) != SESSION_TOKEN:
        return ""
    return (
        f"{name} authenticates with a pasted session token, which Salesforce expires "
        "(two hours by default) and Datawrap cannot refresh: scheduled runs fail once it "
        "expires. Switch the connector to OAuth client credentials (Consumer Key in "
        "Username, Consumer Secret in Password) for scheduled runs."
    )


def clear_cache() -> None:
    with _lock:
        _cache.clear()
