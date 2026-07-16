"""Stripe source connector — list API read for customers, charges, invoices, etc."""

from __future__ import annotations

from typing import Any

from connectors.saas_common import (
    ReadBatch,
    base_url,
    extract_records,
    humanize_http_error,
    object_name,
    request,
    token,
    write_not_supported,
)

DEFAULT_HOST = "api.stripe.com"
DEFAULT_OBJECT = "customers"


def test_stripe(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    table: str = "",
    connection_string: str = "",
    api_key: str = "",
    username: str = "",
    password: str = "",
    ssl: bool = False,
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Probe Stripe connectivity with a secret key."""
    secret_key = token(api_key, connection_string, username, password)
    if not secret_key:
        return False, "Stripe secret key is required. Paste it in the API key field or connection string."
    url = f"{base_url(host, DEFAULT_HOST)}/v1/account"
    try:
        r = request(method="GET", url=url, token=secret_key, timeout=20)
        r.raise_for_status()
        return True, "Stripe reachable"
    except Exception as exc:
        return False, humanize_http_error(exc, "stripe")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **_kwargs: Any,
) -> ReadBatch:
    """Read Stripe object list."""
    secret_key = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not secret_key:
        raise ValueError("Stripe secret key is required")
    obj = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not obj:
        raise ValueError("Stripe object/table name required")

    url = f"{base_url(cfg.get('host', ''), DEFAULT_HOST)}/v1/{obj}"
    params: dict[str, Any] = {"limit": limit}
    if offset:
        # Stripe pagination uses IDs, but a numeric offset is accepted as a last-id approximation.
        params["starting_after"] = str(offset)

    r = request(method="GET", url=url, token=secret_key, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    items = data.get("data", [])
    batch = extract_records(items)
    batch.total_rows = data.get("meta", {}).get("total_count") or len(items)
    return batch
