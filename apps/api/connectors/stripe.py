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
    # Stripe list APIs are cursor-paged (max 100/page). Never send limit=100000 or
    # treat a numeric OFFSET as starting_after (that silently returns wrong pages).
    requested = max(1, int(limit or 100))
    items: list[dict[str, Any]] = []
    starting_after = ""
    skip_remaining = 0
    if offset is not None and str(offset).strip():
        off = str(offset).strip()
        if off.isdigit():
            skip_remaining = int(off)
        else:
            starting_after = off

    while len(items) < requested:
        page_need = skip_remaining + (requested - len(items))
        params: dict[str, Any] = {"limit": min(100, max(1, page_need))}
        if starting_after:
            params["starting_after"] = starting_after
        r = request(method="GET", url=url, token=secret_key, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        page = data.get("data")
        if not isinstance(page, list):
            raise ValueError("Stripe list response missing data array")
        if skip_remaining:
            if len(page) <= skip_remaining:
                skip_remaining -= len(page)
                if not data.get("has_more"):
                    break
                last = page[-1] if page else None
                if not isinstance(last, dict) or not last.get("id"):
                    raise RuntimeError(
                        "Stripe reports more results without a final object id; refusing partial ingest"
                    )
                starting_after = str(last["id"])
                continue
            page = page[skip_remaining:]
            skip_remaining = 0
        items.extend(page)
        if not data.get("has_more"):
            break
        if not page:
            raise RuntimeError(
                "Stripe reports more results but returned an empty page; "
                "refusing partial ingest"
            )
        last = page[-1]
        if not isinstance(last, dict) or not last.get("id"):
            raise RuntimeError(
                "Stripe reports more results without a final object id; refusing partial ingest"
            )
        starting_after = str(last["id"])

    batch = extract_records(items[:requested])
    # Stripe list APIs do not publish authoritative totals — never claim the
    # fetched page length is the object cardinality (stream early-stop trap).
    batch.total_rows = None
    return batch
