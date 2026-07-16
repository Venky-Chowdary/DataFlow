"""HubSpot source connector — CRM object read via private app token."""

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

DEFAULT_HOST = "api.hubapi.com"
DEFAULT_OBJECT = "contacts"


def test_hubspot(
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
    """Probe HubSpot connectivity with a private app access token."""
    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return False, "HubSpot private app token is required. Paste it in the API key field or connection string."
    url = f"{base_url(host, DEFAULT_HOST)}/crm/v3/objects/{DEFAULT_OBJECT}"
    try:
        r = request(method="GET", url=url, token=access_token, params={"limit": 1, "properties": "email"}, timeout=20)
        r.raise_for_status()
        return True, "HubSpot reachable"
    except Exception as exc:
        return False, humanize_http_error(exc, "hubspot")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 500,
    offset: int = 0,
    **_kwargs: Any,
) -> ReadBatch:
    """Read HubSpot CRM object rows."""
    access_token = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not access_token:
        raise ValueError("HubSpot private app token is required")
    obj = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not obj:
        raise ValueError("HubSpot object/table name required")

    url = f"{base_url(cfg.get('host', ''), DEFAULT_HOST)}/crm/v3/objects/{obj}"
    params: dict[str, Any] = {"limit": limit}
    if offset:
        params["after"] = str(offset)

    r = request(method="GET", url=url, token=access_token, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    results = data.get("results", [])
    records: list[dict[str, Any]] = []
    for item in results:
        rec: dict[str, Any] = {"id": item.get("id", "")}
        rec.update(item.get("properties") or {})
        records.append(rec)

    batch = extract_records(records)
    # HubSpot does not return a total count in the basic list response.
    batch.total_rows = len(records)
    return batch
