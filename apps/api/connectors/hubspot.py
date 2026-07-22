"""HubSpot source connector — CRM object read + Properties describe."""

from __future__ import annotations

from typing import Any

from connectors.saas_common import (
    ReadBatch,
    base_url,
    extract_records,
    humanize_http_error,
    is_auth_error,
    object_name,
    request,
    token,
)

DEFAULT_HOST = "api.hubapi.com"
DEFAULT_OBJECT = "contacts"

# Core CRM objects always available without custom schema discovery.
_CORE_OBJECTS = ("contacts", "companies", "deals", "tickets", "products", "line_items")


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


def _access(cfg: dict[str, Any]) -> tuple[str, str]:
    access_token = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not access_token:
        raise ValueError("HubSpot private app token is required")
    return access_token, base_url(cfg.get("host", ""), DEFAULT_HOST)


def list_object_types(cfg: dict[str, Any]) -> list[str]:
    """Return HubSpot CRM object type ids (core + custom schemas when permitted)."""
    access_token, url_base = _access(cfg)
    names = list(_CORE_OBJECTS)
    try:
        r = request(
            method="GET",
            url=f"{url_base}/crm/v3/schemas",
            token=access_token,
            timeout=30,
        )
        r.raise_for_status()
        for item in r.json().get("results") or []:
            oid = item.get("objectTypeId") or item.get("name") or item.get("fullyQualifiedName")
            if oid and str(oid) not in names:
                names.append(str(oid))
    except Exception as exc:
        # Core objects remain available; auth failures must not look like "no custom schemas".
        if is_auth_error(exc):
            raise
    return names


def describe_properties(cfg: dict[str, Any], object_type: str = "") -> list[dict[str, Any]]:
    """Return HubSpot property definitions for an object type."""
    access_token, url_base = _access(cfg)
    obj = (object_type or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not obj:
        raise ValueError("HubSpot object/table name required")
    r = request(
        method="GET",
        url=f"{url_base}/crm/v3/properties/{obj}",
        token=access_token,
        timeout=45,
    )
    r.raise_for_status()
    props: list[dict[str, Any]] = []
    for p in r.json().get("results") or []:
        props.append(
            {
                "name": p.get("name") or "",
                "type": p.get("type") or "string",
                "fieldType": p.get("fieldType") or "",
                "label": p.get("label") or "",
                "hasUniqueValue": bool(p.get("hasUniqueValue")),
            }
        )
    return [p for p in props if p.get("name")]


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 500,
    offset: int = 0,
    **_kwargs: Any,
) -> ReadBatch:
    """Read HubSpot CRM object rows."""
    access_token, url_base = _access(cfg)
    obj = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not obj:
        raise ValueError("HubSpot object/table name required")

    url = f"{url_base}/crm/v3/objects/{obj}"
    params: dict[str, Any] = {"limit": limit}
    if offset:
        params["after"] = str(offset)

    # Ask for described properties so Map/preflight see the real field set.
    try:
        props = describe_properties(cfg, obj)
        names = [p["name"] for p in props if p.get("name")][:200]
        if names:
            params["properties"] = ",".join(names)
    except Exception as exc:
        if is_auth_error(exc):
            raise


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
