"""Generic REST API source connector.

Reads from any HTTP/HTTPS API that returns JSON arrays or paged JSON objects.
Auth, pagination, and response extraction are controlled through the standard
EndpointConfig fields plus optional JSON overrides in ``connection_string``.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import requests

from connectors.saas_common import ReadBatch, base_url, token


COMMON_DATA_PATHS = ["data", "results", "items", "records", "values", "contacts", "accounts", "list", "objects"]
COMMON_TOTAL_PATHS = ["total", "count", "total_count", "meta.total_count", "meta.count", "page.total_elements"]
COMMON_NEXT_PATHS = ["next", "paging.next", "meta.next", "links.next"]


def _deep_get(obj: Any, path: str) -> Any:
    if not path:
        return obj
    for part in path.split("."):
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


def _extract_records(body: Any, data_path: str = "") -> list[dict[str, Any]]:
    if isinstance(body, list):
        return [r for r in body if isinstance(r, dict)]
    if isinstance(body, dict):
        if data_path:
            records = _deep_get(body, data_path)
            if isinstance(records, list):
                return [r for r in records if isinstance(r, dict)]
        for path in COMMON_DATA_PATHS:
            records = _deep_get(body, path)
            if isinstance(records, list):
                return [r for r in records if isinstance(r, dict)]
    return []


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.update(_flatten(v, key))
            elif isinstance(v, list):
                out[key] = json.dumps(v, default=str)
            else:
                out[key] = v
    else:
        out[prefix or "value"] = obj
    return out


def _build_headers(cfg: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json", "User-Agent": "DataFlow/1.0"}
    mode = (cfg.get("auth_mode") or cfg.get("authPrefix") or "").lower()
    auth_header = (cfg.get("auth_header") or "").strip()
    auth_prefix = (cfg.get("auth_prefix") or "").strip() or "Bearer"
    api_key = token(cfg.get("api_key", ""), cfg.get("connection_string", ""), cfg.get("username", ""), cfg.get("password", ""))
    username = (cfg.get("username") or "").strip()
    password = (cfg.get("password") or "").strip()

    if auth_header:
        if api_key:
            headers[auth_header] = api_key
    elif mode in ("bearer", "") and api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif mode in ("token",) and api_key:
        headers["Authorization"] = f"Token {api_key}"
    elif mode in ("api_key", "apikey") and api_key:
        headers["X-Api-Key"] = api_key
    elif username and password:
        creds = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"
    return headers


def _build_params(cfg: dict[str, Any], pagination: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if cfg.get("auth_mode", "").lower() in ("api_key_query", "query") and cfg.get("api_key"):
        params[cfg.get("auth_query") or "api_key"] = cfg["api_key"]
    if pagination:
        params.update(pagination)
    # Strip None values so providers don't choke on empty cursors.
    return {k: v for k, v in params.items() if v is not None and v != ""}


def _default_base_url(catalog_id: str) -> str:
    """Known default hosts for popular REST APIs.  Users can override the host field."""
    catalog_id = (catalog_id or "").lower().strip()
    host_overrides: dict[str, str] = {
        "zendesk": "https://{subdomain}.zendesk.com/api/v2",
        "freshdesk": "https://{domain}.freshdesk.com/api/v2",
        "intercom": "https://api.intercom.io",
        "notion": "https://api.notion.com/v1",
        "asana": "https://app.asana.com/api/1.0",
        "trello": "https://api.trello.com/1",
        "mondaycom": "https://api.monday.com/v2",
        "jira": "https://{domain}.atlassian.net/rest/api/3",
        "confluence": "https://{domain}.atlassian.net/wiki/rest/api",
        "servicenow": "https://{instance}.service-now.com/api/now",
        "slack": "https://slack.com/api",
        "airtable": "https://api.airtable.com/v0",
        "shopify": "https://{shop}.myshopify.com/admin/api/2024-04",
        "github": "https://api.github.com",
        "gitlab": "https://gitlab.com/api/v4",
        "bitbucket": "https://api.bitbucket.org/2.0",
        "twilio": "https://api.twilio.com/2010-04-01",
        "sendgrid": "https://api.sendgrid.com/v3",
        "mailchimp": "https://{dc}.api.mailchimp.com/3.0",
        "klaviyo": "https://a.klaviyo.com/api",
        "stripe": "https://api.stripe.com",
        "hubspot": "https://api.hubapi.com",
        "salesforce": "https://login.salesforce.com",
    }
    return host_overrides.get(catalog_id, "")


def _parse_json_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Allow advanced overrides via the connection_string JSON or extra dict."""
    overrides: dict[str, Any] = {}
    raw = (cfg.get("connection_string") or "").strip()
    if raw.startswith("{"):
        try:
            overrides = json.loads(raw)
        except Exception:
            pass
    if isinstance(cfg.get("extra"), dict):
        overrides.update(cfg["extra"])
    return overrides


def _resolve_config(cfg: dict[str, Any]) -> dict[str, Any]:
    catalog_id = (cfg.get("type") or cfg.get("format") or "").lower().strip()
    overrides = _parse_json_config(cfg)
    merged = {**cfg, **overrides}

    host = (merged.get("host") or merged.get("endpoint_url") or "").strip()
    if not host:
        host = _default_base_url(catalog_id)
    host = base_url(host, "")
    merged["host"] = host

    object_path = (merged.get("table") or merged.get("database") or merged.get("object") or "").strip()
    merged["object_path"] = object_path

    pagination_type = (merged.get("pagination_type") or merged.get("pagination") or "offset").lower()
    if pagination_type not in {"offset", "page", "cursor", "link", "none"}:
        pagination_type = "offset"
    merged["pagination_type"] = pagination_type

    merged.setdefault("data_path", "")
    merged.setdefault("total_path", "")
    merged.setdefault("next_path", "")
    merged.setdefault("offset_param", "offset")
    merged.setdefault("limit_param", "limit")
    merged.setdefault("page_param", "page")
    merged.setdefault("cursor_param", "cursor")
    merged.setdefault("auth_header", "")
    merged.setdefault("auth_prefix", "Bearer")
    merged.setdefault("auth_query", "api_key")

    return merged


def _get_url(cfg: dict[str, Any], pagination: dict[str, Any], next_url: str | None = None) -> str:
    if next_url:
        return next_url
    host = cfg["host"].rstrip("/")
    obj = cfg["object_path"].strip("/")
    if not host:
        raise ValueError("Host / base URL is required for REST API source")
    if obj:
        return f"{host}/{obj}"
    return host


def _extract_next_cursor(body: Any, cfg: dict[str, Any], headers: dict[str, str]) -> str | None:
    next_path = cfg.get("next_path") or ""
    if next_path:
        nxt = _deep_get(body, next_path)
        if isinstance(nxt, str) and nxt:
            return nxt
    for path in COMMON_NEXT_PATHS:
        nxt = _deep_get(body, path)
        if isinstance(nxt, str) and nxt:
            return nxt
    # Link header (RFC 5988) fallback.
    link_header = headers.get("Link") or headers.get("link", "")
    if link_header:
        for part in link_header.split(","):
            url_match = part.split(";")
            if len(url_match) == 2 and 'rel="next"' in url_match[1]:
                url = url_match[0].strip()
                if url.startswith("<") and url.endswith(">"):
                    return url[1:-1]
    return None


def _read_page(cfg: dict[str, Any], pagination: dict[str, Any], next_url: str | None = None) -> tuple[list[dict[str, Any]], str | None, bool]:
    url = _get_url(cfg, pagination, next_url)
    headers = _build_headers(cfg)
    params = _build_params(cfg, pagination)
    resp = requests.request("GET", url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    body = resp.json()
    records = _extract_records(body, cfg.get("data_path", ""))
    next_cursor = _extract_next_cursor(body, cfg, dict(resp.headers))
    has_more = bool(next_cursor)
    return records, next_cursor, has_more


def test_connection(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    api_key: str = "",
    connection_string: str = "",
    ssl: bool = False,
    table: str = "",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe a REST API source with a small request."""
    cfg = _resolve_config({
        "host": host,
        "database": database or table,
        "username": username,
        "password": password,
        "api_key": api_key,
        "connection_string": connection_string,
        "ssl": ssl,
        "type": kwargs.get("type") or kwargs.get("format", ""),
        **kwargs,
    })
    try:
        url = _get_url(cfg, {})
        # Make a lightweight request with limit=1 if the API supports it.
        params = _build_params(cfg, {cfg.get("limit_param", "limit"): 1})
        resp = requests.request("GET", url, headers=_build_headers(cfg), params=params, timeout=30)
        if resp.status_code == 401 or resp.status_code == 403:
            return False, "REST API authentication failed. Check the API token/key and required permissions."
        if resp.status_code == 404:
            return False, "REST API host reachable, but the object/table path was not found. Check the object/table name."
        resp.raise_for_status()
        return True, "REST API reachable"
    except requests.exceptions.Timeout:
        return False, "REST API connection timed out. Check the host/URL and network."
    except requests.exceptions.ConnectionError as exc:
        return False, f"Could not reach the REST API host: {exc}"
    except Exception as exc:
        return False, f"REST API probe failed: {exc}"


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read up to ``limit`` rows from a generic REST API source, paginating as needed."""
    cfg = _resolve_config({**cfg, "table": object or cfg.get("table", "")})
    pagination_type = cfg["pagination_type"]

    all_rows: list[dict[str, Any]] = []
    next_url: str | None = None
    page_index = 0
    offset_param = cfg["offset_param"]
    limit_param = cfg["limit_param"]
    page_param = cfg["page_param"]
    cursor_param = cfg["cursor_param"]

    while len(all_rows) < limit:
        page_limit = min(limit - len(all_rows), 100)
        if pagination_type == "none":
            records, _, _ = _read_page(cfg, {}, next_url)
            all_rows.extend(records)
            break
        elif pagination_type == "offset":
            start = offset + (page_index * 100)
            pagination = {offset_param: start, limit_param: page_limit}
            records, _, has_more = _read_page(cfg, pagination, None)
            all_rows.extend(records)
            if not has_more or not records or len(records) < page_limit:
                break
        elif pagination_type == "page":
            page = offset + page_index + 1
            pagination = {page_param: page, limit_param: page_limit}
            records, _, has_more = _read_page(cfg, pagination, None)
            all_rows.extend(records)
            if not has_more or not records or len(records) < page_limit:
                break
        elif pagination_type == "cursor":
            pagination: dict[str, Any] = {limit_param: page_limit}
            if next_url:
                # next_url is a cursor value in this branch (set below).
                pagination[cursor_param] = next_url
            records, next_cursor, has_more = _read_page(cfg, pagination, None)
            all_rows.extend(records)
            next_url = next_cursor or ""
            if not next_url or not records:
                break
        elif pagination_type == "link":
            records, next_url, _ = _read_page(cfg, {limit_param: page_limit}, next_url)
            all_rows.extend(records)
            if not next_url or not records:
                break
        else:
            break
        page_index += 1

    all_rows = all_rows[:limit]
    if not all_rows:
        return ReadBatch(headers=[], rows=[], offset=0, total_rows=0)

    # Union all keys because flattened records may differ across pages.
    keys: list[str] = []
    seen = set()
    flattened: list[dict[str, str]] = []
    for rec in all_rows:
        flat = _flatten(rec)
        for k in flat:
            if k not in seen:
                seen.add(k)
                keys.append(k)
        flattened.append({k: str(v) for k, v in flat.items()})

    rows = [[r.get(k, "") for k in keys] for r in flattened]
    return ReadBatch(headers=keys, rows=rows, offset=0, total_rows=len(rows))


# Source-only connector: writes are not supported.
from connectors.saas_common import write_not_supported as write_mapped_rows
