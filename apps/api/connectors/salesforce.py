"""Salesforce source connector — REST API read with SOQL + Describe metadata."""

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

DEFAULT_HOST = "login.salesforce.com"
DEFAULT_OBJECT = "Account"
API_VERSION = "v58.0"


def test_salesforce(
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
    """Probe Salesforce connectivity with an access token."""
    access_token = token(api_key, connection_string, username, password)
    if not access_token:
        return False, "Salesforce access token is required. Paste it in the API key field or connection string."
    url = f"{base_url(host, DEFAULT_HOST)}/services/data/{API_VERSION}/limits"
    try:
        r = request(method="GET", url=url, token=access_token, timeout=20)
        r.raise_for_status()
        return True, "Salesforce reachable"
    except Exception as exc:
        return False, humanize_http_error(exc, "salesforce")


def _access(cfg: dict[str, Any]) -> tuple[str, str]:
    access_token = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not access_token:
        raise ValueError("Salesforce access token is required")
    return access_token, base_url(cfg.get("host", ""), DEFAULT_HOST)


def list_sobjects(cfg: dict[str, Any]) -> list[str]:
    """Return queryable SObject API names."""
    access_token, url_base = _access(cfg)
    r = request(
        method="GET",
        url=f"{url_base}/services/data/{API_VERSION}/sobjects",
        token=access_token,
        timeout=30,
    )
    r.raise_for_status()
    out: list[str] = []
    for item in (r.json().get("sobjects") or []):
        if item.get("queryable") and item.get("name"):
            out.append(str(item["name"]))
    return out


def describe_sobject(cfg: dict[str, Any], sobject: str) -> list[dict[str, Any]]:
    """Return Salesforce describe fields for one SObject."""
    access_token, url_base = _access(cfg)
    name = (sobject or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not name:
        raise ValueError("Salesforce object/table name required")
    r = request(
        method="GET",
        url=f"{url_base}/services/data/{API_VERSION}/sobjects/{name}/describe",
        token=access_token,
        timeout=45,
    )
    r.raise_for_status()
    fields: list[dict[str, Any]] = []
    for f in r.json().get("fields") or []:
        fields.append(
            {
                "name": f.get("name") or "",
                "type": f.get("type") or "string",
                "nillable": bool(f.get("nillable", True)),
                "length": f.get("length"),
                "precision": f.get("precision"),
                "scale": f.get("scale"),
                "label": f.get("label") or "",
            }
        )
    return [f for f in fields if f.get("name")]


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 500,
    offset: int = 0,
    **_kwargs: Any,
) -> ReadBatch:
    """Read Salesforce object rows via SOQL."""
    access_token, url_base = _access(cfg)
    sobject = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not sobject:
        raise ValueError("Salesforce object/table name required")

    # Prefer Describe-driven field list when available (more honest than FIELDS(ALL)).
    field_list = "FIELDS(ALL)"
    try:
        described = describe_sobject(cfg, sobject)
        names = [f["name"] for f in described if f.get("name")]
        if names:
            # Cap SOQL field list to avoid URI limits on huge objects.
            field_list = ",".join(names[:200])
    except Exception as exc:
        # Auth failures must surface — never silently fall back with a bad token.
        if is_auth_error(exc):
            raise
        field_list = "FIELDS(ALL)"

    query = f"SELECT {field_list} FROM {sobject} LIMIT {limit}"
    if offset:
        query += f" OFFSET {offset}"
    query_url = f"{url_base}/services/data/{API_VERSION}/query"
    try:
        r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
        r.raise_for_status()
    except Exception as exc:
        if "INVALID_FIELD" in str(exc) or "FIELDS(ALL)" in str(exc) or "EXCEEDED_ID_LIMIT" in str(exc):
            query = f"SELECT Id,Name FROM {sobject} LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
            r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
            r.raise_for_status()
        else:
            raise

    data = r.json()
    records = data.get("records", [])
    batch = extract_records(records)
    batch.total_rows = data.get("totalSize", len(records))
    return batch
