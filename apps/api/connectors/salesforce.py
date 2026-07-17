"""Salesforce source connector — REST API read with SOQL."""

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


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 500,
    offset: int = 0,
    **_kwargs: Any,
) -> ReadBatch:
    """Read Salesforce object rows via SOQL."""
    access_token = token(
        cfg.get("api_key", ""),
        cfg.get("connection_string", ""),
        cfg.get("username", ""),
        cfg.get("password", ""),
    )
    if not access_token:
        raise ValueError("Salesforce access token is required")
    sobject = (object or object_name(cfg, DEFAULT_OBJECT)).strip()
    if not sobject:
        raise ValueError("Salesforce object/table name required")

    url_base = base_url(cfg.get("host", ""), DEFAULT_HOST)

    # Try FIELDS(ALL) first; fall back to a small default field list.
    query = f"SELECT FIELDS(ALL) FROM {sobject} LIMIT {limit}"
    if offset:
        query += f" OFFSET {offset}"
    query_url = f"{url_base}/services/data/{API_VERSION}/query"
    try:
        r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
        r.raise_for_status()
    except Exception as exc:
        if "INVALID_FIELD" in str(exc) or "FIELDS(ALL)" in str(exc):
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
