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

    names: list[str] = []
    field_chunks: list[list[str]] = []
    schema_warnings: list[str] = []
    try:
        described = describe_sobject(cfg, sobject)
        names = [f["name"] for f in described if f.get("name")]
        if names:
            # Chunk Describe fields so wide objects never silently drop columns
            # past the SOQL URI/field cap (Airbyte-class honesty gap).
            id_field = "Id" if "Id" in names else ("id" if "id" in names else names[0])
            others = [n for n in names if n != id_field]
            # Id + up to 99 fields per query keeps URI size manageable.
            for i in range(0, max(len(others), 1), 99):
                chunk = others[i : i + 99]
                field_chunks.append([id_field] + chunk if chunk or not field_chunks else [id_field])
            if not others and not field_chunks:
                field_chunks = [[id_field]]
            if len(names) > 100:
                schema_warnings.append(
                    f"Describe returned {len(names)} fields — fetched via "
                    f"{len(field_chunks)} SOQL chunk(s) and merged by {id_field}"
                )
    except Exception as exc:
        if is_auth_error(exc):
            raise
        field_chunks = []
        schema_warnings.append(f"Describe unavailable ({exc}); using FIELDS(ALL)")

    query_url = f"{url_base}/services/data/{API_VERSION}/query"
    merged: dict[str, dict[str, Any]] = {}
    total_size = 0
    id_field = "Id"

    def _run_query(field_list: str) -> list[dict[str, Any]]:
        nonlocal total_size
        query = f"SELECT {field_list} FROM {sobject} LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
        r.raise_for_status()
        data = r.json()
        total_size = max(total_size, int(data.get("totalSize") or 0))
        return list(data.get("records") or [])

    try:
        if field_chunks:
            for chunk in field_chunks:
                id_field = chunk[0]
                records = _run_query(",".join(chunk))
                for rec in records:
                    clean = {k: v for k, v in rec.items() if k != "attributes"}
                    rid = str(clean.get(id_field) or clean.get("Id") or clean.get("id") or "")
                    if not rid:
                        # No identity — keep as anonymous row key
                        rid = f"_anon_{len(merged)}"
                    if rid not in merged:
                        merged[rid] = clean
                    else:
                        merged[rid].update(clean)
            records = list(merged.values())
        else:
            records = _run_query("FIELDS(ALL)")
    except Exception as exc:
        if "INVALID_FIELD" in str(exc) or "FIELDS(ALL)" in str(exc) or "EXCEEDED_ID_LIMIT" in str(exc):
            schema_warnings.append(
                "SOQL field select failed — fell back to Id,Name "
                "(column coverage may be incomplete; fix Describe/permissions)"
            )
            records = _run_query("Id,Name")
        else:
            raise

    batch = extract_records(records)
    batch.total_rows = total_size or len(records)
    if schema_warnings:
        batch.meta = {**(batch.meta or {}), "schema_warnings": schema_warnings}
    return batch
