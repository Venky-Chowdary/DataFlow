"""Salesforce source connector — REST API read with SOQL + Describe metadata."""

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
        # 401/403 and other Describe failures all refuse incomplete FIELDS(ALL)
        # fallbacks — operators need a clear schema contract, not a partial read.
        raise RuntimeError(
            f"Salesforce Describe is required for a complete transfer schema: {exc}. "
            "Grant object Describe permission or select an explicit approved field set."
        ) from exc

    if not field_chunks:
        raise RuntimeError(
            "Salesforce Describe returned no fields — refuse incomplete schema "
            "(FIELDS(ALL)/Id,Name fallbacks hide missing columns)."
        )

    query_url = f"{url_base}/services/data/{API_VERSION}/query"
    merged: dict[str, dict[str, Any]] = {}
    total_size: int | None = None
    id_field = "Id"

    def _run_query(field_list: str, identity_field: str) -> list[dict[str, Any]]:
        nonlocal total_size
        # Prefer queryMore / nextRecordsUrl over OFFSET — Salesforce OFFSET is capped
        # and unsuitable for multi-page replication.
        # ORDER BY identity so wide-schema field chunks merge by the same row set.
        query = f"SELECT {field_list} FROM {sobject} ORDER BY {identity_field}"
        if offset and offset > 0:
            # Shallow preview only — deep resumes must use cursor/keyset contracts.
            query += f" LIMIT {limit} OFFSET {offset}"
            r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
            r.raise_for_status()
            data = r.json()
            if "totalSize" in data and data.get("totalSize") is not None:
                published = int(data["totalSize"])
                total_size = published if total_size is None else max(total_size, published)
            return list(data.get("records") or [])

        query += f" LIMIT {min(limit, 2000)}"
        r = request(method="GET", url=query_url, token=access_token, params={"q": query}, timeout=60)
        r.raise_for_status()
        data = r.json()
        if "totalSize" in data and data.get("totalSize") is not None:
            published = int(data["totalSize"])
            total_size = published if total_size is None else max(total_size, published)
        out = list(data.get("records") or [])
        next_url = data.get("nextRecordsUrl")
        if not data.get("done", True) and not next_url:
            raise RuntimeError(
                "Salesforce query reported done=false without nextRecordsUrl; "
                "refusing incomplete page"
            )
        while next_url and len(out) < limit:
            if next_url.startswith("/"):
                # Relative path under the instance host.
                from urllib.parse import urlparse

                parsed = urlparse(url_base)
                abs_url = f"{parsed.scheme}://{parsed.netloc}{next_url}"
            else:
                abs_url = next_url
            r = request(method="GET", url=abs_url, token=access_token, timeout=60)
            r.raise_for_status()
            data = r.json()
            chunk = list(data.get("records") or [])
            next_url = data.get("nextRecordsUrl")
            if not data.get("done", True) and not next_url:
                raise RuntimeError(
                    "Salesforce queryMore reported done=false without nextRecordsUrl; "
                    "refusing incomplete page"
                )
            if not chunk:
                if not data.get("done", True):
                    raise RuntimeError(
                        "Salesforce queryMore returned an empty incomplete page; "
                        "refusing partial ingest"
                    )
                break
            out.extend(chunk)
        return out[:limit]

    try:
        for chunk in field_chunks:
            id_field = chunk[0]
            records = _run_query(",".join(chunk), id_field)
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
    except Exception as exc:
        raise RuntimeError(
            f"Salesforce SOQL field select failed — refuse incomplete Id,Name "
            f"fallback: {exc}"
        ) from exc

    batch = extract_records(records)
    # Only claim Salesforce totalSize when the API published it — never fabricate
    # cardinality from the fetched page length (stream early-stop trap).
    batch.total_rows = total_size
    if schema_warnings:
        batch.meta = {**(batch.meta or {}), "schema_warnings": schema_warnings}
    return batch
