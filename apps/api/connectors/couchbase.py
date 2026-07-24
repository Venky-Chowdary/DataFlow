"""Couchbase source connector (N1QL REST endpoint).

Reads from Couchbase Server via the N1QL query service.
Configuration uses host, port, username/password, and bucket.
"""

from __future__ import annotations

from typing import Any

import requests

from connectors.saas_common import ReadBatch, humanize_http_error
from services.value_serializer import cell_to_string


def _url(host: str, port: int, ssl: bool) -> str:
    h = (host or "localhost").strip().rstrip("/")
    if not h:
        h = "localhost"
    if "://" not in h:
        scheme = "https" if ssl else "http"
        h = f"{scheme}://{h}"
    if port:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(h)
        if not parsed.port:
            h = urlunparse(parsed._replace(netloc=f"{parsed.hostname}:{port}"))
    return f"{h.rstrip('/')}/query/service"


def _n1ql(url: str, username: str, password: str, statement: str, timeout: float = 30.0) -> Any:
    auth = (username, password) if username and password else None
    resp = requests.post(
        url,
        json={"statement": statement, "timeout": f"{int(timeout)}s"},
        auth=auth,
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    errors = body.get("errors") or []
    if errors:
        msg = errors[0].get("msg", "Couchbase N1QL error")
        raise RuntimeError(msg)
    return body


def _extract_rows(body: Any) -> tuple[list[str], list[list[str]]]:
    """Union keys across the page — absent ≠ empty string (Airbyte-class trap)."""
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    results = (body or {}).get("results") or []
    if not results:
        return [], []
    records: list[dict[str, Any]] = []
    for raw in results:
        if isinstance(raw, dict) and len(raw) == 1:
            inner = list(raw.values())[0]
            if isinstance(inner, dict):
                record = dict(inner)
            else:
                record = {"value": inner}
        else:
            record = raw if isinstance(raw, dict) else {"value": raw}
        if isinstance(record, dict):
            records.append(record)

    headers: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record.keys():
            name = str(key)
            if name not in seen:
                seen.add(name)
                headers.append(name)
    headers = sorted(headers)

    rows: list[list[str]] = []
    for record in records:
        row: list[str] = []
        for h in headers:
            if h not in record:
                row.append(DF_MISSING_SENTINEL)
            elif record[h] is None:
                row.append(SQL_NULL_SENTINEL)
            else:
                row.append(cell_to_string(record[h]))
        rows.append(row)
    return headers, rows


def test_connection(
    *,
    host: str = "",
    port: int = 8093,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    **kwargs: Any,
) -> tuple[bool, str]:
    cfg = {**kwargs, "host": host, "port": port, "database": database or connection_string, "username": username, "password": password}
    url = _url(cfg.get("host", ""), cfg.get("port", 8093), cfg.get("ssl", False))
    try:
        _n1ql(url, username, password, "SELECT 1 AS ping")
        return True, "Couchbase reachable"
    except requests.exceptions.Timeout:
        return False, "Couchbase connection timed out. Check the host/port and network."
    except requests.exceptions.ConnectionError as exc:
        return False, f"Could not reach Couchbase host: {exc}"
    except requests.exceptions.HTTPError as exc:
        code = exc.response.status_code if exc.response else 0
        if code in (401, 403):
            return False, "Couchbase authentication failed. Check the username/password."
        return False, f"Couchbase probe failed: {exc}"
    except Exception as exc:
        return False, humanize_http_error(exc, "couchbase")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    merged = {**cfg, **kwargs}
    url = _url(merged.get("host", ""), merged.get("port", 8093), merged.get("ssl", False))
    bucket = (object or merged.get("database") or merged.get("connection_string") or "").strip()
    if not bucket:
        return ReadBatch(headers=[], rows=[], offset=0, total_rows=0)
    username = merged.get("username") or ""
    password = merged.get("password") or ""
    quoted = bucket.replace("`", "\\`")
    # ORDER BY META().id makes OFFSET pagination deterministic — without it,
    # pages can silently overlap or skip documents even with a static bucket.
    statement = (
        f"SELECT * FROM `{quoted}` "
        f"ORDER BY META().id "
        f"LIMIT {int(limit)} OFFSET {int(offset)}"
    )
    body = _n1ql(url, username, password, statement)
    headers, rows = _extract_rows(body)
    # A N1QL page length is not collection cardinality — never stop after page one.
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=None)


