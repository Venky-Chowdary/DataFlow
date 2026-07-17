"""InfluxDB source connector (1.x HTTP API).

Reads from an InfluxDB 1.x instance using the /query InfluxQL endpoint.
Configuration uses host, port, database, username/password, and measurement.
"""

from __future__ import annotations

from typing import Any

import requests

from connectors.saas_common import ReadBatch, humanize_http_error


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
    return h.rstrip("/")


def _query(url_base: str, database: str, q: str, username: str = "", password: str = "", timeout: float = 30.0) -> Any:
    params: dict[str, str] = {"q": q}
    if database:
        params["db"] = database
    auth = (username, password) if username and password else None
    resp = requests.get(f"{url_base}/query", params=params, auth=auth, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _extract_rows(body: Any) -> tuple[list[str], list[list[str]]]:
    if not isinstance(body, dict):
        return [], []
    results = body.get("results") or []
    if not isinstance(results, list):
        return [], []
    for result in results:
        series_list = result.get("series") or []
        if series_list:
            series = series_list[0]
            columns = series.get("columns") or []
            values = series.get("values") or []
            rows = [[str(v) for v in row] for row in values]
            return list(columns), rows
    return [], []


def test_connection(
    *,
    host: str = "",
    port: int = 8086,
    database: str = "",
    username: str = "",
    password: str = "",
    api_key: str = "",
    connection_string: str = "",
    ssl: bool = False,
    **kwargs: Any,
) -> tuple[bool, str]:
    cfg = {**kwargs, "host": host, "port": port, "database": database or connection_string, "username": username, "password": password or api_key}
    url_base = _url(cfg.get("host", ""), cfg.get("port", 8086), cfg.get("ssl", False))
    try:
        _query(url_base, database or "", "SHOW DATABASES", username, password)
        return True, "InfluxDB reachable"
    except requests.exceptions.Timeout:
        return False, "InfluxDB connection timed out. Check the host/port and network."
    except requests.exceptions.ConnectionError as exc:
        return False, f"Could not reach InfluxDB host: {exc}"
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in (401, 403):
            return False, "InfluxDB authentication failed. Check the username/password."
        return False, f"InfluxDB probe failed: {exc}"
    except Exception as exc:
        return False, humanize_http_error(exc, "influxdb")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    merged = {**cfg, **kwargs}
    url_base = _url(merged.get("host", ""), merged.get("port", 8086), merged.get("ssl", False))
    database = (merged.get("database") or "").strip()
    measurement = (object or merged.get("table") or merged.get("database") or "").strip()
    if not measurement:
        return ReadBatch(headers=[], rows=[], offset=0, total_rows=0)

    username = merged.get("username") or ""
    password = merged.get("password") or ""
    # InfluxQL identifiers must be double-quoted if they contain special chars.
    quoted = measurement.replace('"', '\\"')
    q = f'SELECT * FROM "{quoted}" LIMIT {limit} OFFSET {offset}'
    body = _query(url_base, database, q, username, password)
    headers, rows = _extract_rows(body)
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=len(rows))


# Source-only connector
