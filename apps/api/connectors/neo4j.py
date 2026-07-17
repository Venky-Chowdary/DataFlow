"""Neo4j source connector (HTTP Cypher endpoint).

Reads from Neo4j 4.x/5.x via the transactional Cypher HTTP endpoint.
Configuration uses host, port, username/password, and database.
"""

from __future__ import annotations

import json
from typing import Any

import requests

from connectors.saas_common import ReadBatch, humanize_http_error


def _url(host: str, port: int, ssl: bool, database: str) -> str:
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
    db = (database or "neo4j").strip()
    return f"{h.rstrip('/')}/db/{db}/tx/commit"


def _run_cypher(url: str, username: str, password: str, statement: str, timeout: float = 30.0) -> Any:
    payload = {"statements": [{"statement": statement, "resultDataContents": ["row"]}]}
    auth = (username, password) if username and password else None
    resp = requests.post(url, json=payload, auth=auth, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    errors = body.get("errors") or []
    if errors:
        raise RuntimeError(errors[0].get("message", "Neo4j Cypher error"))
    return body


def _extract_rows(body: Any) -> tuple[list[str], list[list[str]]]:
    results = (body or {}).get("results") or []
    if not results:
        return [], []
    result = results[0]
    columns = result.get("columns") or []
    data = result.get("data") or []
    rows: list[list[str]] = []
    for item in data:
        row = item.get("row") or []
        # Each row entry may be a node dict (when RETURN n) or scalar/dict.
        flat_row = []
        for value in row:
            if isinstance(value, dict):
                if "properties" in value:
                    flat_row.append(json.dumps(value["properties"], default=str))
                else:
                    flat_row.append(json.dumps(value, default=str))
            elif isinstance(value, list):
                flat_row.append(json.dumps(value, default=str))
            else:
                flat_row.append(str(value))
        rows.append(flat_row)
    return columns, rows


def test_connection(
    *,
    host: str = "",
    port: int = 7474,
    database: str = "neo4j",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    **kwargs: Any,
) -> tuple[bool, str]:
    cfg = {**kwargs, "host": host, "port": port, "database": database, "username": username, "password": password}
    url = _url(cfg.get("host", ""), cfg.get("port", 7474), cfg.get("ssl", False), cfg.get("database", "neo4j"))
    try:
        _run_cypher(url, username, password, "MATCH (n) RETURN count(n) AS count LIMIT 1")
        return True, "Neo4j reachable"
    except requests.exceptions.Timeout:
        return False, "Neo4j connection timed out. Check the host/port and network."
    except requests.exceptions.ConnectionError as exc:
        return False, f"Could not reach Neo4j host: {exc}"
    except requests.exceptions.HTTPError as exc:
        code = exc.response.status_code if exc.response else 0
        if code in (401, 403):
            return False, "Neo4j authentication failed. Check the username/password."
        return False, f"Neo4j probe failed: {exc}"
    except Exception as exc:
        return False, humanize_http_error(exc, "neo4j")


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    merged = {**cfg, **kwargs}
    url = _url(merged.get("host", ""), merged.get("port", 7474), merged.get("ssl", False), merged.get("database", "neo4j"))
    username = merged.get("username") or ""
    password = merged.get("password") or ""
    label = (object or "").strip()
    if label:
        quoted = label.replace("`", "\\`")
        statement = f"MATCH (n:`{quoted}`) RETURN n SKIP {offset} LIMIT {limit}"
    else:
        statement = f"MATCH (n) RETURN n SKIP {offset} LIMIT {limit}"
    body = _run_cypher(url, username, password, statement)
    headers, rows = _extract_rows(body)
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=len(rows))


