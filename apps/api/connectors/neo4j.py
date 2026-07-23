"""Neo4j source connector (HTTP Cypher endpoint).

Reads from Neo4j 4.x/5.x via the transactional Cypher HTTP endpoint.
Configuration uses host, port, username/password, and database.

Nodes are projected with stable identity (elementId + labels + properties) —
never collapse a graph node into an anonymous property bag (Airbyte-class gap).
"""

from __future__ import annotations

import json
from typing import Any

import requests

from connectors.saas_common import ReadBatch, humanize_http_error
from services.value_serializer import cell_to_string, json_default


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


def _node_record_from_row(row: list[Any]) -> dict[str, Any]:
    """Map ``[elementId, labels, properties]`` (or legacy node dict) to a flat record."""
    if len(row) >= 3 and isinstance(row[2], dict) and not (
        isinstance(row[0], dict) and "properties" in row[0]
    ):
        element_id, labels, props = row[0], row[1], row[2]
        record = dict(props) if isinstance(props, dict) else {"value": props}
        record["_neo4j_element_id"] = element_id
        if isinstance(labels, list):
            record["_neo4j_labels"] = json.dumps(labels, default=json_default)
        else:
            record["_neo4j_labels"] = cell_to_string(labels)
        return record

    # Legacy / ad-hoc: single node dict from RETURN n (REST graph or properties map).
    if len(row) == 1 and isinstance(row[0], dict):
        value = row[0]
        if "properties" in value:
            record = dict(value.get("properties") or {})
            meta = value.get("meta") or {}
            if value.get("elementId") is not None:
                record["_neo4j_element_id"] = value.get("elementId")
            elif meta.get("id") is not None:
                record["_neo4j_element_id"] = meta.get("id")
            elif value.get("id") is not None:
                record["_neo4j_element_id"] = value.get("id")
            labels = value.get("labels") or meta.get("labels")
            if labels is not None:
                record["_neo4j_labels"] = (
                    json.dumps(labels, default=json_default)
                    if isinstance(labels, list)
                    else cell_to_string(labels)
                )
            return record
        return dict(value)

    # Scalar / multi-column projection — preserve column values as-is via caller.
    return {}


def _extract_rows(body: Any) -> tuple[list[str], list[list[str]]]:
    results = (body or {}).get("results") or []
    if not results:
        return [], []
    result = results[0]
    columns = [str(c) for c in (result.get("columns") or [])]
    data = result.get("data") or []

    # Identity-aware projection: elementId / labels / properties.
    if columns == ["_neo4j_element_id", "_neo4j_labels", "props"] or (
        len(columns) == 3
        and columns[0] in {"_neo4j_element_id", "elementId(n)", "elementId"}
    ):
        records: list[dict[str, Any]] = []
        for item in data:
            row = item.get("row") or []
            records.append(_node_record_from_row(row))
        headers: list[str] = []
        seen: set[str] = set()
        for preferred in ("_neo4j_element_id", "_neo4j_labels"):
            if any(preferred in r for r in records):
                seen.add(preferred)
                headers.append(preferred)
        for rec in records:
            for k in rec.keys():
                if k not in seen:
                    seen.add(k)
                    headers.append(k)
        matrix = [[cell_to_string(rec.get(h)) for h in headers] for rec in records]
        return headers, matrix

    # Generic column projection (custom Cypher).
    rows: list[list[str]] = []
    for item in data:
        row = item.get("row") or []
        flat_row = []
        for value in row:
            if isinstance(value, dict) and "properties" in value:
                # Honest: serialize full node envelope including identity when present.
                envelope = {
                    "_neo4j_element_id": value.get("elementId") or (value.get("meta") or {}).get("id"),
                    "_neo4j_labels": value.get("labels") or (value.get("meta") or {}).get("labels"),
                    **(value.get("properties") or {}),
                }
                flat_row.append(json.dumps(envelope, default=json_default))
            else:
                flat_row.append(cell_to_string(value))
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
    # Deterministic order + identity columns — SKIP/LIMIT alone is not resume-safe.
    if label:
        quoted = label.replace("`", "\\`")
        statement = (
            f"MATCH (n:`{quoted}`) "
            f"RETURN elementId(n) AS _neo4j_element_id, labels(n) AS _neo4j_labels, properties(n) AS props "
            f"ORDER BY elementId(n) "
            f"SKIP {int(offset)} LIMIT {int(limit)}"
        )
    else:
        statement = (
            "MATCH (n) "
            "RETURN elementId(n) AS _neo4j_element_id, labels(n) AS _neo4j_labels, properties(n) AS props "
            "ORDER BY elementId(n) "
            f"SKIP {int(offset)} LIMIT {int(limit)}"
        )
    body = _run_cypher(url, username, password, statement)
    headers, rows = _extract_rows(body)
    # A page size is not a table total — keep continuation open until a short
    # or empty page establishes end-of-source (same honesty as Influx).
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=None)
