"""Extract-and-query: the query console's fallback for non-SQL connectors.

`_run_sql_query` builds a SQLAlchemy engine, which only exists for real SQL
dialects — S3, SaaS apps, SDK manifest sources and file stores have none and
used to fail hard ("SQLAlchemy dialect/driver for 's3' is not available").

This module materializes a bounded extract of each queryable object into a
temp DuckDB database, then delegates to the SAME safe-SQL path every other
connector uses (sqlglot guard, read-only session, statement timeout, result
limit, playground external-read refusal). One query path, two engines.

Caps are deliberate and reported: a query console page is an interactive
tool, not a bulk export — objects are capped per object and per call, and
`truncated` is set when the extract (or the SQL LIMIT) withheld rows.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger(__name__)

_EXTRACT_PAGE = 1000
_EXTRACT_MAX_ROWS = int(os.environ.get("DATAFLOW_QUERY_EXTRACT_MAX_ROWS", "50000"))
_EXTRACT_MAX_OBJECTS = int(os.environ.get("DATAFLOW_QUERY_EXTRACT_MAX_OBJECTS", "25"))

# Connector families with a `read_object` adapter the transfer engine already
# uses — the query extract reuses exactly those readers.
_STORE_TYPES = {"s3", "gcs", "adls", "sftp"}
_SAAS_TYPES = {
    "salesforce",
    "hubspot",
    "stripe",
    "shopify",
    "zendesk",
    "notion",
    "airtable",
    "rest_api",
    "influxdb",
    "neo4j",
    "couchbase",
}


def connector_queryable_via_extract(ctype: str) -> bool:
    if ctype in _STORE_TYPES or ctype in _SAAS_TYPES:
        return True
    try:
        from connectors.sdk import get_sdk_connector

        return get_sdk_connector(ctype) is not None
    except Exception:
        return False


def _sanitized_table_name(raw: str, used: set[str]) -> str:
    base = os.path.basename(str(raw).rstrip("/")) or str(raw) or "data"
    base = os.path.splitext(base)[0] or base
    name = re.sub(r"[^0-9A-Za-z_]", "_", base).strip("_") or "data"
    if name[0].isdigit():
        name = f"t_{name}"
    candidate, i = name, 2
    while candidate in used:
        candidate = f"{name}_{i}"
        i += 1
    used.add(candidate)
    return candidate


def _connector_cfg(connector: Any) -> dict[str, Any]:
    cfg = {
        "type": getattr(connector, "type", "") or "",
        "host": getattr(connector, "host", "") or "",
        "port": getattr(connector, "port", 0) or 0,
        "database": getattr(connector, "database", "") or "",
        "username": getattr(connector, "username", "") or "",
        "password": getattr(connector, "password", "") or "",
        "connection_string": getattr(connector, "connection_string", "") or "",
        "schema": getattr(connector, "schema", "") or "",
        "warehouse": getattr(connector, "warehouse", "") or "",
        "ssl": bool(getattr(connector, "ssl", False)),
    }
    extra = getattr(connector, "extra", None)
    if isinstance(extra, dict):
        cfg.update(extra)
    return cfg


def _read_sdk_object(cfg: dict[str, Any], ctype: str, stream: str):
    """Page an SDK stream to the extract cap via sdk_state tokens."""
    from connectors.sdk.transfer_bridge import read_object

    cfg = {**cfg, "type": ctype}
    headers: list[str] = []
    rows: list[list[Any]] = []
    state: str | None = None
    truncated = False
    while len(rows) < _EXTRACT_MAX_ROWS:
        # Request a full page, not the remaining cap: the SDK treats `limit`
        # as a hard contract and errors when one page exceeds it. Slice
        # client-side instead and flag truncation.
        batch = read_object(
            cfg=cfg,
            object=stream,
            limit=_EXTRACT_PAGE,
            offset=0,
            sdk_state=state,
        )
        headers = batch.headers or headers
        remaining = _EXTRACT_MAX_ROWS - len(rows)
        rows.extend(batch.rows[:remaining])
        if len(batch.rows) > remaining:
            truncated = True
            break
        meta = batch.meta or {}
        if meta.get("sdk_done") or not batch.rows:
            break
        state = meta.get("sdk_state") or None
        if not state:
            break
    if len(rows) >= _EXTRACT_MAX_ROWS:
        truncated = True
    return headers, rows, truncated


def _read_offset_object(read_fn, cfg: dict[str, Any], **kwargs):
    """Page an offset-based read_object until a short page or the cap."""
    headers: list[str] = []
    rows: list[list[Any]] = []
    offset = 0
    truncated = False
    while len(rows) < _EXTRACT_MAX_ROWS:
        batch = read_fn(
            cfg=cfg,
            offset=offset,
            limit=_EXTRACT_PAGE,
            **kwargs,
        )
        headers = batch.headers or headers
        remaining = _EXTRACT_MAX_ROWS - len(rows)
        rows.extend(batch.rows[:remaining])
        if len(batch.rows) > remaining:
            truncated = True
            break
        offset += len(batch.rows)
        meta = batch.meta or {}
        if meta.get("sdk_done") or not batch.rows:
            break
    if len(rows) >= _EXTRACT_MAX_ROWS:
        truncated = True
    return headers, rows, truncated


def _extract_objects(
    connector: Any, endpoint: Any, ctype: str, errors: list[str]
):
    """Yield (table_name, headers, rows, truncated) for each queryable object.

    Per-object failures are collected into ``errors`` and skipped — one
    unreadable stream must not sink a query that only needs the others.
    """
    cfg = _connector_cfg(connector)
    used: set[str] = set()

    sdk_cls = None
    try:
        from connectors.sdk import get_sdk_connector

        sdk_cls = get_sdk_connector(ctype)
    except Exception:
        sdk_cls = None

    if sdk_cls is not None:
        # Manifest lives on the instance (auto-loaded from manifests/{name}.json).
        instance = sdk_cls(cfg)
        streams = list(getattr(instance.manifest, "streams", []) or [])
        for stream in streams[:_EXTRACT_MAX_OBJECTS]:
            try:
                headers, rows, truncated = _read_sdk_object(cfg, ctype, stream.name)
            except Exception as exc:
                errors.append(f"{stream.name}: {exc}")
                continue
            yield _sanitized_table_name(stream.name, used), headers, rows, truncated
        return

    if ctype in _STORE_TYPES:
        bucket = ""
        key = ""
        if ctype == "sftp":
            bucket = str(getattr(endpoint, "database", "") or cfg.get("database") or "")
            key = str(
                getattr(endpoint, "table", "")
                or getattr(endpoint, "connection_string", "")
                or cfg.get("table")
                or cfg.get("key")
                or ""
            )
        else:
            bucket = str(cfg.get("database") or "")
            key = str(
                getattr(endpoint, "table", "")
                or getattr(endpoint, "collection", "")
                or getattr(endpoint, "schema", "")
                or cfg.get("key")
                or cfg.get("table")
                or ""
            )
        if not key:
            raise ValueError(
                f"{ctype.upper()} query requires a configured object key/path"
            )
        import importlib

        mod = importlib.import_module(f"connectors.{ctype}_reader")
        headers, rows, truncated = _read_offset_object(
            mod.read_object, cfg, bucket=bucket, key=key
        )
        yield _sanitized_table_name(key, used), headers, rows, truncated
        return

    if ctype in _SAAS_TYPES:
        import importlib

        mod = importlib.import_module(f"connectors.{ctype}")
        object_name = str(
            getattr(endpoint, "table", "")
            or getattr(endpoint, "collection", "")
            or getattr(endpoint, "database", "")
            or cfg.get("table")
            or cfg.get("object")
            or ""
        )
        headers, rows, truncated = _read_offset_object(
            mod.read_object, cfg, object=object_name
        )
        yield (
            _sanitized_table_name(object_name or ctype, used),
            headers,
            rows,
            truncated,
        )
        return

    raise ValueError(f"no extract path for connector type {ctype!r}")


def run_extract_query(connector: Any, body: Any, *, workspace_id: str = ""):
    """Materialize connector objects into temp DuckDB and run safe SQL on it."""
    from fastapi import HTTPException

    ctype = (getattr(connector, "type", "") or "").lower().strip()
    if not connector_queryable_via_extract(ctype):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Query isn't supported for connector type '{ctype or 'unknown'}'. "
                "SQL engines run natively; extract-query currently covers SaaS "
                "sources, object stores (s3/gcs/adls/sftp) and SDK sources."
            ),
        )

    from services.connector_probe import endpoint_from_saved_connector

    endpoint = endpoint_from_saved_connector(
        getattr(connector, "id", ""),
        workspace_id=workspace_id,
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Connector not found")

    tmpdir = tempfile.mkdtemp(prefix="df_query_extract_")
    extract_truncated = False
    con = None
    try:
        import duckdb

        db_path = os.path.join(tmpdir, "extract.duckdb")
        con = duckdb.connect(db_path)

        n_tables = 0
        object_errors: list[str] = []
        try:
            for table_name, headers, rows, truncated in _extract_objects(
                connector, endpoint, ctype, object_errors
            ):
                extract_truncated = extract_truncated or truncated
                if not headers:
                    continue
                cols = ", ".join(f'"{h.replace(chr(34), chr(34)*2)}" VARCHAR' for h in headers)
                con.execute(f'CREATE TABLE "{table_name}" ({cols})')
                if rows:
                    placeholders = ", ".join("?" for _ in headers)
                    con.executemany(
                        f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                        rows,
                    )
                n_tables += 1
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to read {ctype} for querying: {exc}",
            ) from exc

        if n_tables == 0:
            detail = f"Connector '{ctype}' has no readable objects to query."
            if object_errors:
                detail += f" First failure: {object_errors[0]}"
            raise HTTPException(status_code=400, detail=detail)
        con.close()
        con = None

        proxy = SimpleNamespace(
            id=getattr(connector, "id", ""),
            type="duckdb",
            host="",
            port=0,
            database=db_path,
            username="",
            password="",
            connection_string="",
            schema="",
            warehouse="",
            ssl=False,
            extra={},
            auth_mode="",
        )
        from src.routers.query_router import _run_sql_query

        rows, columns, schema, truncated = _run_sql_query(proxy, body)
        return rows, columns, schema, bool(truncated or extract_truncated)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        shutil.rmtree(tmpdir, ignore_errors=True)
