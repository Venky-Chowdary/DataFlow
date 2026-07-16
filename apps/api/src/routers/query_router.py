"""Query Playground — run safe, limited queries against saved connectors."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services import connector_store

router = APIRouter(prefix="/query", tags=["query"])

_MAX_ROWS = 10_000
_READ_ONLY_SQL_PATTERN = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)

_MONGODB_WRITE_STAGES = {"$out", "$merge"}


def _is_safe_sql(raw_query: str) -> bool:
    """Allow read and metadata queries; block any destructive or write SQL."""
    import sqlparse
    from sqlparse.sql import TokenList
    from sqlparse.tokens import Comment, Keyword, Newline, Whitespace

    parsed = sqlparse.parse(raw_query.strip())
    if not parsed or len(parsed) != 1:
        return False

    stmt = parsed[0]

    def _walk_tokens(token):
        yield token
        if isinstance(token, TokenList):
            for child in token.tokens:
                yield from _walk_tokens(child)

    destructive = {
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE",
        "COPY", "LOAD",
    }
    safe_starts = {"SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "ANALYZE", "PRAGMA", "VALUES"}
    first_keyword = None

    for token in _walk_tokens(stmt):
        if token.ttype in (Whitespace, Newline) or Comment in (token.ttype, getattr(token.ttype, "__class__", None)):
            continue
        if token.is_whitespace:
            continue
        kw = token.value.upper() if token.value else ""
        if first_keyword is None and kw in safe_starts:
            first_keyword = kw
        if token.ttype in Keyword or (hasattr(token.ttype, "parents") and Keyword in token.ttype.parents):
            if kw in destructive:
                return False
            # SELECT ... INTO / WITH ... INTO creates tables; block it.
            if kw == "INTO" and first_keyword in {"SELECT", "WITH"}:
                return False

    # If sqlparse reports a concrete DML/DDL statement type that is not SELECT, reject it.
    stmt_type = (stmt.get_type() or "").upper()
    if stmt_type and stmt_type not in {"SELECT", "UNKNOWN"}:
        return False

    return first_keyword in safe_starts


def _validate_mongodb_aggregate(pipeline: list[dict]) -> None:
    for stage in pipeline:
        if isinstance(stage, dict):
            for key in stage:
                if key in _MONGODB_WRITE_STAGES:
                    raise HTTPException(
                        status_code=400,
                        detail=f"MongoDB aggregation stage '{key}' is not allowed in the query playground",
                    )


class QueryExecuteRequest(BaseModel):
    connector_id: str = Field(..., description="Saved connector id to query")
    query: str = Field(..., description="SQL SELECT or MongoDB JSON filter")
    database: str = Field("", description="Database/namespace")
    collection: str = Field("", description="Collection or table name")
    limit: int = Field(1000, ge=1, le=_MAX_ROWS)


class QueryExportRequest(QueryExecuteRequest):
    format: str = Field("csv", description="csv, json, jsonl, tsv, excel, parquet")
    output_path: str = Field("", description="Optional server-local path; empty uses exports folder")


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    column_schema: dict[str, str]
    row_count: int
    truncated: bool


class QueryExportResult(BaseModel):
    success: bool
    filename: str = ""
    download_url: str = ""
    path: str = ""
    row_count: int = 0
    format: str = ""
    error: str = ""


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def _check_workspace_read(request: Request, workspace_id: str | None):
    from services.team_store import can_read_workspace
    actor = _actor(request)
    if workspace_id and not can_read_workspace(workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace access denied")


@router.post("/execute", response_model=QueryResult)
async def query_execute(
    body: QueryExecuteRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """Run a read-only query against a saved connector and return rows."""
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)
    connector = connector_store.get_connector(body.connector_id, workspace_id=workspace_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    rows, columns, schema, truncated = _run_query(connector, body)
    return QueryResult(
        columns=columns,
        rows=rows,
        column_schema=schema,
        row_count=len(rows),
        truncated=truncated,
    )


@router.post("/export", response_model=QueryExportResult)
async def query_export(
    body: QueryExportRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """Run a query and export the results to a downloadable file."""
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)
    connector = connector_store.get_connector(body.connector_id, workspace_id=workspace_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    rows, columns, schema, _ = _run_query(connector, body)
    if not rows:
        return QueryExportResult(success=True, row_count=0, format=body.format)

    try:
        from src.transfer.adapters import write_destination_file
        from src.transfer.models import EndpointConfig

        dest = EndpointConfig(kind="file_export", format=body.format, output_path=body.output_path)
        export_bytes, export_name, dest_summary = write_destination_file(
            dest,
            records=rows,
            columns=columns,
            column_types=schema,
        )

        import uuid
        from pathlib import Path

        api_root = Path(__file__).resolve().parents[2]
        ext = Path(export_name).suffix.lstrip(".") or body.format
        if body.output_path:
            out_path = (api_root / body.output_path).resolve()
            if not str(out_path).startswith(str(api_root)):
                raise HTTPException(status_code=400, detail="Output path must be inside the application workspace")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            final_path = out_path
            filename = out_path.name
        else:
            export_dir = api_root / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            filename = f"query_{uuid.uuid4().hex[:16]}.{ext}"
            final_path = export_dir / filename

        final_path.write_bytes(export_bytes)
        return QueryExportResult(
            success=True,
            filename=filename,
            path=str(final_path),
            download_url=f"/api/v1/transfer/download/{filename}",
            row_count=len(rows),
            format=body.format,
        )
    except Exception as e:
        return QueryExportResult(success=False, error=str(e), format=body.format)


def _run_query(connector: connector_store.SavedConnector, body: QueryExecuteRequest):
    if connector.type == "mongodb":
        return _run_mongodb_query(connector, body)
    return _run_sql_query(connector, body)


def _run_mongodb_query(connector, body):
    try:
        import pymongo
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MongoDB driver unavailable: {exc}") from exc

    conn_str = connector.connection_string or _build_mongodb_connection_string(connector)
    client = pymongo.MongoClient(conn_str)
    db_name = body.database or connector.database or "test"
    db = client[db_name]
    coll_name = body.collection or "data"
    coll = db[coll_name]

    query_filter = {}
    if body.query.strip():
        try:
            parsed = json.loads(body.query)
            if isinstance(parsed, dict):
                query_filter = parsed
            elif isinstance(parsed, list):
                _validate_mongodb_aggregate(parsed)
                cursor = coll.aggregate(parsed[:_MAX_ROWS])
                rows = list(cursor)
                return _normalize_rows(rows)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid MongoDB filter JSON: {exc}") from exc

    cursor = coll.find(query_filter).limit(body.limit)
    rows = list(cursor)
    return _normalize_rows(rows)


def _normalize_rows(rows: list[dict]) -> tuple[list[dict], list[str], dict[str, str], bool]:
    if not rows:
        return [], [], {}, False
    keys = sorted({k for r in rows for k in r.keys()})
    cleaned = []
    for r in rows:
        cleaned.append({k: _jsonify_value(r.get(k)) for k in keys})
    schema = {k: "string" for k in keys}
    return cleaned, keys, schema, False


def _jsonify_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return json.loads(json.dumps(value, default=str))
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _build_mongodb_connection_string(connector) -> str:
    if connector.connection_string:
        return connector.connection_string
    if connector.username and connector.password:
        return f"mongodb://{connector.username}:{connector.password}@{connector.host}:{connector.port or 27017}/{connector.database or 'test'}"
    return f"mongodb://{connector.host}:{connector.port or 27017}/{connector.database or 'test'}"


def _run_sql_query(connector, body):
    raw_query = body.query.strip()
    if not raw_query:
        raise HTTPException(status_code=400, detail="SQL query is required")
    if not _is_safe_sql(raw_query):
        raise HTTPException(status_code=400, detail="Only safe read/metadata queries are allowed in the playground")

    from connectors.generic_sql import get_sqlalchemy_engine

    cfg = {
        "type": connector.type,
        "host": connector.host,
        "port": connector.port,
        "database": body.database or connector.database,
        "username": connector.username,
        "password": connector.password,
        "connection_string": connector.connection_string,
        "schema": connector.schema,
        "warehouse": connector.warehouse,
        "ssl": connector.ssl,
    }
    engine = get_sqlalchemy_engine(cfg)

    # Append a safe limit unless the user already supplied one or the query is metadata.
    clean_query = raw_query.rstrip(";")
    upper = clean_query.upper()
    append_limit = (
        not upper.startswith(("SHOW", "DESCRIBE", "EXPLAIN", "ANALYZE", "PRAGMA"))
        and " LIMIT " not in upper
        and " FETCH FIRST " not in upper
        and " TOP " not in upper
    )
    if append_limit:
        clean_query = f"{clean_query} LIMIT {body.limit}"

    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            result = conn.execute(text(clean_query))
            columns = list(result.keys())
            rows = []
            for i, row in enumerate(result):
                if i >= body.limit:
                    break
                rows.append({columns[j]: _jsonify_value(v) for j, v in enumerate(row)})
        schema = {c: "string" for c in columns}
        return rows, columns, schema, False
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc
