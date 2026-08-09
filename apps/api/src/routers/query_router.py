"""Query Playground — run safe, limited queries against saved connectors."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from connectors.mongodb_common import (
    _mongo_client,
    mongodb_database_from_uri,
    normalize_mongodb_connection_string,
)
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from services import connector_store
from services.value_serializer import sanitize_json_value

router = APIRouter(prefix="/query", tags=["query"])

_MAX_ROWS = 10_000
# Rows sampled for result-set type inference. Bounded so a 10k-row preview
# does not pay a full scan just to label columns.
_TYPE_SAMPLE_ROWS = 200
_READ_ONLY_SQL_PATTERN = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)

_MONGODB_WRITE_STAGES = {"$out", "$merge"}


def _is_safe_sql(raw_query: str) -> bool:
    """Allow read and metadata queries; block any destructive or write SQL.

    Uses ``sqlparse`` when installed; otherwise a conservative regex fallback so
    Datawrap Pilot / query playground never hard-fail on a missing optional dep.
    """
    try:
        import sqlparse
        from sqlparse.sql import TokenList
        from sqlparse.tokens import Comment, Keyword, Newline, Whitespace
    except ImportError:
        return _is_safe_sql_fallback(raw_query)

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


def _is_safe_sql_fallback(raw_query: str) -> bool:
    """Conservative read-only gate when sqlparse is unavailable."""
    import re

    text = (raw_query or "").strip()
    if not text:
        return False
    # Reject multi-statement payloads.
    stripped = re.sub(r"--[^\n]*", " ", text)
    stripped = re.sub(r"/\*.*?\*/", " ", stripped, flags=re.S)
    if ";" in stripped.rstrip().rstrip(";"):
        return False
    upper = stripped.upper()
    if not re.match(
        r"^\s*(SELECT|WITH|EXPLAIN|SHOW|DESCRIBE|DESC|ANALYZE|PRAGMA|VALUES)\b",
        upper,
    ):
        return False
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|"
        r"EXEC|EXECUTE|MERGE|COPY|LOAD|REPLACE)\b",
        upper,
    ):
        return False
    if re.search(r"\bINTO\s+(OUTFILE|DUMPFILE)\b", upper) or re.search(
        r"\bSELECT\b[\s\S]*\bINTO\b", upper
    ):
        return False
    return True


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
    params: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Named bind parameters for :name placeholders. Filter values must be "
            "bound, never interpolated, so types survive and injection is impossible."
        ),
    )


class QueryExportRequest(QueryExecuteRequest):
    format: str = Field("csv", description="csv, json, jsonl, tsv, excel, parquet")
    output_path: str = Field("", description="Optional server-local path; empty uses exports folder")
    destination_connector_id: str = Field("", description="Optional saved connector to write results to instead of a file")
    destination: str = Field("", description="Target table, collection, or object name for destination_connector_id")
    sync_mode: str = Field("append", description="append, upsert, or overwrite (only used when writing to a connector)")
    conflict_columns: list[str] = Field(default_factory=list, description="Columns to use for upsert conflict resolution")


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    column_schema: dict[str, str]
    row_count: int
    truncated: bool
    # Server-side wall clock for the query itself, so the console reports
    # engine time rather than round-trip time.
    duration_ms: float = 0.0
    # How ``column_schema`` was derived. Result-set types are inferred from the
    # returned values, NOT read from the source DDL — a console must not imply
    # it is showing declared types. Transfer Studio's introspect path is the
    # authority for DDL identity.
    column_type_source: str = "inferred_from_values"


class QuerySchemaRequest(BaseModel):
    connector_id: str = Field(..., description="Saved connector id to introspect")
    database: str = Field("", description="Database/namespace override")
    schema_name: str = Field("", description="Schema override")
    object_name: str = Field("", description="Optional single table/collection to expand")


class QuerySchemaColumn(BaseModel):
    name: str
    type: str = ""
    nullable: bool | None = None
    primary_key: bool = False


class QuerySchemaObject(BaseModel):
    name: str
    type: str = "table"
    schema_name: str = ""
    columns: list[QuerySchemaColumn] = Field(default_factory=list)
    row_estimate: int = 0


class QuerySchemaResult(BaseModel):
    connected: bool
    connector_type: str = ""
    database: str = ""
    objects: list[QuerySchemaObject] = Field(default_factory=list)
    message: str = ""
    warnings: list[str] = Field(default_factory=list)
    # Whatever the connector's own introspection reported. Deliberately not
    # called "ddl": catalog-backed engines return declared types, while
    # dynamically typed sources (SQLite, Mongo, CSV) return value-inferred
    # ones, and this endpoint must not present the second as the first.
    type_source: str = "connector_introspection"


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


def _check_workspace_write(request: Request, workspace_id: str | None):
    from services.team_store import can_write_workspace
    actor = _actor(request)
    if workspace_id and not can_write_workspace(workspace_id, actor):
        raise HTTPException(status_code=403, detail="Workspace write access denied")


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

    started = time.perf_counter()
    try:
        rows, columns, schema, truncated = _run_query(connector, body)
        return QueryResult(
            columns=columns,
            rows=rows,
            column_schema=schema,
            row_count=len(rows),
            # A full page of rows means the limit may have cut the result short;
            # say so rather than implying the operator saw everything.
            truncated=truncated or len(rows) >= body.limit,
            duration_ms=round((time.perf_counter() - started) * 1000, 3),
        )
    except HTTPException:
        raise
    except Exception as exc:
        # Defensive: prevent an unhandled ExceptionGroup from crashing the worker.
        return JSONResponse(
            status_code=500,
            content={"detail": f"Query execution failed: {exc}"},
        )


@router.post("/export", response_model=QueryExportResult)
async def query_export(
    body: QueryExportRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """Run a query and export the results to a file or a destination connector."""
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)
    connector = connector_store.get_connector(body.connector_id, workspace_id=workspace_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        rows, columns, schema, _ = _run_query(connector, body)
    except HTTPException:
        raise
    except Exception as exc:
        return QueryExportResult(success=False, error=f"Query execution failed: {exc}", format=body.format)
    if not rows:
        return QueryExportResult(success=True, row_count=0, format=body.format)

    if body.destination_connector_id:
        return _export_to_connector(body, rows, columns, schema, workspace_id, request)

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


@router.post("/schema", response_model=QuerySchemaResult)
async def query_schema(
    body: QuerySchemaRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """List queryable objects for a connector, and columns for one object.

    Backs the console's schema browser and schema-aware autocomplete. Reuses
    the transfer engine's ``introspect_endpoint`` rather than adding a second
    introspection path, so the console sees exactly the objects and native
    types Transfer Studio sees.

    Two-phase by design: an unqualified call lists objects only, and columns
    are fetched per object on expand. Listing columns for every table up front
    is what makes schema browsers unusable on large estates.
    """
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)

    from services.connector_probe import endpoint_from_saved_connector

    endpoint = endpoint_from_saved_connector(
        body.connector_id,
        table=body.object_name,
        collection=body.object_name,
        schema=body.schema_name,
        database=body.database,
        workspace_id=workspace_id,
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Connector not found")

    from src.transfer.endpoint_intelligence import introspect_endpoint

    endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source"}
    try:
        info = introspect_endpoint(endpoint)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Query schema introspection failed for %s: %s", body.connector_id, exc
        )
        raise HTTPException(
            status_code=400, detail=f"Schema introspection failed: {exc}"
        ) from exc

    col_types: dict[str, str] = dict(info.get("schema") or {})
    col_names: list[str] = list(info.get("columns") or [])
    # Nullability and keys are already loaded by introspect_endpoint; dropping
    # them would force the browser into a second round trip for data it has.
    col_nulls: dict[str, Any] = dict(info.get("schema_nullability") or {})
    pk_cols = {
        str(c).lower() for c in (info.get("primary_key_columns") or []) if c
    }
    expanded: list[QuerySchemaColumn] = [
        QuerySchemaColumn(
            name=name,
            type=str(col_types.get(name) or ""),
            # Absent nullability stays None — "unknown", never a cheerful
            # nullable=True the catalog never said.
            nullable=(
                bool(col_nulls[name]) if name in col_nulls else None
            ),
            primary_key=name.lower() in pk_cols,
        )
        for name in col_names
    ]

    objects: list[QuerySchemaObject] = []
    for obj in info.get("objects") or []:
        if not isinstance(obj, dict):
            continue
        name = str(obj.get("name") or "")
        if not name:
            continue
        is_target = bool(body.object_name) and _object_matches(name, body.object_name)
        objects.append(
            QuerySchemaObject(
                name=name,
                type=str(obj.get("type") or "table"),
                schema_name=body.schema_name or endpoint.schema or "",
                # Columns only ever attach to the object that was asked for.
                columns=expanded if is_target else [],
                row_estimate=int(info.get("row_estimate") or 0) if is_target else 0,
            )
        )

    # A requested object that the connector does not list (e.g. a Mongo
    # collection created outside the sampled namespace) still returns its
    # columns rather than silently vanishing from the browser.
    if body.object_name and expanded and not any(o.columns for o in objects):
        objects.append(
            QuerySchemaObject(
                name=body.object_name,
                type="table",
                schema_name=body.schema_name or endpoint.schema or "",
                columns=expanded,
            )
        )

    return QuerySchemaResult(
        connected=bool(info.get("connected")),
        connector_type=str(info.get("format") or ""),
        database=body.database or endpoint.database or "",
        objects=objects,
        message=str(info.get("message") or ""),
        # Advisory-key honesty notes (BigQuery/Redshift/Snowflake do not
        # enforce PKs) must reach the console, not die at the adapter edge.
        warnings=[str(w) for w in (info.get("warnings") or []) if w],
    )


def _object_matches(listed: str, requested: str) -> bool:
    """Compare object names ignoring case and schema qualification.

    Introspection may list ``public.jobs`` while the browser asked for
    ``jobs`` (or the reverse), and Snowflake upper-cases everything.
    """
    a = listed.strip().strip('"').lower()
    b = requested.strip().strip('"').lower()
    return a == b or a.split(".")[-1] == b.split(".")[-1]


def _export_to_connector(
    body: QueryExportRequest,
    rows: list[dict],
    columns: list[str],
    schema: dict[str, str],
    workspace_id: str,
    request: Request,
) -> QueryExportResult:
    """Write query results to a saved database, warehouse, or object-store connector."""
    _check_workspace_write(request, workspace_id)
    dest_connector = connector_store.get_connector(body.destination_connector_id, workspace_id=workspace_id)
    if not dest_connector:
        raise HTTPException(status_code=404, detail="Destination connector not found")

    from services.dialect_profiles import normalize_schema

    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(
        kind="database",
        format=dest_connector.type,
        connector_id=dest_connector.id,
        host=dest_connector.host,
        port=dest_connector.port,
        database=dest_connector.database,
        schema=normalize_schema(
            dest_connector.type,
            dest_connector.schema,
            username=dest_connector.username,
        )
        or "",
        table=body.destination or "query_export",
        collection=body.destination or "query_export",
        username=dest_connector.username,
        password=dest_connector.password,
        connection_string=dest_connector.connection_string,
        warehouse=dest_connector.warehouse,
        ssl=dest_connector.ssl,
        auth_mode=dest_connector.auth_mode,
        auth_role=dest_connector.auth_role,
        auth_source=dest_connector.auth_source,
        api_key=dest_connector.api_key,
        service_account=dest_connector.service_account,
        endpoint_url=getattr(dest_connector, "endpoint_url", ""),
        path_style=getattr(dest_connector, "path_style", False),
    )

    mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]
    # Map aliases before the allow-list so overwrite → replace (not insert).
    write_mode = (body.sync_mode or "insert").strip().lower()
    if write_mode in {"overwrite", "full_refresh_overwrite", "truncate", "full_overwrite"}:
        write_mode = "replace"
    elif write_mode in {"append", "full_refresh_append", "full_append"}:
        write_mode = "insert"
    if write_mode not in ("insert", "upsert", "replace"):
        write_mode = "insert"

    try:
        rows_written, ddl_log, dest_summary = write_destination_database(
            dest,
            records=rows,
            columns=columns,
            schema=schema,
            mappings=mappings,
            write_mode=write_mode,
            conflict_columns=body.conflict_columns,
        )
        return QueryExportResult(
            success=True,
            row_count=rows_written,
            format=dest_connector.type,
            path=body.destination or "",
            filename=body.destination or "",
            download_url=dest_summary.get("download_url", ""),
        )
    except Exception as e:
        return QueryExportResult(success=False, error=str(e), format=dest_connector.type)


def _run_query(connector: connector_store.SavedConnector, body: QueryExecuteRequest):
    ctype = (connector.type or "").lower().strip()
    if ctype == "mongodb":
        return _run_mongodb_query(connector, body)
    if ctype == "snowflake":
        return _run_snowflake_query(connector, body)
    # MySQL-wire family must use pymysql dialect (handled in generic_sql._DRIVERNAME_MAP).
    return _run_sql_query(connector, body)


def _run_mongodb_query(connector, body):
    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MongoDB driver unavailable: {exc}") from exc

    conn_str = connector.connection_string or _build_mongodb_connection_string(connector)
    client = _mongo_client(conn_str)
    db_name = body.database or connector.database or mongodb_database_from_uri(conn_str) or "test"
    db = client[db_name]
    coll_name = body.collection or "data"
    coll = db[coll_name]

    query_filter = {}
    if body.query.strip():
        try:
            parsed = _parse_mongodb_json(body.query)
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


def _parse_mongodb_json(raw: str) -> Any:
    """Parse a Mongo filter/pipeline, honouring Extended JSON type wrappers.

    Plain ``json.loads`` turns ``{"$date": "..."}`` into a dict, so a date
    predicate silently matches nothing against a real BSON Date. ``json_util``
    reconstructs the typed values ($date, $oid, $regex) the server expects.
    """
    try:
        from bson import json_util

        return json_util.loads(raw)
    except ImportError:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail=f"Invalid MongoDB query: {exc}"
        ) from exc


def _normalize_rows(rows: list[dict]) -> tuple[list[dict], list[str], dict[str, str], bool]:
    if not rows:
        return [], [], {}, False
    keys = sorted({k for r in rows for k in r.keys()})
    cleaned = []
    for r in rows:
        cleaned.append({k: _jsonify_value(r.get(k)) for k in keys})
    return cleaned, keys, _column_schema(keys, cleaned), False


def _jsonify_value(value: Any) -> Any:
    """Return a JSON-safe Python value (no Python repr() artifacts)."""
    # Query display: null placeholder for NA/NaN is OK — write paths refuse.
    return sanitize_json_value(value, refuse_nonfinite=False)


def _column_schema(columns: list[str], rows: list[dict[str, Any]]) -> dict[str, str]:
    """Infer a canonical logical type per result column from returned values.

    Routes through ``schema_inference.infer_schema_map`` — the single choke
    point every other introspect path uses — so the console reports the same
    logical vocabulary as Map/Validate instead of labelling every column
    ``string``. This is inference over the result set, not the source DDL:
    callers surface it as such via ``QueryResult.column_type_source``.

    Inference never fails a query; if it raises, columns fall back to
    ``unknown`` rather than losing the rows the operator asked for.
    """
    if not columns:
        return {}
    if not rows:
        return {c: "unknown" for c in columns}
    try:
        from services.schema_inference import infer_schema_map

        samples_by_field = {
            c: [
                "" if r.get(c) is None else str(r.get(c))
                for r in rows[:_TYPE_SAMPLE_ROWS]
            ]
            for c in columns
        }
        schema, _intel = infer_schema_map(samples_by_field)
        return {c: str(schema.get(c) or "unknown") for c in columns}
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Result-set type inference failed; reporting unknown: %s", exc
        )
        return {c: "unknown" for c in columns}


def _build_mongodb_connection_string(connector) -> str:
    return normalize_mongodb_connection_string(
        connection_string=connector.connection_string or "",
        host=connector.host,
        port=connector.port or 27017,
        username=connector.username or "",
        password=connector.password or "",
        database=connector.database or "test",
    )


def _to_pyformat(sql: str, params: dict[str, Any]) -> str:
    """Rewrite ``:name`` placeholders to ``%(name)s`` for pyformat drivers."""
    if not params:
        return sql
    names = sorted(params.keys(), key=len, reverse=True)
    pattern = re.compile(
        r"(?<![:\w]):(" + "|".join(re.escape(n) for n in names) + r")\b"
    )
    return pattern.sub(lambda m: f"%({m.group(1)})s", sql)


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
    try:
        engine = get_sqlalchemy_engine(cfg)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not create database engine: {exc}") from exc

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
            result = conn.execute(text(clean_query), dict(body.params or {}))
            columns = list(result.keys())
            rows = []
            for i, row in enumerate(result):
                if i >= body.limit:
                    break
                rows.append({columns[j]: _jsonify_value(v) for j, v in enumerate(row)})
        return rows, columns, _column_schema(columns, rows), False
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc


def _run_snowflake_query(connector, body):
    """Run a read-only Snowflake query using the native connector.

    Avoids the snowflake-sqlalchemy dependency in production while still
    allowing the query playground to preview Snowflake data.
    """
    raw_query = body.query.strip()
    if not raw_query:
        raise HTTPException(status_code=400, detail="SQL query is required")
    if not _is_safe_sql(raw_query):
        raise HTTPException(status_code=400, detail="Only safe read/metadata queries are allowed in the playground")

    try:
        import snowflake.connector
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Snowflake driver unavailable: {exc}") from exc

    account = connector.host or connector.connection_string or ""
    # Strip the well-known domain suffix if the user entered the full host.
    if account.endswith(".snowflakecomputing.com"):
        account = account[: -len(".snowflakecomputing.com")]
    database = body.database or connector.database or ""
    schema = connector.schema or "PUBLIC"
    warehouse = connector.warehouse or ""
    role = getattr(connector, "auth_role", "")

    conn = None
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=connector.username or "",
            password=connector.password or "",
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not connect to Snowflake: {exc}") from exc

    try:
        clean_query = raw_query.rstrip(";")
        upper = clean_query.upper()
        append_limit = (
            not upper.startswith(("SHOW", "DESCRIBE", "EXPLAIN", "ANALYZE"))
            and " LIMIT " not in upper
        )
        if append_limit:
            clean_query = f"{clean_query} LIMIT {body.limit}"

        params = dict(body.params or {})
        if params:
            # snowflake-connector-python defaults to pyformat, so :name has to be
            # rewritten. Only names we were actually given are substituted, which
            # leaves ``::`` casts and time literals untouched.
            clean_query = _to_pyformat(clean_query, params)

        with conn.cursor() as cur:
            cur.execute(clean_query, params or None)
            description = cur.description or []
            columns = [desc[0] for desc in description]
            rows = []
            for i, row in enumerate(cur.fetchall()):
                if i >= body.limit:
                    break
                rows.append({columns[j]: _jsonify_value(v) for j, v in enumerate(row)})
        return rows, columns, _column_schema(columns, rows), False
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)
