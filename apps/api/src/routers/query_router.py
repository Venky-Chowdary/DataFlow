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
import services.query_extract as services_query_extract
from services.connector_telemetry import connector_operation, driver_family
from services.log_redaction import redact_log_text
from services.safe_errors import public_error, safe_http_exception, user_query_error
from services.value_serializer import sanitize_json_value
from src.transfer.connector_registry import humanize_connection_error

router = APIRouter(prefix="/query", tags=["query"])

_MAX_ROWS = 10_000
# Rows sampled for result-set type inference. Bounded so a 10k-row preview
# does not pay a full scan just to label columns.
_TYPE_SAMPLE_ROWS = 200
_READ_ONLY_SQL_PATTERN = re.compile(r"^\s*SELECT\s+", re.IGNORECASE)

_MONGODB_WRITE_STAGES = {"$out", "$merge"}
# Operators that run server-side JavaScript; read-only playground never needs them.
_MONGODB_JS_OPERATORS = {"$where", "$function", "$accumulator", "$mapReduce"}
_MONGODB_FORBIDDEN_OPERATORS = _MONGODB_WRITE_STAGES | _MONGODB_JS_OPERATORS


def _is_safe_sql(raw_query: str) -> bool:
    """Allow read and metadata queries; block any destructive or write SQL.

    Uses ``sqlparse`` when installed; otherwise a conservative regex fallback so
    Datawrap Pilot / query playground never hard-fail on a missing optional dep.
    Every lexing in ``sql_code_views`` must pass: a quote inside a comment or a
    quoted identifier is not a literal, and text one engine reads as a comment
    can be a second statement on another.
    """
    try:
        import sqlparse  # noqa: F401
    except ImportError:
        return _is_safe_sql_fallback(raw_query)

    from services.sql_literals import sql_code_views

    if _raw_has_second_statement(raw_query):
        return False
    heads: set[str] = set()
    for code in sql_code_views(raw_query):
        head = _read_only_head(code)
        if head is None:
            return False
        heads.add(head)
    if _has_side_effect_functions(raw_query):
        return False
    if "PRAGMA" in heads and not _pragma_is_readonly(raw_query):
        return False
    return bool(heads)


_DESTRUCTIVE_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE",
    "COPY", "LOAD", "CALL", "DO", "LOCK", "UNLOCK",
})
_SAFE_STARTS = frozenset({
    "SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "ANALYZE", "PRAGMA", "VALUES",
})


def _raw_has_second_statement(raw_query: str) -> bool:
    """Cross-check on the unmasked text with sqlparse's own lexer.

    A ``;`` token followed by anything but comments is a second statement.
    """
    import sqlparse
    from sqlparse.tokens import Comment, Newline, Punctuation, Whitespace

    seen_semicolon = False
    for ttype, value in sqlparse.lexer.tokenize(raw_query or ""):
        if ttype in (Whitespace, Newline) or ttype in Comment:
            continue
        if ttype is Punctuation and value == ";":
            seen_semicolon = True
            continue
        if seen_semicolon:
            return True
    return False


def _read_only_head(code: str) -> str | None:
    """Safe leading keyword of one masked view, or ``None`` when it is a write."""
    import sqlparse
    from sqlparse.sql import TokenList
    from sqlparse.tokens import Comment, Keyword, Newline, Whitespace

    try:
        parsed = sqlparse.parse(code.strip())
    except Exception:
        # sqlparse's grouping pass refuses left-associative chains deeper than
        # 100 (a 250-column ``||`` concat is valid SQL — M19). Re-validate on
        # the ungrouped token stream: the keyword scan needs no nesting.
        return _flat_read_only_head(code)
    if not parsed or len(parsed) != 1:
        return None

    stmt = parsed[0]

    def _walk_tokens(token):
        yield token
        if isinstance(token, TokenList):
            for child in token.tokens:
                yield from _walk_tokens(child)

    first_keyword = None
    for token in _walk_tokens(stmt):
        if token.ttype in (Whitespace, Newline) or Comment in (token.ttype, getattr(token.ttype, "__class__", None)):
            continue
        if token.is_whitespace:
            continue
        kw = token.value.upper() if token.value else ""
        if first_keyword is None and kw in _SAFE_STARTS:
            first_keyword = kw
        if token.ttype in Keyword or (hasattr(token.ttype, "parents") and Keyword in token.ttype.parents):
            if kw in _DESTRUCTIVE_KEYWORDS:
                return None
            # SELECT ... INTO / WITH ... INTO creates tables; block it.
            if kw == "INTO" and first_keyword in {"SELECT", "WITH"}:
                return None

    # If sqlparse reports a concrete DML/DDL statement type that is not SELECT, reject it.
    stmt_type = (stmt.get_type() or "").upper()
    if stmt_type and stmt_type not in {"SELECT", "UNKNOWN"}:
        return None
    return first_keyword if first_keyword in _SAFE_STARTS else None


_READ_ONLY_REFUSAL = (
    "Only read-only SQL is allowed (SELECT / WITH / SHOW / DESCRIBE / EXPLAIN)."
)
_PLAYGROUND_REFUSAL = "Only safe read/metadata queries are allowed in the playground"
_STATEMENT_STARTS = frozenset({
    "select", "with", "explain", "show", "describe", "desc", "analyze", "pragma", "values",
})
_DESTRUCTIVE_HEADS = frozenset({
    "insert", "update", "delete", "drop", "create", "alter", "truncate",
    "grant", "revoke", "merge", "copy", "call", "exec", "execute", "do",
})


def sql_refusal_message(raw: str, *, playground: bool = False) -> str:
    """Name a syntax error. Keep the read-only sentence for a real write.

    ``SELEC * FROM t`` is not a write, so it must not be reported as
    "read-only only". ``DELETE`` and a second statement stay on the
    read-only refusal.
    """
    from services.sql_literals import sql_outside_literals

    readonly = _PLAYGROUND_REFUSAL if playground else _READ_ONLY_REFUSAL
    code = sql_outside_literals(raw or "").strip()
    head = ""
    if code:
        head = code.split(None, 1)[0].lower().rstrip(";")
    body = code.rstrip().rstrip(";")
    if head in _DESTRUCTIVE_HEADS or body.count(";") >= 1:
        return readonly
    if head not in _STATEMENT_STARTS:
        shown = head or (raw or "").strip()[:40]
        return (
            f"SQL syntax error: {shown!r} is not a statement the playground can run. "
            "Start with SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN."
        )
    return readonly


# SQLite PRAGMA is not in the documented read allowlist. Catalog reads the
# operator already uses (table_info, index_list) stay. An assignment
# (writable_schema, journal_mode=WAL) is a write and is refused.
_READONLY_PRAGMAS = frozenset({
    "table_info",
    "table_list",
    "table_xinfo",
    "index_info",
    "index_list",
    "index_xinfo",
    "database_list",
    "foreign_key_list",
    "foreign_key_check",
    "function_list",
    "module_list",
    "pragma_list",
    "compile_options",
    "collation_list",
    "integrity_check",
    "quick_check",
})


def _pragma_is_readonly(sql: str) -> bool:
    from services.sql_literals import sql_code_views

    return all(_pragma_view_is_readonly(code) for code in sql_code_views(sql or ""))


def _pragma_view_is_readonly(code: str) -> bool:
    match = re.match(
        r"\s*PRAGMA\s+(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)",
        code,
        re.I,
    )
    if not match:
        return False
    name = match.group(1).lower()
    head = code[match.end():].split("(", 1)[0]
    if "=" in head:
        return False
    return name in _READONLY_PRAGMAS


# Read-only is a *session* guarantee, not a keyword filter. A SELECT can still
# invoke volatile functions that mutate session/server state (QA R06):
# set_config flips transaction_read_only, nextval burns sequences,
# pg_terminate_backend kills connections, dblink/lo_* run side-channel writes.
# These names are blocked inside any read-path query, plus a session-level
# read-only mode is applied on dialects that support one.
_SIDE_EFFECT_FUNCTIONS = frozenset({
    # session/config mutation
    "set_config", "reset", "set", "use",
    # sequence mutation (burns values even in a rolled-back tx)
    "nextval", "setval", "currval",
    # backend control / admin
    "pg_terminate_backend", "pg_cancel_backend", "pg_reload_conf",
    "pg_rotate_logfile", "pg_promote", "pg_create_restore_point",
    "pg_switch_wal", "pg_switch_xlog", "pg_backup_start", "pg_backup_stop",
    "pg_control_checkpoint", "pg_logical_emit_message", "pg_notify",
    # replication slot mutation
    "pg_create_physical_replication_slot", "pg_drop_replication_slot",
    "pg_create_logical_replication_slot", "pg_logical_slot_get_changes",
    "pg_logical_slot_peek_changes", "pg_replication_slot_advance",
    # server filesystem read (information disclosure)
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "pg_stat_file",
    "pg_ls_logdir", "pg_ls_waldir", "pg_ls_archive_statusdir", "pg_ls_tmpdir",
    # advisory locks / denial of service
    "pg_advisory_lock", "pg_advisory_unlock", "pg_advisory_unlock_all",
    "pg_advisory_xact_lock", "pg_try_advisory_lock", "pg_try_advisory_xact_lock",
    "pg_advisory_xact_lock_shared", "pg_advisory_lock_shared",
    "pg_try_advisory_xact_lock_shared", "pg_try_advisory_lock_shared",
    "pg_sleep", "sleep", "benchmark",
    # side-channel drivers (pg dblink, mysql locks/files, mssql xp_)
    "get_lock", "release_lock", "load_file",
    "xp_cmdshell", "xp_dirtree", "xp_fileexist", "xp_regread", "xp_regwrite",
    "openrowset", "opendatasource", "bulk",
    # oracle administrative packages are prefixed dbms_/utl_ below
})
_SIDE_EFFECT_PREFIXES = (
    "dblink", "lo_", "dbms_", "utl_", "sys_exec", "sys_eval", "sp_",
)

_FUNC_CALL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _has_side_effect_functions(sql: str) -> bool:
    """True when a read query invokes a function with session/server side effects."""
    from services.sql_literals import sql_call_views

    for scrubbed in sql_call_views(sql or ""):
        for match in _FUNC_CALL_RE.finditer(scrubbed):
            name = match.group(1).lower()
            if name in _SIDE_EFFECT_FUNCTIONS:
                return True
            if any(name.startswith(prefix) for prefix in _SIDE_EFFECT_PREFIXES):
                return True
    return False


# DuckDB reads host files and URLs from a plain SELECT: reader table
# functions (read_csv, read_text, glob, *_scan), SQL-in-a-string runners
# (query(), json_execute_serialized_sql) and replacement scans
# (``FROM '/etc/passwd'``). A DuckDB file bind is exempt from the egress
# guard, so the playground refuses these up front and, for local DuckDB,
# also opens the connection with ``enable_external_access = false``.
_DUCKDB_EXTERNAL_FUNCTIONS = frozenset({
    "glob", "sniff_csv", "query", "query_table", "json_execute_serialized_sql",
    "getenv", "load_aws_credentials", "duckdb_secrets", "which_secret",
})
_DUCKDB_EXTERNAL_PREFIXES = (
    "read_", "parquet_", "iceberg_", "delta_", "ducklake_", "sqlite_",
    "postgres_", "mysql_", "odbc_", "http", "s3_", "azure_", "gcs_",
)
_DUCKDB_EXTERNAL_SUFFIXES = ("_scan",)
# Table references can follow FROM/JOIN or a comma of the FROM list. These
# keywords end the FROM list at their paren depth.
_DUCKDB_TOKEN_RE = re.compile(
    r'"(?:""|[^"])*"|__DFQ\d+__|[A-Za-z_][A-Za-z0-9_$]*|[(),]'
)
_DUCKDB_FROM_LIST_END = frozenset({
    "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "FETCH", "UNION",
    "EXCEPT", "INTERSECT", "QUALIFY", "WINDOW", "SELECT", "ON", "USING",
    "RETURNING", "SAMPLE",
})
# ``EXTRACT(year FROM '2020-01-01')`` and ``IS DISTINCT FROM 'x'`` take a
# value after FROM, not a table.
_DUCKDB_FROM_VALUE_FUNCTIONS = frozenset({"extract", "trim", "substring", "overlay", "position"})
_DUCKDB_PATH_HINT_RE = re.compile(
    r"[/\\:*?]|\.(?:csv|tsv|txt|parquet|json|jsonl|ndjson|arrow|xlsx|"
    r"db|duckdb|sqlite|gz|zst)$",
    re.I,
)
_DUCKDB_EXTERNAL_REFUSAL = (
    "DuckDB file and network readers (read_csv, read_text, glob, *_scan, "
    "FROM '<path>') are not allowed in the playground. Query tables in the "
    "connected database."
)


def is_duckdb_engine(engine: Any) -> bool:
    url = getattr(engine, "url", None)
    return str(getattr(url, "drivername", "") or "").lower().startswith("duckdb")


def _duckdb_reads_external(sql: str) -> bool:
    """True when DuckDB SQL would read a host file/URL or run SQL from a string.

    Comments and string literals are lexed away first (``sql_outside_literals``),
    so a ``'--'`` literal cannot swallow a reader call and a quoted function
    name is still a call. The connect-time ``enable_external_access = false``
    in :func:`_duckdb_playground_engine` is the backstop for local DuckDB.
    """
    from services.sql_literals import sql_outside_literals

    tokens = _DUCKDB_TOKEN_RE.findall(sql_outside_literals(sql or ""))
    # Per paren depth: [inside a FROM list, FROM takes a value (EXTRACT & co)].
    levels: list[list[bool]] = [[False, False]]
    expect_table = False
    for i, tok in enumerate(tokens):
        upper = tok.upper()
        prev = tokens[i - 1].lower() if i else ""
        if tok == "(":
            levels.append([False, prev in _DUCKDB_FROM_VALUE_FUNCTIONS])
            continue
        if tok == ")":
            if len(levels) > 1:
                levels.pop()
            expect_table = False
            continue
        if tok == ",":
            expect_table = levels[-1][0]
            continue
        if upper in ("FROM", "JOIN"):
            if upper == "FROM" and (levels[-1][1] or prev == "distinct"):
                expect_table = False
            else:
                levels[-1][0] = True
                expect_table = True
            continue
        if upper in _DUCKDB_FROM_LIST_END:
            levels[-1][0] = False
            expect_table = False
            continue
        if expect_table:
            if tok.startswith("__DFQ"):
                return True
            if tok.startswith('"') and _DUCKDB_PATH_HINT_RE.search(tok[1:-1].replace('""', '"')):
                return True
        expect_table = False
        # A quoted function name is still a call: ``"read_csv"('/x')``.
        if i + 1 < len(tokens) and tokens[i + 1] == "(":
            name = tok[1:-1].replace('""', '"') if tok.startswith('"') else tok
            name = name.lower()
            if (
                name in _DUCKDB_EXTERNAL_FUNCTIONS
                or name.startswith(_DUCKDB_EXTERNAL_PREFIXES)
                or name.endswith(_DUCKDB_EXTERNAL_SUFFIXES)
            ):
                return True
    return False


def _duckdb_playground_engine(engine: Any) -> Any:
    """Same DuckDB target, opened with external (file/network) access disabled.

    ``SET enable_external_access = false`` at runtime is global to the
    database instance and cannot be undone while it is open, so a shared
    connection would be crippled for a concurrent transfer. A connect-time
    config keeps it scoped to this playground connection. MotherDuck
    (``md:``) loads its extension through external access, so it relies on
    the SQL refusal alone.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    database = str(getattr(engine.url, "database", "") or "")
    if database.startswith("md:"):
        return engine
    guarded = create_engine(
        engine.url,
        poolclass=NullPool,
        connect_args={"config": {"enable_external_access": False}},
    )
    guarded.dialect.supports_native_decimal = True
    return guarded


def _is_safe_sql_flat(raw_query: str) -> bool:
    """Keyword scan over the ungrouped token stream (deep-chain fallback).

    sqlparse's grouping pass refuses left-associative chains deeper than 100
    (a 250-column ``||`` concat is valid SQL — M19). The read-only policy —
    one statement, a safe head, no destructive keywords, no SELECT INTO, the
    existing side-effect/pragma checks — needs no nesting, so this walks the
    lexer's flat token stream of every ``sql_code_views`` lexing instead.
    """
    from services.sql_literals import sql_code_views

    if _raw_has_second_statement(raw_query):
        return False
    heads: set[str] = set()
    for code in sql_code_views(raw_query):
        head = _flat_read_only_head(code)
        if head is None:
            return False
        heads.add(head)
    if _has_side_effect_functions(raw_query):
        return False
    if "PRAGMA" in heads and not _pragma_is_readonly(raw_query):
        return False
    return bool(heads)


def _flat_read_only_head(code: str) -> str | None:
    import sqlparse
    from sqlparse.tokens import Comment, Keyword, Newline, Punctuation, Whitespace

    first_keyword = None
    for ttype, value in sqlparse.lexer.tokenize(code.strip().rstrip(";")):
        if ttype in (Whitespace, Newline) or ttype in Comment:
            continue
        kw = str(value or "").upper()
        if ttype is Punctuation and kw == ";":
            # A real semicolon token means a second statement follows.
            return None
        if first_keyword is None and kw in _SAFE_STARTS:
            first_keyword = kw
        if ttype in Keyword or (
            hasattr(ttype, "parents") and Keyword in ttype.parents
        ):
            if kw in _DESTRUCTIVE_KEYWORDS:
                return None
            if kw == "INTO" and first_keyword in {"SELECT", "WITH"}:
                return None
    return first_keyword if first_keyword in _SAFE_STARTS else None


def _is_safe_sql_fallback(raw_query: str) -> bool:
    """Conservative read-only gate when sqlparse is unavailable."""
    from services.sql_literals import sql_code_views

    text = (raw_query or "").strip()
    if not text:
        return False
    views = sql_code_views(text)
    if not all(_fallback_view_is_safe(code, text) for code in views):
        return False
    return not _has_side_effect_functions(text)


def _fallback_view_is_safe(code: str, text: str) -> bool:
    # Reject multi-statement payloads. Literals and comments are masked first
    # so a semicolon or DROP inside quotes is not a second statement.
    if ";" in code.rstrip().rstrip(";"):
        return False
    upper = code.upper()
    if not re.match(
        r"^\s*(SELECT|WITH|EXPLAIN|SHOW|DESCRIBE|DESC|ANALYZE|PRAGMA|VALUES)\b",
        upper,
    ):
        return False
    if upper.lstrip().startswith("PRAGMA") and not _pragma_is_readonly(text):
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


def _reject_mongodb_operator(key: str) -> None:
    kind = "aggregation stage" if key in _MONGODB_WRITE_STAGES else "operator"
    raise HTTPException(
        status_code=400,
        detail=f"MongoDB {kind} '{key}' is not allowed in the query playground",
    )


def _validate_mongodb_query_tree(node: Any) -> None:
    """Reject write stages and server-side JavaScript anywhere in a filter/pipeline.

    Walks every nested document and array, so sub-pipelines (``$lookup``,
    ``$facet``, ``$unionWith``) and ``$expr`` bodies are covered too.
    """
    try:
        from bson.code import Code
    except ImportError:  # pragma: no cover - pymongo ships bson
        Code = ()  # type: ignore[assignment]

    stack = [node]
    while stack:
        current = stack.pop()
        if Code and isinstance(current, Code):
            _reject_mongodb_operator("$code")
        if isinstance(current, dict):
            for key, value in current.items():
                if isinstance(key, str) and key in _MONGODB_FORBIDDEN_OPERATORS:
                    _reject_mongodb_operator(key)
                stack.append(value)
        elif isinstance(current, (list, tuple)):
            stack.extend(current)


def _validate_mongodb_aggregate(pipeline: list[dict]) -> None:
    _validate_mongodb_query_tree(pipeline)


def _validate_mongodb_filter(query_filter: dict) -> None:
    _validate_mongodb_query_tree(query_filter)


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
    output_path: str = Field(
        "",
        description="Optional file name inside the server exports folder; empty generates one",
    )
    destination_connector_id: str = Field("", description="Optional saved connector to write results to instead of a file")
    destination: str = Field("", description="Target table, collection, or object name for destination_connector_id")
    sync_mode: str = Field(
        "append",
        description="append only when writing to a connector; overwrite/upsert must go through Transfer Studio",
    )
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
    objects_truncated: bool = False
    # Whatever the connector's own introspection reported. Deliberately not
    # called "ddl": catalog-backed engines return declared types, while
    # dynamically typed sources (SQLite, Mongo, CSV) return value-inferred
    # ones, and this endpoint must not present the second as the first.
    type_source: str = "connector_introspection"


class QueryCatalogNode(BaseModel):
    kind: str
    name: str
    children: list["QueryCatalogNode"] = Field(default_factory=list)


class QuerySchemasResult(BaseModel):
    connected: bool
    connector_type: str = ""
    database: str = ""
    schema_name: str = ""
    nodes: list[QueryCatalogNode] = Field(default_factory=list)
    listed_live: bool = False
    message: str = ""


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
    from services.effective_principal import request_may_read_workspace

    if workspace_id and not request_may_read_workspace(request, workspace_id):
        raise HTTPException(status_code=403, detail="Workspace access denied")


def _check_workspace_write(request: Request, workspace_id: str | None):
    from services.effective_principal import request_may_write_workspace

    if workspace_id and not request_may_write_workspace(request, workspace_id):
        raise HTTPException(status_code=403, detail="Workspace write access denied")


def _check_connector_read(request: Request, connector_id: str | None) -> None:
    from services.workspace_access import assert_connector_accessible

    assert_connector_accessible(request, connector_id)


def _check_connector_write(request: Request, connector_id: str | None) -> None:
    from services.workspace_access import assert_connector_accessible

    assert_connector_accessible(request, connector_id, write=True)


@router.post("/execute", response_model=QueryResult)
async def query_execute(
    body: QueryExecuteRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """Run a read-only query against a saved connector and return rows."""
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)
    _check_connector_read(request, body.connector_id)
    connector = connector_store.get_connector(body.connector_id, workspace_id=workspace_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    started = time.perf_counter()
    try:
        rows, columns, schema, truncated = _run_query(
            connector, body, workspace_id=workspace_id
        )
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
        detail, cid = public_error(
            exc,
            event="query.execute.errored",
            public_message="Query execution failed.",
            status_code=500,
        )
        return JSONResponse(
            status_code=500,
            content={
                "detail": detail,
                "correlation_id": cid,
            },
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
    _check_connector_read(request, body.connector_id)
    connector = connector_store.get_connector(body.connector_id, workspace_id=workspace_id)
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        rows, columns, schema, _ = _run_query(
            connector,
            body,
            surface="query_export",
            json_engine_text=True,
            workspace_id=workspace_id,
        )
    except HTTPException:
        raise
    except Exception as exc:
        detail, _cid = public_error(
            exc,
            event="query.export.errored",
            public_message="Query execution failed.",
        )
        return QueryExportResult(
            success=False,
            error=detail,
            format=body.format,
        )
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

        from services.export_paths import (
            ExportPathError,
            generated_export_path,
            resolve_export_path,
        )

        ext = Path(export_name).suffix.lstrip(".") or body.format
        try:
            if body.output_path.strip():
                final_path = resolve_export_path(body.output_path, default_ext=ext)
            else:
                final_path = generated_export_path(f"query_{uuid.uuid4().hex[:16]}.{ext}")
        except ExportPathError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        final_path.parent.mkdir(parents=True, exist_ok=True)
        filename = final_path.name

        final_path.write_bytes(export_bytes)
        return QueryExportResult(
            success=True,
            filename=filename,
            path=str(final_path),
            download_url=f"/api/v1/transfer/download/{filename}",
            row_count=len(rows),
            format=body.format,
        )
    except HTTPException:
        raise
    except Exception as exc:
        detail, _cid = public_error(
            exc,
            event="query.export.errored",
            public_message="Export failed.",
        )
        return QueryExportResult(
            success=False,
            error=detail,
            format=body.format,
        )


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
    _check_connector_read(request, body.connector_id)

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

    endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source", "introspect_strict": True}
    try:
        info = introspect_endpoint(endpoint)
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Query schema introspection failed for %s: %s", body.connector_id, exc
        )
        driver = getattr(endpoint, "format", "") or ""
        _ep_mode = str(
            (getattr(endpoint, "extra", None) or {}).get("auth_mode")
            or getattr(endpoint, "auth_mode", "")
            or ""
        )
        raise HTTPException(
            status_code=400,
            detail=f"Schema introspection failed: {humanize_connection_error(driver, exc, auth_mode=_ep_mode)}",
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
        objects_truncated=bool(info.get("objects_truncated")),
    )


@router.post("/schemas", response_model=QuerySchemasResult)
async def query_schemas(
    body: QuerySchemaRequest,
    request: Request,
    x_workspace_id: str | None = Header(None, alias="X-Workspace-Id"),
):
    """Databases and schemas this connector can name.

    Snowflake asks the account. Other engines return the configured database
    and schema as one node. A connector that names neither still returns one
    node so the browser is not an empty catalog.
    """
    workspace_id = x_workspace_id or ""
    _check_workspace_read(request, workspace_id)
    _check_connector_read(request, body.connector_id)

    from services.connector_probe import endpoint_from_saved_connector
    from services.query_catalog import build_catalog_tree, live_schema_names

    endpoint = endpoint_from_saved_connector(
        body.connector_id,
        schema=body.schema_name,
        database=body.database,
        workspace_id=workspace_id,
    )
    if endpoint is None:
        raise HTTPException(status_code=404, detail="Connector not found")

    live = live_schema_names(endpoint)
    tree = build_catalog_tree(
        connector_type=str(getattr(endpoint, "format", "") or ""),
        database=body.database or str(getattr(endpoint, "database", "") or ""),
        schema=body.schema_name or str(getattr(endpoint, "schema", "") or ""),
        live_schemas=live,
    )
    return QuerySchemasResult(**tree)


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
    _check_connector_write(request, body.destination_connector_id)
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
    # Query export is a playground side door. Overwrite/upsert skip Validate,
    # population-fit, and quarantine — Transfer Studio is the write path that
    # proves those gates. Append/insert only.
    write_mode = (body.sync_mode or "insert").strip().lower()
    if write_mode in {"overwrite", "full_refresh_overwrite", "truncate", "full_overwrite", "replace"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "Query export cannot overwrite a destination table. "
                "Use Transfer Studio Validate → Execute so population-fit and "
                "quarantine run before any replace."
            ),
        )
    if write_mode in {"upsert", "incremental", "incremental_deduped", "cdc", "merge"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "Query export cannot upsert. Use Transfer Studio for MERGE/"
                "conflict writes so the primary key and preflight gates apply."
            ),
        )
    if write_mode in {"append", "full_refresh_append", "full_append"}:
        write_mode = "insert"
    if write_mode != "insert":
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
    except Exception as exc:
        detail, _cid = public_error(
            exc,
            event="query.export.errored",
            public_message="Export failed.",
        )
        return QueryExportResult(
            success=False,
            error=detail,
            format=dest_connector.type,
        )


def _run_query(
    connector: connector_store.SavedConnector,
    body: QueryExecuteRequest,
    *,
    surface: str = "query",
    json_engine_text: bool = False,
    workspace_id: str = "",
):
    ctype = (connector.type or "").lower().strip()
    with connector_operation(
        "query",
        driver_family=driver_family(ctype),
        role="source",
        surface=surface,
        connector_id=connector.id,
    ) as op:
        try:
            if ctype == "mongodb":
                result = _run_mongodb_query(connector, body)
            elif ctype == "snowflake":
                result = _run_snowflake_query(connector, body)
            elif services_query_extract.connector_queryable_via_extract(ctype):
                # Non-SQL connectors (s3, SaaS, SDK sources, stores) have no
                # SQLAlchemy dialect — extract a bounded snapshot into DuckDB
                # and run the same guarded SQL path instead of hard-failing.
                # Unknown/other types keep the SQL path and its honest dialect
                # error (kafka is a stream, not a queryable snapshot).
                result = services_query_extract.run_extract_query(
                    connector, body, workspace_id=workspace_id
                )
            else:
                # MySQL-wire family must use pymysql dialect (handled in generic_sql._DRIVERNAME_MAP).
                result = _run_sql_query(connector, body, json_engine_text=json_engine_text)
        except HTTPException as exc:
            if exc.status_code < 500:
                op.failed(category="invalid_request", error=exc)
            raise
        rows, columns, *_ = result
        op.succeeded(row_count=len(rows), column_count=len(columns))
        return result


def _run_mongodb_query(connector, body):
    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise safe_http_exception(
            exc,
            event="query.mongodb_driver.unavailable",
            public_message="MongoDB driver unavailable.",
            status_code=500,
        ) from exc

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
                _validate_mongodb_filter(parsed)
                query_filter = parsed
            elif isinstance(parsed, list):
                _validate_mongodb_aggregate(parsed)
                cursor = coll.aggregate(parsed[:_MAX_ROWS])
                rows = list(cursor)
                return _normalize_rows(rows)
        except json.JSONDecodeError as exc:
            raise user_query_error(
                exc,
                public_message="Invalid MongoDB filter JSON",
                status_code=400,
            ) from exc

    cursor = coll.find(query_filter).limit(body.limit)
    rows = list(cursor)
    return _normalize_rows(rows)


def _parse_mongodb_json(raw: str) -> Any:
    """Parse a Mongo filter/pipeline, honouring Extended JSON type wrappers.

    ``json_util.loads`` rebuilds ``$date`` / ``$oid`` / ``$numberDecimal``, but
    it still calls stdlib ``json.loads``, so a long fraction in a predicate
    collapses to IEEE before ``find()``. ``parse_float=Decimal`` plus
    ``demote_exact_json`` keeps digits; ``object_hook`` still rebuilds BSON.
    """
    from decimal import Decimal

    from services.value_serializer import demote_exact_json, json_loads_exact

    try:
        from bson import json_util
    except ImportError:
        return json_loads_exact(raw)
    try:
        parsed = json.loads(
            raw,
            parse_float=Decimal,
            object_hook=json_util.object_hook,
        )
    except json.JSONDecodeError:
        raise
    except Exception as exc:
        raise user_query_error(
            exc,
            public_message="Invalid MongoDB query",
            status_code=400,
        ) from exc
    return demote_exact_json(parsed)


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


def _unique_column_names(columns: list[str]) -> list[str]:
    """Disambiguate duplicate result column names.

    ``{col: value}`` row dicts silently collapse duplicate projection names —
    ``SELECT count(*), count(*)`` returns one ``count`` key and the second
    column is unrecoverable (QA R23, silent data loss in reconciliation output).
    Suffix the nth duplicate so every projected value survives and ``columns``
    still documents the mapping.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in columns:
        key = str(name)
        count = seen.get(key, 0) + 1
        seen[key] = count
        out.append(key if count == 1 else f"{key}__{count}")
    return out


def _apply_read_only_session(conn: Any, db_type: str) -> None:
    """Enforce read-only at the session level where the dialect supports it.

    The keyword filter is the first line of defense; a session-level READ ONLY
    mode is the second (QA R06). Best-effort — an unsupported dialect falls back
    to the keyword + side-effect-function filters rather than failing the query.
    """
    db = (db_type or "").lower()
    try:
        from sqlalchemy import text

        if db in {"postgresql", "postgres", "cockroachdb", "redshift", "greenplum", "yugabytedb"}:
            conn.execute(text("SET TRANSACTION READ ONLY"))
        elif db in {"mysql", "mariadb", "singlestore", "tidb"}:
            # Transaction-scoped, not SESSION: a pooled connection must not
            # hand a stale read-only mode to the next write query.
            conn.execute(text("SET TRANSACTION READ ONLY"))
        elif db == "sqlite":
            # DuckDB has no query_only PRAGMA; the failed statement aborts the
            # transaction. Its guard is the connect-time config in
            # _duckdb_playground_engine.
            conn.execute(text("PRAGMA query_only = ON"))
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "read-only session option not applied on %s: %s", db, exc
        )


#: Interactive statement bound; ``0`` switches it off (D181).
DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS = 120.0


def _apply_statement_timeout(conn: Any, db_type: str) -> None:
    """Have the server cancel an interactive query that outlives the tool deadline.

    The deadline frees the caller; this frees the database and the worker, which
    would otherwise keep executing for nobody. Best-effort per dialect.
    """
    import os

    raw = os.environ.get("DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS", "").strip()
    try:
        seconds = float(raw) if raw else DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS
    except ValueError:
        seconds = DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS
    if seconds <= 0:
        return
    from src.ai.copilot.tool_deadline import statement_timeout_seconds

    budget = statement_timeout_seconds()
    if budget is not None:
        # Never past the calling tool's own deadline (D181).
        seconds = min(seconds, budget)
    millis = int(seconds * 1000)
    db = (db_type or "").lower()
    if db in {"postgresql", "postgres", "cockroachdb", "greenplum", "yugabytedb"}:
        statement = f"SET LOCAL statement_timeout = {millis}"
    elif db in {"mysql", "tidb"}:
        statement = f"SET SESSION MAX_EXECUTION_TIME = {millis}"
    elif db in {"sqlserver", "mssql"}:
        # Bounds lock waits, the usual reason a read stalls; SQL Server has no
        # per-statement execution cap.
        statement = f"SET LOCK_TIMEOUT {millis}"
    else:
        return
    try:
        from sqlalchemy import text

        conn.execute(text(statement))
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "statement timeout not applied on %s: %s", db, exc
        )


def _catalog_declared_columns(
    conn: Any, sql: str, columns: list[str], dialect: str
) -> dict[str, str]:
    """Declared types of the base table's columns for single-table reads.

    Value inference mislabels TEXT columns whose values look numeric
    ('DECIMAL(8,4)' on a TEXT c126 — M20); the engine's own catalog is the
    honest source. Multi-table/expression queries keep the inference labels.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return {}
    try:
        tree = sqlglot.parse_one(sql)
        tables = [
            t.name for t in tree.find_all(exp.Table)
            if not (t.catalog or t.db)
        ]
        if len(tables) != 1:
            return {}
        table = tables[0]
        db = (dialect or "").lower()
        declared: dict[str, str] = {}
        from sqlalchemy import text as _text

        if db in {"sqlite", "duckdb"}:
            rows = conn.execute(
                _text(f"PRAGMA table_info({_quote_sqlite_ident(table)})")
            ).fetchall()
            declared = {str(r[1]): str(r[2] or "unknown") for r in rows}
        elif db in {"postgresql", "postgres", "mysql", "mariadb",
                    "cockroachdb", "redshift", "greenplum", "yugabytedb",
                    "singlestore", "tidb"}:
            rows = conn.execute(
                _text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = :t"
                ),
                {"t": table},
            ).fetchall()
            declared = {str(r[0]): str(r[1] or "unknown") for r in rows}
        else:
            return {}
        lower = {k.lower(): v for k, v in declared.items()}
        return {
            c: lower[c.lower()] for c in columns if c.lower() in lower
        }
    except Exception:
        return {}


def _quote_sqlite_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


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


def _is_oracle_connector(connector_type: object) -> bool:
    from connectors.generic_sql import _is_oracle_wire

    return _is_oracle_wire("", str(connector_type or ""))


def _run_sql_query(connector, body, *, json_engine_text: bool = False):
    raw_query = body.query.strip()
    if not raw_query:
        raise HTTPException(status_code=400, detail="SQL query is required")
    if not _is_safe_sql(raw_query):
        raise HTTPException(
            status_code=400,
            detail=sql_refusal_message(raw_query, playground=True),
        )

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
        # TLS trust / SID / driver keywords the operator saved (QA round 11).
        "extra": dict(getattr(connector, "extra", None) or {}),
    }
    try:
        engine = get_sqlalchemy_engine(cfg)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=400, detail=redact_log_text(str(exc))
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=humanize_connection_error(
                connector.type or "", exc,
                auth_mode=str(getattr(connector, "auth_mode", "") or ""),
            ),
        ) from exc

    if is_duckdb_engine(engine):
        if _duckdb_reads_external(raw_query):
            raise HTTPException(status_code=400, detail=_DUCKDB_EXTERNAL_REFUSAL)
        engine = _duckdb_playground_engine(engine)

    from services.dialect_profiles import append_result_limit

    # Oracle/DB2 reject LIMIT (ORA-03047). A query that already has FETCH or TOP
    # is left alone — that is why a hand-written Oracle extract succeeded while
    # a table sample did not.
    # Fetch limit+1: one row past the cap is how we know rows were withheld —
    # a LIMIT at exactly `limit` can never distinguish "capped" from "short".
    clean_query = append_result_limit(connector.type, raw_query, body.limit + 1)

    restore_json = lambda: None  # noqa: E731
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            _apply_read_only_session(conn, connector.type)
            _apply_statement_timeout(conn, connector.type)
            from services.sql_literals import escape_sqlalchemy_literal_colons

            if json_engine_text:
                # Samples and exports carry a JSON cell as its document text
                # (D207: a jsonb "hello" arrived as bare hello, null as NULL).
                from services.json_polarity import keep_pg_json_engine_text

                restore_json = keep_pg_json_engine_text(conn)

            tz_positions: frozenset[int] = frozenset()
            if _is_oracle_connector(connector.type):
                # python-oracledb reads TIMESTAMP WITH [LOCAL] TIME ZONE naive,
                # and this result feeds the preflight sample: a naive cell for
                # a tz-aware target blocked g3 (D155).
                from connectors.oracle_tstz import tz_projected_query

                def _probe_description(sql: str):
                    probe = conn.execute(
                        text(escape_sqlalchemy_literal_colons(sql)),
                        dict(body.params or {}),
                    )
                    desc = getattr(getattr(probe, "cursor", None), "description", None)
                    probe.close()
                    return desc

                clean_query, tz_positions = tz_projected_query(
                    _probe_description, clean_query
                )
            result = conn.execute(
                text(escape_sqlalchemy_literal_colons(clean_query)),
                dict(body.params or {}),
            )
            columns = _unique_column_names(list(result.keys()))
            raw_desc = getattr(getattr(result, "cursor", None), "description", None)
            description = (
                tuple(tuple(col) if col is not None else None for col in raw_desc)
                if raw_desc
                else None
            )
            withheld = 0
            rows = []
            for i, row in enumerate(result):
                if i >= body.limit:
                    withheld += 1
                    break
                if tz_positions:
                    from connectors.oracle_tstz import parse_oracle_tz_row

                    row = parse_oracle_tz_row(row, tz_positions)
                rows.append({columns[j]: _jsonify_value(v) for j, v in enumerate(row)})
            try:
                result.close()
            except Exception:
                pass
            restore_json()
            # Name custom enums after the result is closed. A pg_type
            # lookup on the open cursor cancels the extract.
            from services.decimal_observe import annotate_unresolved_pg_types

            description = annotate_unresolved_pg_types(
                conn, description, dialect=str(connector.type or "")
            )
            # No cursor types (SQLite) — the base table's catalog DDL is the
            # honest source for single-table reads, not value-guessing (M20).
            catalog_declared = _catalog_declared_columns(
                conn, raw_query, columns, str(connector.type or "")
            )
        schema = _column_schema(columns, rows)
        from services.decimal_observe import cursor_declared_carriers

        declared = cursor_declared_carriers(
            columns, description, dialect=str(connector.type or "")
        )
        if declared:
            schema = {**schema, **declared}
        schema = {**schema, **catalog_declared}
        return rows, columns, schema, bool(withheld)
    except Exception as exc:
        raise user_query_error(
            exc,
            public_message="Query failed",
            status_code=400,
        ) from exc
    finally:
        restore_json()  # idempotent; the pooled connection decodes again


def _run_snowflake_query(connector, body):
    """Run a read-only Snowflake query using the native connector.

    Avoids the snowflake-sqlalchemy dependency in production while still
    allowing the query playground to preview Snowflake data.
    """
    raw_query = body.query.strip()
    if not raw_query:
        raise HTTPException(status_code=400, detail="SQL query is required")
    if not _is_safe_sql(raw_query):
        raise HTTPException(
            status_code=400,
            detail=sql_refusal_message(raw_query, playground=True),
        )

    from connectors.snowflake_conn import classify_snowflake_connect_error, get_connection
    from services.connector_auth import engine_login_role

    database = body.database or connector.database or ""
    schema = connector.schema or "PUBLIC"
    warehouse = connector.warehouse or ""

    conn = None
    try:
        conn = get_connection(
            account=connector.host or "",
            username=connector.username or "",
            password=connector.password or "",
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=getattr(connector, "connection_string", "") or "",
            role=engine_login_role(
                getattr(connector, "auth_role", ""),
                getattr(connector, "role", ""),
            ),
            private_key=getattr(connector, "private_key", "") or "",
            private_key_passphrase=connector.password or "",
        )
    except Exception as exc:
        classified = classify_snowflake_connect_error(
            redact_log_text(str(exc)),
            auth_mode=str(getattr(connector, "auth_mode", "") or "").strip().lower(),
        )
        connection_detail = humanize_connection_error("snowflake", classified or exc)
        raise HTTPException(
            status_code=400,
            detail=f"Could not connect to Snowflake: {connection_detail}",
        ) from exc

    try:
        clean_query = raw_query.rstrip(";")
        upper = clean_query.upper()
        append_limit = (
            not upper.startswith(("SHOW", "DESCRIBE", "EXPLAIN", "ANALYZE"))
            and " LIMIT " not in upper
        )
        if append_limit:
            clean_query = f"{clean_query} LIMIT {body.limit + 1}"

        params = dict(body.params or {})
        if params:
            # snowflake-connector-python defaults to pyformat, so :name has to be
            # rewritten. Only names we were actually given are substituted, which
            # leaves ``::`` casts and time literals untouched.
            clean_query = _to_pyformat(clean_query, params)

        with conn.cursor() as cur:
            cur.execute(clean_query, params or None)
            description = cur.description or []
            columns = _unique_column_names([desc[0] for desc in description])
            withheld = 0
            rows = []
            for i, row in enumerate(cur.fetchall()):
                if i >= body.limit:
                    withheld += 1
                    break
                rows.append({columns[j]: _jsonify_value(v) for j, v in enumerate(row)})
        schema = _column_schema(columns, rows)
        # Snowflake's cursor description carries declared result types
        # (VARCHAR for TO_VARCHAR, NUMBER for a SUM) — prefer it over
        # value inference (M11/M20).
        declared = {
            columns[i]: str(desc[1])
            for i, desc in enumerate(description)
            if len(desc) > 1 and desc[1] is not None
        }
        if declared:
            schema = {**schema, **declared}
        return rows, columns, schema, bool(withheld)
    except Exception as exc:
        raise user_query_error(
            exc,
            public_message="Query failed",
            status_code=400,
        ) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)
