"""Query Playground — run safe, limited queries against saved connectors.

**What keeps a playground query read-only.** Two things, in this order:

1. *The read-only session* (``_apply_read_only_session``): PostgreSQL, MySQL,
   SQLite and Oracle run the statement in a transaction the server itself
   refuses writes in, and local DuckDB opens with external access off. Where
   the engine has one, this is the guard.
2. *The statement filter* (``_is_safe_sql``): one statement, a read-only head,
   no write keyword, and a **deny-list** of function, package and table-function
   names with side effects outside the transaction (network calls, server
   files, sleeps, backend control, dynamic SQL, database links). It is a
   **backstop**: a deny-list names what is known. It is the only guard on SQL
   Server-family engines (T-SQL has no read-only transaction) and for effects a
   read-only transaction does not stop (dblink, ``pg_terminate_backend``, file
   readers), so new names belong here as soon as they are known.

Follow-up (security review M2, not done here): replace the deny-list with a
per-dialect allow-list of statement shapes and functions, so an unknown name
is refused instead of allowed.
"""

from __future__ import annotations

import asyncio
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
from pydantic import BaseModel, Field, field_validator
from services import connector_store
import services.query_extract as services_query_extract
from services.brand_env import getenv_brand
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


def _is_safe_sql(raw_query: str, dialect: str = "") -> bool:
    """Allow read and metadata queries; block any destructive or write SQL.

    ``dialect`` is the connector's type (or its URL scheme). For SQL Server
    the text must also parse as exactly one SELECT (``services.tsql_select``,
    security review round 3, M2); no other dialect is held to T-SQL's grammar
    (M1).

    Uses ``sqlparse`` when installed; otherwise a conservative regex fallback so
    Datawrap Pilot / query playground never hard-fail on a missing optional dep.
    Every lexing in ``sql_code_views`` must pass: a quote inside a comment or a
    quoted identifier is not a literal, and text one engine reads as a comment
    can be a second statement on another.
    """
    if _is_sqlserver_dialect(dialect):
        from services.tsql_select import tsql_single_select

        if not tsql_single_select(raw_query):
            return False
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


def _is_sqlserver_dialect(dialect: str) -> bool:
    """SQL Server, Azure SQL, Synapse, or a ``mssql+...`` URL."""
    text = str(dialect or "").strip().lower()
    if not text:
        return False
    if text.startswith("mssql") or text in {"sqlserver", "sql_server", "azure_sql", "azuresql", "synapse"}:
        return True
    from src.transfer.connector_capabilities import resolve_driver_type

    return resolve_driver_type(text).lower() == "sqlserver"


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

    A ``;`` token after the statement, followed by anything but comments, is
    a second statement. Semicolons before it are not: ``;WITH cte AS (...)``
    is the T-SQL idiom for a CTE (PR 403 review 4, low).
    """
    import sqlparse
    from sqlparse.tokens import Comment, Newline, Punctuation, Whitespace

    seen_statement = False
    seen_semicolon = False
    for ttype, value in sqlparse.lexer.tokenize(raw_query or ""):
        if ttype in (Whitespace, Newline) or ttype in Comment:
            continue
        if ttype is Punctuation and value == ";":
            seen_semicolon = seen_statement
            continue
        if seen_semicolon:
            return True
        seen_statement = True
    return False


def _read_only_head(code: str) -> str | None:
    """Safe leading keyword of one masked view, or ``None`` when it is a write."""
    import sqlparse
    from sqlparse.sql import TokenList
    from sqlparse.tokens import Comment, Keyword, Newline, Whitespace

    try:
        # Leading semicolons end no statement (``;WITH cte AS ...``, review 4 low).
        parsed = sqlparse.parse(re.sub(r"^[\s;]+", "", code).strip())
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
    "pg_sleep", "pg_sleep_for", "pg_sleep_until", "sleep", "sleepeachrow", "benchmark",
    # T-SQL runs a second statement with no ``;`` before it: ``select 1 exec('...')``
    # reads as a call of a function named exec (security review M2).
    "exec", "execute",
    # side-channel drivers (pg dblink, mysql locks/files, mssql xp_)
    "get_lock", "release_lock", "load_file",
    "xp_cmdshell", "xp_dirtree", "xp_fileexist", "xp_regread", "xp_regwrite",
    "openrowset", "opendatasource", "openquery", "bulk",
    # mssql server-file readers
    "fn_xe_file_target_read_file", "fn_trace_gettable", "fn_get_audit_file",
    "fn_dblog", "fn_dump_dblog",
    # pg: run a statement passed as a string, so the scan never sees it
    "query_to_xml", "query_to_xmlschema", "query_to_xml_and_xmlschema",
    "cursor_to_xml", "cursor_to_xmlschema", "xpath_table", "ts_stat", "ts_rewrite",
    "connectby",
    # oracle URI / file object constructors (HTTP and file reads from a SELECT)
    "httpuritype", "dburitype", "xdburitype", "uritype", "bfilename",
    # bigquery federated pass-through
    "external_query",
    # ClickHouse table functions: a URL, a server file or another server from a SELECT
    "url", "urlcluster", "file", "filecluster", "remote", "remotesecure", "cluster",
    "clusterallreplicas", "s3", "s3cluster", "gcs", "azureblobstorage", "hdfs", "hdfscluster",
    "mysql", "postgresql", "mongodb", "redis", "sqlite", "jdbc", "odbc", "executable",
    "deltalake", "hudi", "iceberg", "cosn", "oss",
    # SQLite: load native code, read and write server files
    "load_extension", "writefile", "readfile", "fts3_tokenizer", "edit", "fsdir", "zipfile",
    # DuckDB (reached through a generic connector) and Databricks: environment and secrets
    "getenv", "sniff_csv", "json_execute_serialized_sql", "duckdb_secrets", "which_secret",
    "load_aws_credentials",
    # pg extensions: server-side HTTP (pgsql-http)
    "http", "http_get", "http_post", "http_put", "http_delete", "http_head", "http_patch",
    # Security review round 2 (M3). ClickHouse: lakehouse, blob and Arrow Flight
    # readers, every-table merge(), and unbounded generators (denial of service).
    "icebergs3", "iceberglocal", "icebergazure", "iceberghdfs", "icebergs3cluster",
    "icebergazurecluster", "iceberghdfscluster", "deltalakecluster", "deltalakes3",
    "deltalakeazure", "hudicluster", "azureblobstoragecluster", "arrowflight", "merge",
    "numbers", "numbers_mt", "zeros", "zeros_mt", "generaterandom",
    # PostgreSQL backup / replication / WAL control, server-dir listings, catalog writes.
    "pg_start_backup", "pg_stop_backup", "pg_copy_logical_replication_slot",
    "pg_copy_physical_replication_slot", "pg_wal_replay_pause", "pg_wal_replay_resume",
    "pg_ls_replslotdir", "pg_ls_logicalsnapdir", "pg_ls_logicalmapdir",
    "pg_log_backend_memory_contexts", "pg_import_system_collations", "loread", "lowrite",
    # Snowflake: URLs that hand out stage files.
    "get_presigned_url", "build_scoped_file_url", "build_stage_file_url",
    # MySQL: replication waits that hold the connection.
    "master_pos_wait", "source_pos_wait", "wait_for_executed_gtid_set",
    "wait_until_sql_thread_after_gtids",
    # oracle administrative packages are prefixed dbms_/utl_ below
})
_SIDE_EFFECT_PREFIXES = (
    "dblink", "lo_", "dbms_", "utl_", "sys_exec", "sys_eval", "sp_",
    # mssql extended procedures, pg tablefunc crosstabN, snowflake account control
    "xp_", "crosstab", "system$",
    # pg adminpack (server files) and statistics resets
    "pg_file_", "pg_stat_reset",
    # pg replication origins; Oracle Data Pump's master process (runs PL/SQL
    # outside the read-only transaction) (round 2, M3)
    "pg_replication_origin_", "kupp$",
    # DuckDB / Databricks file and URL readers: read_csv, read_text, read_files ...
    "read_",
)
#: Members of the refused Oracle packages that only compute on their arguments.
#: Allowed by exact ``package.function`` name; every other member of the same
#: package (``dbms_lob.loadfromfile``, ``utl_raw`` is all pure but stays listed
#: by name) is still refused (security review M2: the package rule refused
#: ordinary LOB and RAW reads).
_PURE_PACKAGE_FUNCTIONS = frozenset({
    "dbms_lob.substr", "dbms_lob.getlength", "dbms_lob.instr", "dbms_lob.compare",
    "utl_raw.cast_to_varchar2", "utl_raw.cast_to_nvarchar2", "utl_raw.cast_to_raw",
    "utl_raw.cast_to_number", "utl_raw.cast_to_binary_integer", "utl_raw.cast_to_binary_float",
    "utl_raw.cast_to_binary_double", "utl_raw.cast_from_number", "utl_raw.length",
    "utl_raw.substr", "utl_raw.concat",
    "utl_i18n.raw_to_char", "utl_i18n.string_to_raw",
    "utl_encode.base64_encode", "utl_encode.base64_decode",
    "utl_match.edit_distance", "utl_match.edit_distance_similarity",
    "utl_match.jaro_winkler", "utl_match.jaro_winkler_similarity",
})
# Statements T-SQL accepts after a SELECT with no separator. As bare words
# (not ``t.exec``, not a quoted identifier) they are refused anywhere.
_STATEMENT_WORD_RE = re.compile(
    r"(?<![A-Za-z0-9_$#@.])(?:exec|execute|waitfor)(?![A-Za-z0-9_$#])", re.I
)
# T-SQL statements after a SELECT with no separator (``select 1 backup ...``,
# ``select 1 use master``) are refused for SQL Server by parsing the whole text
# as exactly one SELECT (``services.tsql_select``, review round 3, M2). The
# word deny-list that ran on every dialect refused ordinary column names
# (``open``, ``begin``) on all of them (M1) and is gone.
# ``U&"d\0061ta"`` / ``U&'...'``: PostgreSQL reads the escapes, this filter
# would read the text. Refused outright; the views mask the quoted part.
_UNICODE_ESCAPED_RE = re.compile(r"(?<![A-Za-z0-9_$#])U&__DF[IQ]\d+__", re.I)
# Oracle database link: ``fn@link(...)``, ``table@link``. Attached to the name
# it is a link in every dialect; after whitespace it is one unless the word
# before is a keyword (``where a = 1 and @flag = 1``, ``select @x``, ``from @t``).
_DBLINK_ATTACHED_RE = re.compile(r"(?:[A-Za-z0-9_$#\]]|__DF[IQ]\d+__)@(?!@)\s*(?:[A-Za-z_\[]|__DF)")
_DBLINK_SPACED_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_$#]*|__DF[IQ]\d+__|\])\s+@(?!@)\s*(?:[A-Za-z_\[]|__DF)")
_NOT_AN_OBJECT_NAME = frozenset({
    "select", "and", "or", "not", "where", "on", "when", "then", "else", "by", "limit", "top",
    "offset", "in", "like", "ilike", "between", "is", "set", "values", "having", "case", "from",
    "join", "distinct", "all", "union", "as", "exists", "any", "some", "with", "over", "using",
    "end", "null", "true", "false", "fetch", "first", "next", "rows", "only", "escape", "into",
    "return", "if", "while", "percent", "div", "mod", "xor", "rlike", "regexp",
})

# A call's whole dotted name: ``utl_http.request(``, ``sys.dbms_xmlgen.getxml(``,
# ``master..xp_cmdshell(``, and Oracle's remote form ``pkg.fn@dblink(``.
# Capturing only the identifier before ``(`` read ``request`` / ``getxml`` and
# never met the package prefix (audit M-5).
_FUNC_CALL_RE = re.compile(
    r"(?<![A-Za-z0-9_$#.@])"
    r"((?:[A-Za-z_][A-Za-z0-9_$#]*\s*\.\s*(?:\.\s*)?)*[A-Za-z_][A-Za-z0-9_$#]*)"
    r"(?:\s*@\s*[A-Za-z0-9_$#.]+)?\s*\("
)


def _is_side_effect_name(name: str) -> bool:
    return name in _SIDE_EFFECT_FUNCTIONS or name.startswith(_SIDE_EFFECT_PREFIXES)


def _has_side_effect_functions(sql: str) -> bool:
    """True when a read query invokes a function with session/server side effects.

    Every part of a qualified name is judged: the function itself against the
    names and prefixes, and each qualifier (schema, package, type) against the
    prefixes, so a package is refused whatever member is called on it.
    """
    from services.sql_literals import sql_call_views, sql_code_views

    for code in sql_code_views(sql or ""):
        if _STATEMENT_WORD_RE.search(code) or _UNICODE_ESCAPED_RE.search(code) or _names_database_link(code):
            return True
    for scrubbed in sql_call_views(sql or ""):
        for match in _FUNC_CALL_RE.finditer(scrubbed):
            parts = [part.strip().lower() for part in match.group(1).split(".") if part.strip()]
            if ".".join(parts[1:] if parts[0] == "sys" else parts) in _PURE_PACKAGE_FUNCTIONS:
                continue  # a remote one (``@link``) was refused above
            *qualifiers, name = parts
            if _is_side_effect_name(name):
                return True
            if any(q.startswith(_SIDE_EFFECT_PREFIXES) for q in qualifiers):
                return True
    return False


def _names_database_link(code: str) -> bool:
    """True when the statement reaches through an Oracle database link (``name@link``)."""
    if _DBLINK_ATTACHED_RE.search(code):
        return True
    return any(
        match.group(1).lower() not in _NOT_AN_OBJECT_NAME for match in _DBLINK_SPACED_RE.finditer(code)
    )


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
# Statements that take a table the way FROM does: ``DESCRIBE '<path>'``,
# ``SHOW '<path>'``, ``SUMMARIZE '<path>'`` and ``PIVOT``/``UNPIVOT '<path>'``
# read the file (its header, or its values as column names; review round 3, H7).
_DUCKDB_TABLE_STATEMENTS = frozenset({
    "DESCRIBE", "SHOW", "SUMMARIZE", "PIVOT", "UNPIVOT", "PIVOT_WIDER", "PIVOT_LONGER",
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
        if upper in _DUCKDB_TABLE_STATEMENTS:
            expect_table = True
            continue
        if expect_table and upper in ("TABLE", "ALL"):
            continue  # DESCRIBE TABLE '<path>', SHOW ALL ...
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
    (``md:``) loads its extension through external access, so the playground
    refuses it.
    """
    import sqlalchemy
    from sqlalchemy.pool import NullPool

    from connectors.sqlite_common import is_duckdb_config_key

    database = str(getattr(engine.url, "database", "") or "")
    if database.strip().lower().startswith(("md:", "motherduck:")):
        # MotherDuck runs DuckDB in this process too, and needs external access
        # on to load its extension, so the backstop cannot apply (review 3, H7).
        raise HTTPException(
            status_code=400,
            detail="MotherDuck databases cannot be queried in the playground. "
            "Use Transfer Studio to move the data, or query it in MotherDuck.",
        )
    # duckdb-sqlalchemy merges URL query keys into the DuckDB config after
    # connect_args, so a configuration key in the URL would switch the backstop
    # off; connector URLs refuse them, and this URL never carries one.
    query = {k: v for k, v in dict(engine.url.query).items() if not is_duckdb_config_key(k)}
    guarded = sqlalchemy.create_engine(
        engine.url.set(query=query),
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
    for ttype, value in sqlparse.lexer.tokenize(re.sub(r"^[\s;]+", "", code).strip().rstrip(";")):
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
    # File export of a completed transfer must not inherit the 1000-row preview.
    # Connector writes stay on the preview limit and say so.
    full_population: bool = False
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
    # G7 (CWE-1236): a console export is a file a person opens in a spreadsheet.
    formula_protection: str = Field(
        "prefix",
        description=(
            "CSV/TSV file export: \"prefix\" (default; also for null or empty) puts a leading ' "
            "on text cells and header names that start with = + - @ tab or CR so a spreadsheet "
            "never runs them as formulas (numbers such as -5 are untouched); \"off\" writes "
            "exact values. Case and surrounding spaces are ignored; anything else is a 422"
        ),
    )

    @field_validator("formula_protection", mode="before")
    @classmethod
    def _formula_protection_vocabulary(cls, value: Any) -> str:
        """Same vocabulary as a transfer destination's formula_protection (G7)."""
        from services.csv_safety import formula_protection_mode

        return formula_protection_mode(value, default="prefix")


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
    # True when a cap withheld rows. A full-population file export is false
    # unless the statement itself named a window.
    truncated: bool = False
    population_note: str = ""


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


# A full-population file can outlive the interactive 120s preview. 15 minutes
# covers a million-row CSV without holding the rows in the API process.
_FULL_EXPORT_STATEMENT_TIMEOUT_SECONDS = 900.0
_FULL_EXPORT_STREAM_FORMATS = frozenset({"csv", "tsv", "jsonl"})


def _export_full_population_file(connector, body: QueryExportRequest, *, workspace_id: str = ""):  # noqa: ARG001
    """Write every row of a read-only query to a file. The preview limit is not applied.

    Rows are written as they arrive. The response carries the count and a
    download URL, not the rows. Excel and Parquet stay on the preview path
    and say so — they would buffer the population.
    """
    import csv
    import json
    import uuid

    fmt = (body.format or "csv").strip().lower()
    if fmt not in _FULL_EXPORT_STREAM_FORMATS:
        return QueryExportResult(
            success=False,
            format=fmt,
            truncated=True,
            population_note=(
                f"{fmt.upper()} export buffers rows and stays on the preview limit. "
                "Choose CSV to export every row of this query."
            ),
            error=(
                f"{fmt.upper()} cannot stream the full population. "
                "Choose CSV, TSV, or JSONL."
            ),
        )
    ctype = (connector.type or "").lower().strip()
    if ctype in {"mongodb", "snowflake"} or services_query_extract.connector_queryable_via_extract(ctype):
        return QueryExportResult(
            success=False,
            format=fmt,
            truncated=True,
            population_note=(
                "Full-population file export streams a SQL result. "
                "This connector was not silently capped at 1000 rows — the export was refused."
            ),
            error=(
                "Full-population export is a streaming SQL file. "
                "Run the query against the SQL destination, or export CSV from a SQL connector."
            ),
        )

    from services.export_paths import ExportPathError, generated_export_path, resolve_export_path

    ext = "jsonl" if fmt == "jsonl" else fmt
    try:
        if body.output_path.strip():
            final_path = resolve_export_path(body.output_path, default_ext=ext)
        else:
            final_path = generated_export_path(f"query_{uuid.uuid4().hex[:16]}.{ext}")
    except ExportPathError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    final_path.parent.mkdir(parents=True, exist_ok=True)

    written = {"n": 0}
    handle = final_path.open("w", encoding="utf-8", newline="")
    writer_box: dict[str, Any] = {}
    guard_formulas = body.formula_protection == "prefix" and fmt in {"csv", "tsv"}
    from services.csv_safety import neutralize_export_cell

    def _on_row(columns: list[str], record: dict[str, Any]) -> None:
        if fmt == "jsonl":
            handle.write(json.dumps(record, default=str) + "\n")
        else:
            writer = writer_box.get("w")
            if writer is None:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=columns,
                    extrasaction="ignore",
                    delimiter="\t" if fmt == "tsv" else ",",
                )
                # Alias names are data too: guarded like cells (G7 review M1).
                writer.writerow(
                    {c: neutralize_export_cell(c) if guard_formulas else c for c in columns}
                )
                writer_box["w"] = writer
            row = {k: "" if record.get(k) is None else record.get(k) for k in columns}
            if guard_formulas:
                # G7: text that would open as a formula is quoted; numbers are not.
                row = {k: neutralize_export_cell(v) for k, v in row.items()}
            writer.writerow(row)
        written["n"] += 1

    try:
        _run_sql_query(
            connector,
            body,
            json_engine_text=True,
            apply_limit=False,
            on_row=_on_row,
            statement_timeout_seconds=_FULL_EXPORT_STATEMENT_TIMEOUT_SECONDS,
        )
    except HTTPException:
        handle.close()
        final_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        handle.close()
        final_path.unlink(missing_ok=True)
        detail, _cid = public_error(
            exc,
            event="query.export.errored",
            public_message="Query execution failed.",
        )
        return QueryExportResult(success=False, error=detail, format=fmt)
    finally:
        if not handle.closed:
            handle.close()

    filename = final_path.name
    return QueryExportResult(
        success=True,
        filename=filename,
        path=str(final_path),
        download_url=f"/api/v1/transfer/download/{filename}",
        row_count=written["n"],
        format=fmt,
        truncated=False,
        population_note=(
            "Full population. The preview row limit was not applied. "
            "A LIMIT, TOP, or FETCH already in the query is still honored."
        ),
    )


def _export_residency_check(request: Request, body: QueryExportRequest, workspace_id: str) -> None:
    """403 when the export's destination is outside the workspace's data region."""
    from services.data_residency import addressing_refusal_for, residency_refusal
    from src.transfer.models import EndpointConfig

    destination_id = (body.destination_connector_id or "").strip()
    if destination_id:
        dest = EndpointConfig(kind="database", connector_id=destination_id)
    else:
        dest = EndpointConfig(kind="file_export", format=body.format, output_path=body.output_path)
    tenant = str(getattr(request.state, "tenant_id", "") or "")
    keyed = addressing_refusal_for(workspace_id, dest, host_tenant_id=tenant)
    if keyed:
        raise HTTPException(status_code=422, detail=keyed)
    refusal = residency_refusal(
        workspace_id, dest, host_tenant_id=str(getattr(request.state, "tenant_id", "") or "")
    )
    if refusal:
        raise HTTPException(status_code=403, detail=refusal)


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
    if (body.destination_connector_id or "").strip():
        from services.body_id_ownership import assert_body_ids

        # Before the query is read (review 3, body routes): another workspace's
        # destination answers like a missing one.
        await asyncio.to_thread(assert_body_ids, request, connectors=(body.destination_connector_id,), write=True)
    # Security review H2: an export writes the workspace's rows without a job,
    # so nothing else would ask where they land. Both branches (a connector, a
    # file on this deployment) are judged before the query is read.
    _export_residency_check(request, body, str(getattr(connector, "workspace_id", "") or "") or workspace_id)

    if body.full_population and not (body.destination_connector_id or "").strip():
        return _export_full_population_file(connector, body, workspace_id=workspace_id)

    try:
        rows, columns, schema, truncated = _run_query(
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
        exported = _export_to_connector(body, rows, columns, schema, workspace_id, request)
        if body.full_population and exported.success:
            exported.truncated = True
            exported.population_note = (
                f"Writing to a connector uses the preview limit of {body.limit} rows. "
                "File export writes every row of this query."
            )
        return exported

    try:
        from src.transfer.adapters import write_destination_file
        from src.transfer.models import EndpointConfig

        dest = EndpointConfig(
            kind="file_export",
            format=body.format,
            output_path=body.output_path,
            extra={"formula_protection": body.formula_protection},
        )
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
        note = ""
        if truncated:
            note = (
                f"Export stopped at the preview limit of {body.limit} rows. "
                "File export with the full population writes every row."
            )
        return QueryExportResult(
            success=True,
            filename=filename,
            path=str(final_path),
            download_url=f"/api/v1/transfer/download/{filename}",
            row_count=len(rows),
            format=body.format,
            truncated=bool(truncated),
            population_note=note,
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
    from src.ai.copilot.tool_deadline import run_off_event_loop

    def _timed_out(message: str) -> Any:
        raise HTTPException(status_code=504, detail=message)

    try:
        # Off the event loop, under the connector deadline (D321).
        info = await run_off_event_loop(
            "introspect_connector_schema",
            lambda: _in_connector_scope(body.connector_id, introspect_endpoint, endpoint),
            _timed_out,
        )
    except HTTPException:
        raise
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

    live = _in_connector_scope(body.connector_id, live_schema_names, endpoint)
    tree = build_catalog_tree(
        connector_type=str(getattr(endpoint, "format", "") or ""),
        database=body.database or str(getattr(endpoint, "database", "") or ""),
        schema=body.schema_name or str(getattr(endpoint, "schema", "") or ""),
        live_schemas=live,
    )
    return QuerySchemasResult(**tree)


def _in_connector_scope(connector_id: str, fn: Any, *args: Any) -> Any:
    """Run ``fn`` with the stored connector's own workspace bound for the exports tree (H5)."""
    from services.export_paths import stored_connector_export_scope

    with stored_connector_export_scope(connector_id):
        return fn(*args)


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

    from services.egress_guard import egress_workspace
    from services.export_paths import connector_export_scope

    owner = str(getattr(dest_connector, "workspace_id", "") or "")
    try:
        # The write is made for the connector's workspace: its file area, and
        # the workspace the S3 bucket-region guard judges (review round 3, H4).
        with connector_export_scope(owner), egress_workspace(owner or workspace_id):
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
        elif _is_oracle_connector(db):
            # Must be the transaction's first statement, which it is: this
            # runs first on a fresh connection checkout. DML and SELECT ... FOR
            # UPDATE then fail with ORA-01456 (audit M-5).
            conn.execute(text("SET TRANSACTION READ ONLY"))
        elif db == "clickhouse":
            # Per query (clickhouse_driver sends it with each query), never a
            # session SET that would follow the pooled connection to a writer
            # (security review round 2, M3).
            current = dict((conn.get_execution_options() or {}).get("settings") or {})
            conn.execution_options(settings={**current, "readonly": 1})
        # SQL Server / Azure SQL / Synapse: T-SQL has no read-only transaction,
        # and a session-level SET would follow the pooled connection to the
        # next writer. The guard there is the statement filter, and that this
        # connection is closed without a commit.
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "read-only session option not applied on %s: %s", db, exc
        )


#: Interactive statement bound; ``0`` switches it off (D181).
DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS = 120.0


def _apply_statement_timeout(conn: Any, db_type: str, *, seconds_override: float | None = None) -> None:
    """Have the server cancel an interactive query that outlives the tool deadline.

    The deadline frees the caller; this frees the database and the worker, which
    would otherwise keep executing for nobody. Best-effort per dialect.
    """

    raw = getenv_brand("DATAFLOW_QUERY_STATEMENT_TIMEOUT_SECONDS", "").strip()
    try:
        seconds = float(raw) if raw else DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS
    except ValueError:
        seconds = DEFAULT_QUERY_STATEMENT_TIMEOUT_SECONDS
    if seconds_override is not None and seconds_override > 0:
        seconds = seconds_override
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
        # Output name -> the base column it passes through unchanged. An
        # expression aliased to a column's name (``count(distinct name) AS
        # name``) is not that column: it was labelled TEXT (D17).
        passthrough: dict[str, str] = {}
        star = False
        for proj in getattr(tree, "selects", None) or []:
            inner = proj.this if isinstance(proj, exp.Alias) else proj
            if isinstance(inner, exp.Star) or (
                isinstance(inner, exp.Column) and isinstance(inner.this, exp.Star)
            ):
                star = True
            elif isinstance(inner, exp.Column) and inner.name:
                passthrough[str(proj.alias_or_name).lower()] = str(inner.name).lower()
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
        out: dict[str, str] = {}
        for c in columns:
            source = passthrough.get(c.lower())
            if source is None and star and c.lower() not in passthrough:
                source = c.lower()
            if source is not None and source in lower:
                out[c] = lower[source]
        return out
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


def _run_sql_query(
    connector,
    body,
    *,
    json_engine_text: bool = False,
    apply_limit: bool = True,
    on_row=None,
    statement_timeout_seconds: float | None = None,
):
    raw_query = body.query.strip()
    if not raw_query:
        raise HTTPException(status_code=400, detail="SQL query is required")
    dialect = str(connector.type or "")
    if str(connector.connection_string or "").lower().startswith("mssql"):
        dialect = "mssql"
    if not _is_safe_sql(raw_query, dialect=dialect):
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
    from services.export_paths import connector_export_scope

    try:
        # An embedded file in the exports tree opens as the connector's own
        # workspace, never the request header's (security review round 2, H5).
        with connector_export_scope(getattr(connector, "workspace_id", "")):
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
    # Preview adds LIMIT so the grid cannot pull a million rows into the
    # browser. A full-population file export does not. A window the operator
    # already wrote (LIMIT / TOP / FETCH) is still honored.
    if apply_limit:
        clean_query = append_result_limit(connector.type, raw_query, body.limit + 1)
    else:
        clean_query = raw_query.strip().rstrip(";")

    restore_json = lambda: None  # noqa: E731
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            _apply_read_only_session(conn, connector.type)
            _apply_statement_timeout(
                conn, connector.type, seconds_override=statement_timeout_seconds
            )
            from services.sql_literals import escape_sqlalchemy_literal_colons

            if json_engine_text:
                # Samples and exports carry a JSON cell as its document text
                # (D207: a jsonb "hello" arrived as bare hello, null as NULL).
                from services.json_polarity import keep_pg_json_engine_text

                restore_json = keep_pg_json_engine_text(conn)
            else:
                # The playground shows a parsed document, digits intact (D352:
                # pgvector metadata 99999999999.999999 showed 100000000000.0).
                from services.json_polarity import exact_pg_json_numbers

                restore_json = exact_pg_json_numbers(conn)

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
                if apply_limit and i >= body.limit:
                    withheld += 1
                    break
                if tz_positions:
                    from connectors.oracle_tstz import parse_oracle_tz_row

                    row = parse_oracle_tz_row(row, tz_positions)
                record = {columns[j]: _jsonify_value(v) for j, v in enumerate(row)}
                if on_row is not None:
                    on_row(columns, record)
                else:
                    rows.append(record)
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
    if not _is_safe_sql(raw_query, dialect="snowflake"):
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
        # (VARCHAR for TO_VARCHAR, NUMBER(p,s) for a SUM) — prefer it over
        # value inference (M11/M20). Its integer type codes are Snowflake's
        # own: the driver names them (named_cursor_description) and the shared
        # reader honours the name; a raw code is never read (D17, MX2-07).
        from connectors.snowflake_conn import named_cursor_description
        from services.decimal_observe import cursor_declared_carriers

        declared = cursor_declared_carriers(
            columns,
            named_cursor_description(description),
            dialect="snowflake",
        )
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
