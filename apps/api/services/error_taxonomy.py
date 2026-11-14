"""One error taxonomy for every surface Datawrap shows a failure on.

A failure reaches a user through Test, the Query page, the schema browser,
Validate, a job, a schedule notification, an email or Pilot. Each of those
used to word the same driver exception its own way, by substring search over
the driver's text. This module is the single classifier and the single
wording table they all call.

``classify(exc, side=..., phase=...)`` looks a failure up in this order:

1. typed Datawrap exceptions (an ``error_code`` attribute, or a stdlib type
   whose meaning is fixed: ``ModuleNotFoundError``, ``FileNotFoundError``,
   an ``OSError`` errno, ``socket.gaierror``);
2. the driver's structured code: PostgreSQL SQLSTATE (``pgcode``), MySQL
   error number (``args[0]``), MongoDB ``code``, SQLite error name;
3. anchored per-engine text patterns, for drivers that give no code
   (libpq connect failures, SQLite, pymongo topology errors).

The result carries a code from :data:`TEMPLATES`, a message that names the
object, one next action (a UI form and an API form), and the sanitised driver
sentence. ``driver_sentence(exc)`` is the only way driver text reaches a user:
it drops the SQL, ``LINE``/caret lines, HINT/DETAIL, ``[parameters]``,
sqlalche.me links, repeated lines, host:port pairs and row values.
"""

from __future__ import annotations

import errno as _errno
import re
import socket
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Iterator

from services.log_redaction import redact_log_text

SIDES = ("source", "destination", "connection", "query", "platform", "")
PHASES = ("connect", "read", "write", "ddl", "introspect", "probe", "query", "export", "validate", "")


class OperatorCopy(str):
    """Text Datawrap wrote for a user. Humanisers pass it through unchanged;
    driver text that merely starts with the same words is not this type."""


class DatawrapError(Exception):
    """A failure Datawrap itself raises, already classified.

    ``code`` is a key of :data:`TEMPLATES`; ``object`` names what failed.
    """

    def __init__(self, code: str, message: str = "", *, object: dict[str, Any] | None = None) -> None:
        self.error_code = code
        self.error_object = dict(object or {})
        super().__init__(message or code)


@dataclass(frozen=True)
class Template:
    title: str
    message: str
    action: str
    ui: str
    api: str = ""
    retriable: bool = False
    #: Per-side (label, ui) overrides of the action.
    side_actions: dict[str, tuple[str, str]] = field(default_factory=dict)


_RETRY_API = "POST /api/v1/connectors/jobs/{job_id}/retry"
_TEST_API = "POST /api/v1/connectors/{connector_id}/test"
_EDIT_API = "PUT /api/v1/connectors/{connector_id}"

#: The one wording table. Placeholders render as `` "name"`` or nothing, so a
#: message reads naturally whether or not the driver named the object.
TEMPLATES: dict[str, Template] = {
    # -- auth ---------------------------------------------------------------
    "auth.password_rejected": Template(
        "Sign-in rejected",
        "{engine} rejected the sign-in{for_user}: the username or password is wrong.",
        "Update the username and password on the connection, then test it again.",
        "open_connection", _EDIT_API,
    ),
    "auth.user_unknown": Template(
        "User not found",
        "{engine} has no user{u} for this connection.",
        "Check the username on the connection, then test it again.",
        "open_connection", _EDIT_API,
    ),
    "auth.mongo_auth_source": Template(
        "Sign-in rejected",
        "MongoDB rejected the sign-in for this connection: the username, password or Auth source is wrong.",
        "Check the username and password, and set Auth source to the database where the user is defined (often admin).",
        "open_connection", _EDIT_API,
    ),
    "auth.token_invalid": Template(
        "Credential rejected",
        "{engine} rejected this connection's token or key.",
        "Replace the token or key on the connection, then test it again.",
        "open_connection", _EDIT_API,
    ),
    "auth.mfa_required": Template(
        "Multi-factor sign-in required",
        "{engine} requires multi-factor authentication for this user, which a connection cannot answer.",
        "Use a key-pair or programmatic access token for this connection.",
        "open_connection", _EDIT_API,
    ),
    # -- access -------------------------------------------------------------
    "access.source_read_denied": Template(
        "Source read not permitted",
        "Datawrap couldn't read{tbl} on the source: the connection's database role doesn't have SELECT on it.",
        "Grant SELECT on the table to the source connection's role, then run again.",
        "open_source", _RETRY_API,
    ),
    "access.dest_write_denied": Template(
        "Destination write not permitted",
        "The destination refused the write{to_t}: the connection's database role can't insert or update rows there.",
        "Grant INSERT and UPDATE on the table to the destination connection's role, then run again.",
        "open_destination", _RETRY_API,
    ),
    "access.dest_ddl_denied": Template(
        "Destination table can't be created",
        "The destination refused to create or change a table{in_s}: the connection's database role lacks CREATE on the schema.",
        "Grant CREATE on the schema to the destination connection's role, or create the table first, then run again.",
        "open_destination", _RETRY_API,
    ),
    "access.denied": Template(
        "Access not permitted",
        "The database refused access{to_obj}: the connection's role doesn't have the privilege this needs.",
        "Ask the database owner to grant this connection's role access{to_obj}, then try again.",
        "open_connection", _TEST_API,
    ),
    "access.database_not_granted": Template(
        "Database not available to this user",
        "{engine} denied this user access to database{db}: it does not exist or the user has no grant on it.",
        "Check the database name on the connection, or grant the user access to it.",
        "open_connection", _EDIT_API,
    ),
    # -- network ------------------------------------------------------------
    "network.dns_not_found": Template(
        "Host not found",
        "Host not found: the connection's host name didn't resolve (DNS).",
        "Check the spelling of the host name on the connection.",
        "open_connection", _EDIT_API,
    ),
    "network.refused": Template(
        "Connection refused",
        "Connection refused: the host answered, but nothing accepts connections on the connection's port.",
        "Check the port number on the connection and that the database is running.",
        "open_connection", _EDIT_API, retriable=True,
    ),
    "network.unreachable": Template(
        "Network unreachable",
        "Network unreachable: there's no route from Datawrap to the connection's host.",
        "Check the host address and the VPN, VPC peering or firewall routes to it.",
        "open_connection", _EDIT_API, retriable=True,
    ),
    "network.connect_timeout": Template(
        "Connection timed out",
        "Connection timed out: the connection's host didn't answer before the time limit.",
        "Check the host and port, and that a firewall or security group lets Datawrap connect.",
        "open_connection", _TEST_API, retriable=True,
    ),
    "network.handshake_timeout": Template(
        "Handshake timed out",
        "Handshake timed out: the connection's port accepted the connection, but the server never completed the database handshake.",
        "Check the port: it may belong to another service, or sit behind a proxy that doesn't forward the database protocol.",
        "open_connection", _EDIT_API, retriable=True,
    ),
    "network.tls_required": Template(
        "TLS setting mismatch",
        "The server and this connection disagree about TLS (SSL).",
        "Turn SSL on or off on the connection to match the server, then test it again.",
        "open_connection", _EDIT_API,
    ),
    "network.tls_untrusted": Template(
        "Server certificate not trusted",
        "The server's TLS certificate couldn't be verified.",
        "Install a trusted certificate on the server, or turn on Trust server certificate for this connection.",
        "open_connection", _EDIT_API,
    ),
    "network.egress_blocked": Template(
        "Host blocked by network policy",
        "Datawrap's network policy doesn't allow connections to this host.",
        "Ask your administrator to allow this host for your workspace.",
        "contact_admin",
    ),
    "network.connection_lost": Template(
        "Connection lost",
        "The database connection closed while Datawrap was using it.",
        "Run again. If it repeats, check the database server's logs and idle timeouts.",
        "retry", _RETRY_API, retriable=True,
    ),
    # -- object -------------------------------------------------------------
    "object.database_not_found": Template(
        "Database not found",
        "Database{db} does not exist on this {engine} server.",
        "Check the database name on the connection.",
        "open_connection", _EDIT_API,
    ),
    "object.schema_not_found": Template(
        "Schema not found",
        "Schema{s} does not exist{side_on}.",
        "Check the schema name on the connection (names can be case-sensitive), or create the schema.",
        "open_connection", _EDIT_API,
    ),
    "object.table_not_found": Template(
        "Table not found",
        "Table{t} does not exist{side_on}.",
        "Check the table name.",
        "open_connection",
        side_actions={
            "source": ("Pick an existing table from the source catalog, then run again.", "open_source"),
            "destination": ("Check the destination table name, or let Datawrap create the table.", "open_destination"),
            "query": ("Check the table name in the query.", "edit_query"),
        },
    ),
    "object.table_not_visible": Template(
        "Table not visible",
        "Table{t} isn't visible to this connection: it does not exist, or the connection's role has no privilege on it.",
        "Check the table name, or grant the connection's role SELECT on it.",
        "open_connection",
    ),
    "object.column_not_found": Template(
        "Column not found",
        "Column{c} does not exist{in_t}{side_on}.",
        "Check the column name.",
        "open_map",
        side_actions={
            "source": ("Refresh the source schema and update the mapping, then run again.", "open_map"),
            "destination": ("Open Map and rematch the column, or add it to the destination table.", "open_map"),
            "query": ("Check the column name in the query.", "edit_query"),
        },
    ),
    "object.collection_not_found": Template(
        "Collection not found",
        "Collection{t} does not exist in this MongoDB database.",
        "Check the collection name.",
        "open_connection",
    ),
    "object.upload_expired": Template(
        "Upload no longer available",
        "This job's uploaded file is no longer on the server, so it can't run again from here.",
        "Upload the file again in Transfer Studio and start a new run.",
        "open_studio",
    ),
    "access.path_not_allowed": Template(
        "File location not allowed",
        "This server only opens local files inside its connector folders, and this file is outside them.",
        "Move the file into the connector data folder or upload it, or ask the operator to allow its folder.",
        "open_connection",
    ),
    "object.file_not_found": Template(
        "File not found",
        "File{f} wasn't found.",
        "Check the file path on the connection, or upload the file again.",
        "open_source",
    ),
    # -- query --------------------------------------------------------------
    "query.syntax": Template(
        "Query syntax error",
        "The query has a syntax error{near}.",
        "Fix the query text and run it again.",
        "edit_query",
    ),
    "query.refused_function": Template(
        "Function not allowed",
        "The query calls{fn}, which Datawrap's query editor doesn't allow.",
        "Remove the call and run the query again.",
        "edit_query",
    ),
    "query.division_by_zero": Template(
        "Division by zero",
        "The query divided by zero.",
        "Guard the divisor (for example NULLIF(x, 0)) and run it again.",
        "edit_query",
    ),
    "query.undefined_function": Template(
        "Unknown function",
        "The query calls a function{fn} that this {engine} database doesn't have.",
        "Check the function name and its argument types.",
        "edit_query",
    ),
    # -- value --------------------------------------------------------------
    "value.not_null": Template(
        "Required value missing",
        "A row has no value for required column{c}{in_t}.",
        "Map a source column with values to it, give it a default, or make the column nullable, then run again.",
        "open_map", _RETRY_API,
    ),
    "value.type_mismatch": Template(
        "Value doesn't fit the column type",
        "A value doesn't fit the type of column{c}{in_t}{as_type}.",
        "Change the column's type or transform in Map, or fix the source values, then run Validate again.",
        "open_map",
    ),
    "value.too_long": Template(
        "Value too long",
        "A value is longer than column{c}{in_t} allows{as_type}.",
        "Widen the destination column, or shorten the value with a transform in Map, then run again.",
        "open_map",
    ),
    "value.numeric_overflow": Template(
        "Number out of range",
        "A number is too large for column{c}{in_t}{as_type}.",
        "Widen the column's precision, or map the value to a larger numeric type, then run again.",
        "open_map",
    ),
    "value.check_violation": Template(
        "Row broke a CHECK constraint",
        "A row broke CHECK constraint{k}{in_t}, so the write was rolled back.",
        "Run Validate to see the rows that break it, fix or filter them, then run again.",
        "open_validate",
    ),
    "value.duplicate_key": Template(
        "Duplicate key",
        "A row repeats a key value that already exists{in_k}{in_t}.",
        "Switch to upsert on this key, remove the duplicate source rows, or clear the destination table, then run again.",
        "open_map",
    ),
    "value.fk_violation": Template(
        "Missing referenced row",
        "A row refers to a key that does not exist{in_k}{in_t}.",
        "Load the referenced table first, or remove the foreign key, then run again.",
        "open_map",
    ),
    # -- timeout ------------------------------------------------------------
    "timeout.connect_deadline": Template(
        "Didn't answer in time",
        "The {side_noun} didn't answer before Datawrap's time limit for this step.",
        "Check the host and port, the firewall or VPN, and that the database is healthy, then run again.",
        "retry", _RETRY_API, retriable=True,
    ),
    "timeout.statement": Template(
        "Statement time limit reached",
        "The database stopped the statement: it ran longer than the statement time limit.",
        "Narrow the query or read, or raise the database's statement timeout, then run again.",
        "retry",
    ),
    "timeout.lock": Template(
        "Table is locked",
        "Another session holds a lock{on_t}, and the wait for it ran out.",
        "Close the session holding the lock (an open SQL client or an earlier run), then run again.",
        "retry", _RETRY_API, retriable=True,
    ),
    "timeout.deadlock": Template(
        "Write deadlocked",
        "The database cancelled the write to break a deadlock with another session.",
        "Run again. If it repeats, stop the other process writing to the same table.",
        "retry", _RETRY_API, retriable=True,
    ),
    # -- capacity -----------------------------------------------------------
    "capacity.disk_full": Template(
        "Out of disk space",
        "The {side_noun} ran out of disk space.",
        "Free disk space or expand the volume on the {side_noun} host, then resume the job.",
        "retry", _RETRY_API,
    ),
    "capacity.table_full": Template(
        "Table is full",
        "Table{t} is full: the database can't allocate more space for it.",
        "Expand the tablespace or free space on the database host, then resume the job.",
        "retry", _RETRY_API,
    ),
    "capacity.connection_limit": Template(
        "Connection limit reached",
        "{engine} has reached its connection limit.",
        "Run fewer jobs at once on this connection, or raise the database's connection limit, then run again.",
        "retry", _RETRY_API, retriable=True,
    ),
    "capacity.quota": Template(
        "Quota or rate limit reached",
        "{engine} refused the request because a quota or rate limit was reached.",
        "Wait for the quota to reset, or raise it with the provider, then run again.",
        "retry", _RETRY_API, retriable=True,
    ),
    # -- config -------------------------------------------------------------
    "config.driver_missing": Template(
        "Driver not installed",
        "{engine} connections aren't available on this server (driver not installed).",
        "Contact your administrator.",
        "contact_admin",
    ),
    "config.invalid_connection_string": Template(
        "Connection string not valid",
        "The connection string for this connection isn't valid.",
        "Check the connection string's format, credentials and options.",
        "open_connection", _EDIT_API,
    ),
    # -- platform -----------------------------------------------------------
    "platform.job_store_unavailable": Template(
        "Job history unavailable",
        "Datawrap can't reach its job history right now.",
        "Try again in a minute. If it persists, contact support with the reference.",
        "retry", retriable=True,
    ),
    "platform.gateway_timeout": Template(
        "Request timed out",
        "Datawrap's server didn't answer in time.",
        "Try again. If it persists, contact support with the reference.",
        "retry", retriable=True,
    ),
    "platform.internal": Template(
        "Something went wrong",
        "Something went wrong on Datawrap's side.",
        "Try again. If it persists, contact support with the reference.",
        "retry", retriable=True,
    ),
    "unclassified": Template(
        "Cause not identified",
        "Datawrap couldn't identify the cause of this failure.{sentence}",
        "Contact support with the reference.",
        "contact_support",
    ),
}

#: DEPRECATED, one release only: the job-failure code each taxonomy code
#: replaced, for API and MCP clients still keyed on the old codes. Carried as
#: ``legacy_code`` on the envelope and the job record; removed next release.
LEGACY_CODES: dict[str, str] = {
    "access.source_read_denied": "destination_permission_denied",
    "access.dest_write_denied": "destination_permission_denied",
    "access.dest_ddl_denied": "destination_permission_denied",
    "access.denied": "destination_permission_denied",
    "object.column_not_found": "destination_column_missing",
    "object.table_not_found": "table_not_found",
    "timeout.lock": "destination_lock_timeout",
    "unclassified": "transfer_failed",
    "capacity.table_full": "destination_table_full",
    "capacity.disk_full": "destination_disk_full",
    "capacity.connection_limit": "destination_connection_limit",
    "value.check_violation": "destination_check_violation",
}


def legacy_code(code: str) -> str:
    """The deprecated pre-taxonomy code for ``code``, or ``""``."""
    return LEGACY_CODES.get(str(code or ""), "")


_ENGINE_NAMES = {
    "postgresql": "PostgreSQL", "postgres": "PostgreSQL", "redshift": "Amazon Redshift",
    "mysql": "MySQL", "mariadb": "MariaDB", "mongodb": "MongoDB", "sqlite": "SQLite",
    "sqlserver": "SQL Server", "mssql": "SQL Server", "azure_sql": "SQL Server",
    "oracle": "Oracle", "snowflake": "Snowflake", "bigquery": "BigQuery",
    "db2": "Db2", "clickhouse": "ClickHouse", "redis": "Redis", "csv": "CSV",
}
_MODULE_ENGINES = (
    ("psycopg", "postgresql"), ("pg8000", "postgresql"), ("pymysql", "mysql"),
    ("mysql", "mysql"), ("mariadb", "mariadb"), ("pymongo", "mongodb"), ("bson", "mongodb"),
    ("sqlite3", "sqlite"), ("pyodbc", "sqlserver"), ("pymssql", "sqlserver"),
    ("oracledb", "oracle"), ("cx_oracle", "oracle"), ("snowflake", "snowflake"),
)
_DRIVER_PACKAGES = {
    "psycopg2": "postgresql", "psycopg": "postgresql", "pymysql": "mysql", "pymongo": "mongodb",
    "pyodbc": "sqlserver", "pymssql": "sqlserver", "oracledb": "oracle", "cx_Oracle": "oracle",
    "snowflake": "snowflake", "google": "bigquery",
}


def engine_name(engine: str) -> str:
    key = (engine or "").strip().lower()
    return _ENGINE_NAMES.get(key) or (key.replace("_", " ").title() if key else "The database")


def _root(exc: Any) -> Any:
    """The driver exception under SQLAlchemy / our own wrappers."""
    seen = 0
    cur = exc
    while seen < 6 and isinstance(cur, BaseException):
        orig = getattr(cur, "orig", None)
        if isinstance(orig, BaseException) and orig is not cur:
            cur = orig
            seen += 1
            continue
        break
    return cur


def _infer_engine(exc: Any) -> str:
    root = _root(exc)
    if not isinstance(root, BaseException):
        return ""
    module = (type(root).__module__ or "").lower()
    for prefix, engine in _MODULE_ENGINES:
        if module.startswith(prefix):
            return engine
    return ""


def driver_code(exc: Any) -> tuple[str, str]:
    """``(engine, code)`` the driver attached, or ``("", "")``."""
    root = _root(exc)
    if not isinstance(root, BaseException):
        wrapped = _WRAPPED_MYSQL.match(str(root or ""))
        return ("mysql", wrapped.group(1)) if wrapped else ("", "")
    engine = _infer_engine(root)
    module = (type(root).__module__ or "").lower()
    args = getattr(root, "args", ())
    if module.startswith(("pymysql", "mysql")) and args and isinstance(args[0], int):
        return engine or "mysql", str(args[0])
    pgcode = getattr(root, "pgcode", None)
    if not pgcode and module.startswith("psycopg"):
        pgcode = getattr(root, "sqlstate", None)
    if isinstance(pgcode, str) and pgcode:
        return engine or "postgresql", pgcode
    if module.startswith("pymongo"):
        code = getattr(root, "code", None)
        if isinstance(code, int):
            return "mongodb", str(code)
    if module.startswith("sqlite3"):
        name = getattr(root, "sqlite_errorname", None)
        if isinstance(name, str) and name:
            return "sqlite", name
    # A writer that re-raised a pymysql error as text keeps its "(1146, ...)" head.
    wrapped = _WRAPPED_MYSQL.match(str(root))
    if wrapped and not engine:
        return "mysql", wrapped.group(1)
    return engine, ""


_WRAPPED_MYSQL = re.compile(r"^\s*(?:\([\w.]+\)\s*)?\((\d{4}),\s*[\"']")


# -- driver text sanitiser ----------------------------------------------------

_ENVELOPE = re.compile(r"^\s*\((?:[A-Za-z_][\w]*\.)+[A-Za-z_]\w*\)\s*")
_INNER_ENVELOPE = re.compile(
    r"\((?:psycopg2?|pymysql|MySQLdb|sqlite3|pymongo|pyodbc|pymssql|oracledb|sqlalchemy|snowflake)(?:\.\w+)+\)\s*"
)
_SQL_BLOCK = re.compile(r"\[SQL:.*?\](?=\s*(?:\[parameters:|\(Background|$))", re.S)
_PARAMS_BLOCK = re.compile(r"\[parameters:.*?\](?=\s*(?:\(Background|$))", re.S)
_BACKGROUND = re.compile(r"\(Background on this error at:[^)]*\)", re.S)
_URL = re.compile(r"https?://(?:www\.)?sqlalche\.me\S*")
#: A connection URI names host, port and user; the user already knows which connection it is.
_DSN = re.compile(r"\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|mssql|sqlserver|oracle|redis|snowflake|jdbc:[a-z]+)"
                  r"(?:\+\w+)?://\S+", re.I)
_PG_SERVER = re.compile(r'connection to server (?:at|on) "[^"]*"(?:\s*\([^)]*\))?,\s*port \d+ failed:\s*', re.I)
_PG_SOCKET = re.compile(r'connection to server on socket "[^"]*" failed:\s*', re.I)
_MYSQL_USER_HOST = re.compile(r"'([^'@]*)'@'[^']*'")
_IPV4_PORT = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b")
_IPV6 = re.compile(r"\[[0-9a-fA-F:]+\](?::\d{1,5})?")
_HOST_PORT = re.compile(r"\b(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}:\d{2,5}\b|\blocalhost:\d{2,5}\b")
_ON_HOST = re.compile(r"(?:server|host) (?:on|at) '[^']*'(?: \(\d+\))?", re.I)
_PG_LEVEL = re.compile(r"^\s*(?:FATAL|ERROR|WARNING|PANIC):\s+", re.I)
_DROP_LINE = re.compile(r"^\s*(?:LINE \d+:|HINT:|DETAIL:|CONTEXT:|QUERY:|STATEMENT:|WHERE:|\^)", re.I)
_MONGO_FULL = re.compile(r",\s*full error:.*$", re.S)
_MONGO_TOPOLOGY = re.compile(r",\s*(?:Timeout:|Topology Description:).*$", re.S)
_MONGO_CONFIGURED = re.compile(r"\s*\(configured timeouts:[^)]*\)")
_QUOTED_VALUE_AFTER_TYPE = re.compile(r'(for type [\w ()]+):\s*"[^"]*"')
_MYSQL_VALUE = re.compile(r"(Incorrect \w+ value:)\s*'[^']*'")
_DUP_ENTRY = re.compile(r"Duplicate entry '[^']*'")
_KEY_VALUE = re.compile(r"Key \(([^)]*)\)=\([^)]*\)")
_WS = re.compile(r"\s+")
_SENTENCE_MAX = 220


def _shorten(text: str, limit: int = _SENTENCE_MAX) -> str:
    if len(text) <= limit:
        return text
    cut = text[:limit]
    space = cut.rfind(" ")
    if space > limit * 0.6:
        cut = cut[:space]
    return cut.rstrip(" ,;:.-") + "…"


_WRAPPED_MYSQL_TEXT = re.compile(r"^\s*(?:\([\w.]+\)\s*)?\(\d{4},\s*([\"'])(.*)\1\)", re.S)


def _raw_driver_text(exc: Any) -> str:
    text = _raw_driver_text_inner(exc)
    wrapped = _WRAPPED_MYSQL_TEXT.match(text)
    return wrapped.group(2) + text[wrapped.end():] if wrapped else text


def _raw_driver_text_inner(exc: Any) -> str:
    if isinstance(exc, str):
        return exc
    root = _root(exc)
    diag = getattr(root, "diag", None)
    primary = getattr(diag, "message_primary", None) if diag is not None else None
    if isinstance(primary, str) and primary.strip():
        return primary
    module = (type(root).__module__ or "").lower() if isinstance(root, BaseException) else ""
    args = getattr(root, "args", ())
    if module.startswith(("pymysql", "mysql")) and len(args) >= 2 and isinstance(args[0], int):
        return str(args[1])
    if module.startswith("pymongo"):
        details = getattr(root, "details", None)
        if isinstance(details, dict) and isinstance(details.get("errmsg"), str):
            return details["errmsg"]
    return str(root)


def driver_sentence(exc: Any, *, limit: int = _SENTENCE_MAX) -> str:
    """The driver's own sentence, safe to show a user. Never SQL, values or hosts."""
    text = _raw_driver_text(exc)
    text = _BACKGROUND.sub(" ", text)
    text = _SQL_BLOCK.sub(" ", text)
    text = _PARAMS_BLOCK.sub(" ", text)
    text = re.sub(r"\[SQL:.*$", " ", text, flags=re.S)
    text = _MONGO_FULL.sub("", text)
    text = _MONGO_TOPOLOGY.sub("", text)
    text = _MONGO_CONFIGURED.sub("", text)
    text = _ENVELOPE.sub("", text)
    lines: list[str] = []
    seen: set[str] = set()
    for line in text.splitlines():
        if _DROP_LINE.match(line):
            continue
        line = _PG_SERVER.sub("", line)
        line = _PG_SOCKET.sub("", line)
        line = _PG_LEVEL.sub("", line)
        line = _ENVELOPE.sub("", line).strip()
        key = line.lower()
        if not line or key in seen:
            continue
        seen.add(key)
        lines.append(line)
    text = " ".join(lines)
    text = _INNER_ENVELOPE.sub("", text)
    text = _DSN.sub("the connection string", text)
    text = _MYSQL_USER_HOST.sub(r"'\1'", text)
    text = _ON_HOST.sub("server", text)
    text = _IPV6.sub("the host", text)
    text = _IPV4_PORT.sub("the host", text)
    text = _HOST_PORT.sub("the host", text)
    text = _URL.sub("", text)
    text = _QUOTED_VALUE_AFTER_TYPE.sub(r"\1", text)
    text = _MYSQL_VALUE.sub(r"\1", text)
    text = _DUP_ENTRY.sub("Duplicate entry", text)
    text = _KEY_VALUE.sub(r"Key (\1)", text)
    text = re.sub(r"\(\s*\)", "", text)
    text = _WS.sub(" ", redact_log_text(text)).strip(" :;,")
    return _shorten(text, limit)


# -- object extraction --------------------------------------------------------

def _last_part(name: str) -> str:
    name = (name or "").strip().strip('"`[]')
    return name.split(".")[-1].strip('"`[]') if name else ""


_OBJ_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("table", re.compile(r'relation "([^"]+)"')),
    ("table", re.compile(r"Table '([^']+)' doesn't exist")),
    ("table", re.compile(r"no such table: ([\w.]+)")),
    ("table", re.compile(r"permission denied for (?:table|relation|view|sequence) ([\w.\"]+)")),
    ("table", re.compile(r"command denied to user '[^']*'(?:@'[^']*')? for table '([^']+)'")),
    ("table", re.compile(r"The table '([^']+)' is full")),
    ("table", re.compile(r"ns (?:does not exist|not found): ([\w.$-]+)")),
    ("schema", re.compile(r'schema "([^"]+)"')),
    ("schema", re.compile(r"permission denied for schema ([\w\"]+)")),
    ("database", re.compile(r'database "([^"]+)" does not exist')),
    ("database", re.compile(r"Unknown database '([^']+)'")),
    ("database", re.compile(r"to database '([^']+)'")),
    ("column", re.compile(r'column "([^"]+)"')),
    ("column", re.compile(r"[Uu]nknown column '([^']+)'")),
    ("column", re.compile(r"[Cc]olumn '([^']+)'")),
    ("column", re.compile(r"for column '([^']+)'")),
    ("column", re.compile(r"no such column: ([\w.]+)")),
    ("column", re.compile(r"NOT NULL constraint failed: ([\w.]+)")),
    ("constraint", re.compile(r'constraint "([^"]+)"')),
    ("constraint", re.compile(r"for key '([^']+)'")),
    ("constraint", re.compile(r"[Cc]heck constraint '([^']+)'")),
    ("function", re.compile(r"function ([\w.]+)\(")),
    ("user", re.compile(r'for user "([^"]+)"')),
    ("user", re.compile(r"for user '([^']*)'")),
    ("user", re.compile(r'^role "([^"]+)" does not exist')),
    ("type", re.compile(r"for type ([\w ]+(?:\(\d+(?:,\s*\d+)?\))?)")),
    ("type", re.compile(r"is of type ([\w ]+?) but")),
    ("type", re.compile(r"Incorrect (\w+) value")),
    ("near", re.compile(r'(?:at or )?near "([^"]+)"')),
    ("near", re.compile(r"near '(\S+)")),
    ("near", re.compile(r'near "(\S+)": syntax error')),
    ("relation_of", re.compile(r'of relation "([^"]+)"')),
)


def _extract_object(exc: Any, raw: str) -> dict[str, str]:
    found: dict[str, str] = {}
    root = _root(exc)
    diag = getattr(root, "diag", None)
    if diag is not None:
        for attr, key in (
            ("table_name", "table"), ("column_name", "column"), ("schema_name", "schema"),
            ("constraint_name", "constraint"), ("datatype_name", "type"),
        ):
            val = getattr(diag, attr, None)
            if isinstance(val, str) and val:
                found[key] = val
    for key, pat in _OBJ_PATTERNS:
        if key in found:
            continue
        m = pat.search(raw)
        if m:
            found[key] = m.group(1).strip()
    if "relation_of" in found:
        found.setdefault("table", found["relation_of"])
        found.pop("relation_of")
    for key in ("table", "column", "schema"):
        if key in found:
            found[key] = _last_part(found[key])
    if found.get("near"):
        found["near"] = found["near"].strip("'\"")[:40]
    if found.get("constraint", "").upper() == "PRIMARY" or found.get("constraint", "").endswith(".PRIMARY"):
        found["constraint"] = "PRIMARY KEY"
    elif "." in found.get("constraint", ""):
        found["constraint"] = found["constraint"].split(".")[-1]
    return found


# -- classification -----------------------------------------------------------

@dataclass
class ClassifiedError:
    code: str
    title: str
    message: str
    action: dict[str, str]
    object: dict[str, Any]
    retriable: bool
    driver: dict[str, str]
    side: str
    phase: str
    sentence: str
    matched_by: str

    @property
    def text(self) -> str:
        """Message and the one next action: what every surface shows."""
        return f"{self.message} {self.action.get('label', '')}".strip()

    def user_text(self, reference: str = "") -> str:
        return f"{self.text} Reference: {reference}" if reference else self.text

    def envelope(self, reference: str) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "title": self.title,
            "message": self.message,
            "action": dict(self.action),
            "object": dict(self.object),
            "retriable": self.retriable,
            "reference": reference,
            "driver": dict(self.driver),
        }
        if legacy_code(self.code):
            error["legacy_code"] = legacy_code(self.code)  # deprecated, one release
        return {"error": error, "detail": self.user_text(reference)}

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code, "title": self.title, "message": self.message,
            "action": dict(self.action), "object": dict(self.object),
            "retriable": self.retriable, "driver": dict(self.driver),
            "side": self.side, "phase": self.phase,
        }


class _Blank(dict):
    def __missing__(self, key: str) -> str:
        return ""


def _q(name: str) -> str:
    return f' "{name}"' if name else ""


#: Codes whose source text is Datawrap's own refusal and already names the
#: object and the fix (the egress policy names the refused host the user
#: typed). Humanisers keep that text; the code still rides on the envelope.
PRODUCT_COPY_CODES = frozenset({"network.egress_blocked", "access.path_not_allowed", "timeout.connect_deadline"})

_SIDE_NOUN = {"source": "source", "destination": "destination", "query": "database",
              "connection": "database", "platform": "Datawrap server"}


def _render(code: str, *, engine: str, side: str, obj: dict[str, Any], sentence: str) -> tuple[str, str, dict[str, str]]:
    tpl = TEMPLATES[code]
    t = str(obj.get("table") or obj.get("collection") or "")
    c = str(obj.get("column") or "")
    k = str(obj.get("constraint") or "")
    typ = str(obj.get("type") or "")
    db = str(obj.get("database") or "")
    s = str(obj.get("schema") or "")
    f = str(obj.get("file") or "")
    if f:
        f = f.replace("\\", "/").rsplit("/", 1)[-1]
    fn = str(obj.get("function") or "")
    named = t or s or db
    values = _Blank(
        engine=engine_name(engine),
        t=_q(t), c=_q(c), k=_q(k), db=_q(db), s=_q(s), f=_q(f), fn=_q(fn),
        to_t=f" to table{_q(t)}" if t else "",
        tbl=f" table{_q(t)}" if t else " the table",
        u=_q(str(obj.get("user") or "")),
        for_user=f" for user{_q(str(obj.get('user')))}" if obj.get("user") else " for this connection",
        in_t=f" in table{_q(t)}" if t and code != "object.table_not_found" else "",
        on_t=f" on table{_q(t)}" if t else "",
        in_s=f" in schema{_q(s)}" if s else "",
        in_k=f" (constraint{_q(k)})" if k else "",
        to_obj=f" to{_q(named)}" if named else "",
        as_type=f" ({typ})" if typ else "",
        near=f" near{_q(str(obj.get('near') or ''))}" if obj.get("near") else "",
        side_on={"source": " on the source", "destination": " on the destination"}.get(side, ""),
        side_noun=_SIDE_NOUN.get(side, "database"),
        sentence=f" The database said: {sentence}" if sentence else "",
    )
    message = tpl.message.format_map(values)
    label, ui = tpl.side_actions.get(side, (tpl.action, tpl.ui))
    label = label.format_map(values)
    action = {"label": label, "ui": ui, "api": tpl.api}
    message = _WS.sub(" ", message).strip()
    if message and message[-1] not in ".!?":
        message += "."
    return tpl.title, message, action


def _access_code(side: str, phase: str, raw: str) -> str:
    low = raw.lower()
    m = re.search(r"\b(select|insert|update|delete|create|alter|drop|index|references)\b command denied", low)
    command = m.group(1) if m else ""
    ddl = phase == "ddl" or command in {"create", "alter", "drop", "index", "references"} or (
        "permission denied for schema" in low and side == "destination"
    )
    if side == "source" or (not side and command == "select"):
        return "access.source_read_denied"
    if side == "destination":
        return "access.dest_ddl_denied" if ddl else "access.dest_write_denied"
    return "access.denied"


_PG_CODES: dict[str, str] = {
    "28P01": "auth.password_rejected", "3D000": "object.database_not_found",
    "3F000": "object.schema_not_found", "42P01": "object.table_not_found",
    "42703": "object.column_not_found", "42601": "query.syntax", "42883": "query.undefined_function",
    "22012": "query.division_by_zero", "23502": "value.not_null", "42804": "value.type_mismatch",
    "22P02": "value.type_mismatch", "22007": "value.type_mismatch", "22008": "value.type_mismatch",
    "22001": "value.too_long", "22003": "value.numeric_overflow", "23514": "value.check_violation",
    "23505": "value.duplicate_key", "23503": "value.fk_violation", "57014": "timeout.statement",
    "55P03": "timeout.lock", "40P01": "timeout.deadlock", "53100": "capacity.disk_full",
    "53300": "capacity.connection_limit", "57P01": "network.connection_lost",
}
_MYSQL_CODES: dict[str, str] = {
    "1045": "auth.password_rejected", "1044": "access.database_not_granted",
    "1049": "object.database_not_found", "1146": "object.table_not_found",
    "1054": "object.column_not_found", "1064": "query.syntax", "1365": "query.division_by_zero",
    "1048": "value.not_null", "1364": "value.not_null", "1366": "value.type_mismatch",
    "1265": "value.type_mismatch", "1406": "value.too_long", "1264": "value.numeric_overflow",
    "3819": "value.check_violation", "1062": "value.duplicate_key", "1452": "value.fk_violation",
    "1451": "value.fk_violation", "3024": "timeout.statement", "1205": "timeout.lock",
    "1213": "timeout.deadlock", "1114": "capacity.table_full", "1040": "capacity.connection_limit",
    "1203": "capacity.connection_limit", "2005": "network.dns_not_found",
    "2006": "network.connection_lost", "2013": "network.connection_lost",
}
_MONGO_CODES: dict[str, str] = {
    "18": "auth.mongo_auth_source", "26": "object.collection_not_found",
    "11000": "value.duplicate_key", "50": "timeout.statement", "9": "query.syntax",
}
_SQLITE_NAMES: dict[str, str] = {
    "SQLITE_CANTOPEN": "object.file_not_found", "SQLITE_BUSY": "timeout.lock",
    "SQLITE_LOCKED": "timeout.lock", "SQLITE_FULL": "capacity.disk_full",
    "SQLITE_CONSTRAINT_UNIQUE": "value.duplicate_key", "SQLITE_CONSTRAINT_PRIMARYKEY": "value.duplicate_key",
    "SQLITE_CONSTRAINT_NOTNULL": "value.not_null", "SQLITE_CONSTRAINT_CHECK": "value.check_violation",
    "SQLITE_CONSTRAINT_FOREIGNKEY": "value.fk_violation",
}
_ACCESS_CODES = {"42501", "1142", "1143", "13"}
_REPLICATION_GRANT = re.compile(r"replication|walsender|superuser|binlog|binary log", re.I)


def _structured(engine: str, code: str) -> str:
    if not code:
        return ""
    if code in _ACCESS_CODES:
        return "access"
    if engine in {"postgresql", "redshift"} or (len(code) == 5 and not code.isdigit()):
        return _PG_CODES.get(code, "")
    if engine == "mongodb":
        return _MONGO_CODES.get(code, "")
    if engine == "sqlite":
        return _SQLITE_NAMES.get(code, "")
    return _MYSQL_CODES.get(code, "")


# Anchored per-engine text. Each pattern names the driver phrase it matches;
# nothing matches a bare keyword ("auth", "login") inside an object name.
_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("config.driver_missing", re.compile(
        r"^platform driver missing:|^sqlalchemy dialect/driver for |^no module named |"
        r"^can't load plugin: sqlalchemy\.dialects|connector driver is not installed", re.I)),
    ("network.egress_blocked", re.compile(r"^refused connector host \S+: (?!dns did not resolve)", re.I)),
    # SEC-9's own refusal (services.local_file_policy), when a probe passes it on as text.
    ("access.path_not_allowed", re.compile(r"(?:file is|names a key file) outside the folders connectors may open", re.I)),
    ("network.dns_not_found", re.compile(
        r"^refused connector host \S+: dns did not resolve|could not translate host name|"
        r"name or service not known|nodename nor servname provided|temporary failure in name resolution|"
        r"no address associated with hostname|getaddrinfo failed|\[errno -[235]\]|"
        r"the dns query name does not exist|unknown mysql server host", re.I)),
    ("auth.password_rejected", re.compile(
        r"password authentication failed for user|access denied for user '[^']*'(?:@'[^']*')? \(using password|"
        r"login failed for user|ora-01017|invalid username/password", re.I)),
    ("auth.user_unknown", re.compile(r'^role "[^"]+" does not exist', re.I)),
    ("auth.mongo_auth_source", re.compile(r"^authentication failed\.?$|'codename': 'authenticationfailed'", re.I)),
    ("object.database_not_found", re.compile(r'database "[^"]+" does not exist|unknown database \'', re.I)),
    ("network.tls_untrusted", re.compile(
        r"certificate verify failed|self[- ]signed certificate|^tls certificate could not be verified|"
        r"certificate isn't trusted", re.I)),
    ("network.tls_required", re.compile(r"^tls mismatch\b|^the server refused the tls handshake", re.I)),
    ("network.tls_required", re.compile(
        r"server does not support ssl, but ssl was required|insecure transport are prohibited|"
        r"no pg_hba\.conf entry for host .*(?:no encryption|ssl off)|ssl connection is required", re.I)),
    ("timeout.statement", re.compile(
        r"canceling statement due to statement timeout|maximum statement execution time exceeded|"
        r"operation exceeded time limit", re.I)),
    ("timeout.lock", re.compile(
        r"lock wait timeout exceeded|canceling statement due to lock timeout|^database is locked|"
        r"lock request time out period exceeded", re.I)),
    ("timeout.deadlock", re.compile(r"deadlock detected|deadlock found when trying to get lock", re.I)),
    ("network.handshake_timeout", re.compile(
        r"lost connection to (?:mysql )?server (?:during query|at '?reading initial communication packet|"
        r"at '?handshake)[^)]*\(timed out\)|reading initial communication packet|"
        r"accepted the tcp connection but did not answer", re.I)),
    ("network.refused", re.compile(r"connection refused|\[errno 111\]|econnrefused|actively refused", re.I)),
    ("network.unreachable", re.compile(r"network is unreachable|no route to host|host is unreachable|\[errno 11[03]\]", re.I)),
    ("network.connect_timeout", re.compile(
        r"timeout expired|connection timed out|timed out connecting|serverselectiontimeout|"
        r"connect timeout|\[errno 110\]|login timeout expired", re.I)),
    ("network.connection_lost", re.compile(
        r"server closed the connection unexpectedly|lost connection to mysql server during query|"
        r"mysql server has gone away|terminating connection due to administrator command|connection reset by peer", re.I)),
    ("object.file_not_found", re.compile(r"^unable to open database file|\[errno 2\] no such file", re.I)),
    ("object.table_not_found", re.compile(
        r"^no such table: |relation \"[^\"]+\" does not exist|table '[^']+' doesn't exist|"
        r"invalid object name '|ora-00942", re.I)),
    ("object.column_not_found", re.compile(
        r"^no such column: |column \"[^\"]+\" (?:of relation \"[^\"]+\" )?does not exist|unknown column '|"
        r"invalid column name '|has no column named ", re.I)),
    ("object.schema_not_found", re.compile(r'schema "[^"]+" does not exist|unknown schema ', re.I)),
    ("value.duplicate_key", re.compile(
        r"duplicate key value violates unique constraint|^duplicate entry |unique constraint failed: |"
        r"e11000 duplicate key|violation of primary key constraint|ora-00001", re.I)),
    ("value.not_null", re.compile(
        r"violates not-null constraint|not null constraint failed: |column '[^']+' cannot be null|"
        r"cannot insert the value null into column|ora-01400", re.I)),
    ("value.check_violation", re.compile(
        r"violates check constraint|check constraint failed|check constraint '[^']+' is violated|"
        r"conflicted with the check constraint|ora-02290", re.I)),
    ("value.fk_violation", re.compile(r"violates foreign key constraint|foreign key constraint failed|"
                                      r"a foreign key constraint fails", re.I)),
    ("value.too_long", re.compile(r"value too long for type|data too long for column|string or binary data would be truncated", re.I)),
    ("value.numeric_overflow", re.compile(r"numeric field overflow|out of range value for column|integer out of range|"
                                          r"arithmetic overflow error", re.I)),
    ("value.type_mismatch", re.compile(r"invalid input syntax for type|is of type \w[\w ]* but expression is of type|"
                                       r"incorrect \w+ value: |datatype mismatch|conversion failed when converting", re.I)),
    ("query.division_by_zero", re.compile(r"division by zero|divide by zero", re.I)),
    ("query.syntax", re.compile(r"syntax error at or near|you have an error in your sql syntax|"
                                r"near \"[^\"]+\": syntax error|incorrect syntax near", re.I)),
    ("access", re.compile(r"permission denied for (?:table|relation|schema|view|sequence|database)|"
                          r"command denied to user|^not authorized on \S+ to execute", re.I)),
    ("capacity.disk_full", re.compile(r"no space left on device|could not extend file|disk full|\benospc\b", re.I)),
    ("capacity.table_full", re.compile(r"the table '[^']+' is full|tablespace is full|innodb: error: tablespace", re.I)),
    ("capacity.connection_limit", re.compile(r"too many connections|remaining connection slots are reserved|"
                                             r"sorry, too many clients already", re.I)),
    ("config.invalid_connection_string", re.compile(
        r"invalid uri|invalid connection string|could not parse rfc1738 url|invalid dsn", re.I)),
)


def _typed(exc: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(exc, BaseException):
        return "", {}
    for cur in (exc, getattr(exc, "__cause__", None)):
        code = getattr(cur, "error_code", None)
        if isinstance(code, str) and code in TEMPLATES:
            return code, dict(getattr(cur, "error_object", None) or {})
    if isinstance(exc, ModuleNotFoundError):
        package = (exc.name or "").split(".")[0]
        return "config.driver_missing", {"engine": _DRIVER_PACKAGES.get(package, "")}
    if isinstance(exc, socket.gaierror):
        return "network.dns_not_found", {}
    if isinstance(exc, FileNotFoundError):
        return "object.file_not_found", {"file": exc.filename or ""}
    if isinstance(exc, OSError) and not _infer_engine(exc):
        number = exc.errno
        if number == _errno.ENOSPC:
            return "capacity.disk_full", {}
        if number == _errno.ECONNREFUSED or isinstance(exc, ConnectionRefusedError):
            return "network.refused", {}
        if number in {_errno.EHOSTUNREACH, _errno.ENETUNREACH}:
            return "network.unreachable", {}
        if number == _errno.ETIMEDOUT or isinstance(exc, TimeoutError):
            return "network.connect_timeout", {}
        if number in {_errno.EACCES, _errno.EPERM}:
            return "access.denied", {"file": exc.filename or ""}
    return "", {}


def _anchored(raw: str) -> str:
    text = raw.strip()
    for code, pat in _PATTERNS:
        if pat.search(text):
            return code
    return ""


def classify(
    exc: Any,
    *,
    side: str = "",
    phase: str = "",
    engine: str = "",
    object: dict[str, Any] | None = None,  # noqa: A002 — the envelope's field name
    fallback: bool = True,
) -> ClassifiedError | None:
    """Classify ``exc`` (an exception or driver text) into one taxonomy code.

    ``side`` is source / destination / connection / query; ``phase`` is
    connect / read / write / ddl / introspect / probe / query. A tagged
    exception (:func:`tag_failure`) supplies them when the caller does not.
    Returns ``None`` only when ``fallback`` is False and nothing matched.
    """
    side = side or str(getattr(exc, "datawrap_side", "") or "")
    phase = phase or str(getattr(exc, "datawrap_phase", "") or "")
    drv_engine, drv_code = driver_code(exc)
    engine = (engine or drv_engine or _infer_engine(exc) or "").lower()
    raw = _raw_driver_text(exc)
    full_raw = str(exc) if not isinstance(exc, str) else exc
    matched_by = "typed"
    code, typed_obj = _typed(exc)
    if not code and drv_code:
        code = _structured(engine, drv_code)
        matched_by = "structured"
        if code == "access" and _REPLICATION_GRANT.search(raw):
            # A replication / log privilege is a CDC setup grant with its own
            # remedy (error_handling's cdc rules), not a table SELECT/INSERT.
            code = ""
    clean = driver_sentence(exc, limit=2000)
    if not code:
        code = _anchored(raw) or _anchored(clean) or (_anchored(full_raw) if full_raw != raw else "")
        matched_by = "pattern"
    if code == "access":
        code = _access_code(side, phase, raw)
    if code == "network.connection_lost" and phase in {"connect", "probe", "introspect"}:
        # MySQL 2013 at connect is a handshake that never finished, not a drop.
        if _anchored(raw) == "network.handshake_timeout":
            code = "network.handshake_timeout"
    if not code:
        if not fallback:
            return None
        code = "unclassified"
        matched_by = "fallback"
    if code == "auth.password_rejected" and engine == "mongodb":
        code = "auth.mongo_auth_source"
    obj = _extract_object(exc, raw)
    for key, val in _extract_object(None, clean).items():
        obj.setdefault(key, val)
    obj.update({k: v for k, v in typed_obj.items() if v and k != "engine"})
    if typed_obj.get("engine") and not engine:
        engine = str(typed_obj["engine"])
    if code == "config.driver_missing" and not engine:
        m = re.search(r"dialect/driver for '([^']+)'|no module named '?([\w]+)", full_raw, re.I)
        if m:
            engine = m.group(1) or _DRIVER_PACKAGES.get(m.group(2) or "", "")
    for key, val in (object or {}).items():
        if val and not obj.get(key):
            obj[key] = val
    if code == "object.schema_not_found" and str(obj.get("schema") or "").lower() == "public" and engine == "snowflake":
        obj["schema"] = "public"
    sentence = driver_sentence(exc) if code == "unclassified" else ""
    title, message, action = _render(code, engine=engine, side=side, obj=obj, sentence=sentence)
    public_obj = {k: v for k, v in obj.items() if k in {
        "kind", "id", "name", "table", "column", "schema", "database", "constraint", "function", "collection", "user",
    } and v}
    if obj.get("file"):
        public_obj["file"] = str(obj["file"]).replace("\\", "/").rsplit("/", 1)[-1]
    if side:
        public_obj["side"] = side
    return ClassifiedError(
        code=code,
        title=title,
        message=message,
        action=action,
        object=public_obj,
        retriable=TEMPLATES[code].retriable,
        driver={"engine": engine, "code": drv_code} if (engine or drv_code) else {},
        side=side,
        phase=phase,
        sentence=driver_sentence(exc),
        matched_by=matched_by,
    )


def user_message(exc: Any, **kw: Any) -> str:
    """The one text every surface shows for ``exc`` (no reference)."""
    classified = classify(exc, **kw)
    assert classified is not None  # fallback=True
    return classified.text


def new_reference() -> str:
    from services import tracing

    return tracing.get_correlation_id() or uuid.uuid4().hex


# -- side / phase tagging -----------------------------------------------------

def tag_failure(exc: BaseException, *, side: str, phase: str) -> BaseException:
    """Record which side and phase raised ``exc``. The innermost tag wins."""
    try:
        if not getattr(exc, "datawrap_side", ""):
            exc.datawrap_side = side  # type: ignore[attr-defined]
            exc.datawrap_phase = phase  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        pass
    return exc


@contextmanager
def failure_scope(side: str, phase: str) -> Iterator[None]:
    """Tag any exception leaving the block with ``side`` and ``phase``."""
    try:
        yield
    except BaseException as exc:
        tag_failure(exc, side=side, phase=phase)
        raise


def failure_side(side: str, phase: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator form of :func:`failure_scope` for reader / writer entry points."""

    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with failure_scope(side, phase):
                return fn(*args, **kwargs)

        return wrapper

    return deco


#: Plain words for internal sync-mode and drift ids in user copy (ERR-35).
_SYNC_MODE_WORDS = {
    "full_refresh_append": "append", "full_refresh_overwrite": "overwrite",
    "incremental_append": "incremental append", "incremental_deduped": "upsert",
    "incremental_dedup": "upsert", "append": "append", "upsert": "upsert", "overwrite": "overwrite",
    "mirror": "mirror", "scd2": "history (SCD2)", "cdc": "change capture (CDC)",
}
_DRIFT_KIND_WORDS = {
    "type_change": "a column's type changed", "column_removed": "a column was removed",
    "column_added": "a column was added", "column_renamed": "a column was renamed",
    "nullability_change": "a column's nullability changed", "pk_change": "the primary key changed",
}


def sync_mode_words(mode: str) -> str:
    key = str(mode or "").strip().lower()
    return _SYNC_MODE_WORDS.get(key, key.replace("_", " "))


def drift_kind_words(kind: str) -> str:
    key = str(kind or "").strip().lower()
    return _DRIFT_KIND_WORDS.get(key, key.replace("_", " "))
