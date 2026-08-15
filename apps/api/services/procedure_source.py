"""Stored-procedure / custom-SQL extract — one source-read owner.

Transfer Studio sources are table, query, or procedure. This module is the
only parser, security gate, peek, and paged reader for query/procedure mode.

Competitor facts (cite; do not conclude a product is "not good"):
- Airbyte incremental docs: at-least-once via inclusive cursors
  (docs.airbyte.com incremental-append-deduped). No first-class paste-SP
  source (github.com/airbytehq/airbyte/issues/7010, discussion #36068).
- Fivetran: upsert + checkpoint; custom extract via Connector SDK, not a
  Studio "paste CALL" control.
- Informatica CDI: stored procedure *is* a source path (SQL Override
  ``call a();`` plus a dummy SELECT for metadata — Informatica KB HOW-TO).
- AWS DMS: tables/views; views full-load only; source SPs are not the extract.

DataFlow:
- Result columns go through ``map_columns`` (Map SSOT) and
  ``shape_contract`` (dest-exists extra columns = remap, never silent drop).
- Procedure extract is a **snapshot of the result set**, not CDC.
- CDC / exactly-once stay **at-least-once upsert** until dest-engine EOS
  is proven. This module refuses CDC + callable source.

SQLite has no stored procedures — procedure mode is rejected; query mode
(SELECT) is allowed.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

logger = logging.getLogger(__name__)

MODE_TABLE = "table"
MODE_QUERY = "query"
MODE_PROCEDURE = "procedure"
CALLABLE_MODES = frozenset({MODE_QUERY, MODE_PROCEDURE})

#: Dialects that can execute CALL/EXEC (or a set-returning function).
PROCEDURE_DIALECTS = frozenset({
    "postgresql",
    "postgres",
    "pgvector",
    "redshift",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
    "snowflake",
    "generic_sql",
    "greenplum",
    "cockroachdb",
    "timescaledb",
    "yugabytedb",
    "alloydb",
    "amazon_rds_postgresql",
    "amazon_rds_mysql",
    "amazon_rds_sql_server",
    "azure_sql_database",
    "microsoft_sql_server",
})

QUERY_ONLY_DIALECTS = frozenset({"sqlite", "duckdb"})

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_QUALIFIED = rf"(?:{_IDENT}\.){{0,2}}{_IDENT}"

_BARE_IDENT_RE = re.compile(rf"^\s*({_QUALIFIED})\s*;?\s*$", re.IGNORECASE)
_CALL_RE = re.compile(
    rf"^\s*(CALL)\s+({_QUALIFIED})\s*(?:\((.*)\))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_EXEC_RE = re.compile(
    rf"^\s*(EXEC(?:UTE)?)\s+({_QUALIFIED})\s*(.*?)?;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SELECT_FUNC_RE = re.compile(
    rf"^\s*SELECT\s+\*\s+FROM\s+({_QUALIFIED})\s*\((.*)\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_DENIED_NAME_PREFIXES = (
    "xp_",
    "sp_configure",
    "sp_executesql",
    "sp_password",
    "sp_addlogin",
    "sp_droplogin",
    "sp_grantdbaccess",
)

_DENIED_TOKENS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|"
    r"MERGE|COPY|LOAD|REPLACE|OPENROWSET|OPENDATASOURCE|OPENQUERY|"
    r"BULK|SHUTDOWN|DBCC|xp_cmdshell|INTO\s+OUTFILE|INTO\s+DUMPFILE|"
    r"EXECUTE\s+IMMEDIATE|EXEC\s*\(|sp_executesql"
    r")\b",
    re.IGNORECASE,
)

_COMMENT_LINE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)

_BIND_NAME = re.compile(r"^:[A-Za-z_][A-Za-z0-9_]*$")
_BIND_AT = re.compile(r"^@[A-Za-z_][A-Za-z0-9_]*$")
_NUMBER = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")
_STRING = re.compile(r"^'(?:[^']|'')*'$")

PEEK_ROW_LIMIT = 100
DEFAULT_TIMEOUT_S = 300
PEEK_TIMEOUT_S = 30


class ProcedureSourceError(ValueError):
    """Operator-visible refusal — never execute the payload."""


@dataclass(frozen=True)
class CallableSpec:
    mode: str
    dialect: str
    verb: str
    identifier: str
    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    stream_name: str = ""


@dataclass
class _ResultSpool:
    path: Path
    headers: list[str]
    total: int
    schema: dict[str, str]
    job_id: str = ""


_SPOOL_LOCK = threading.Lock()
_SPOOLS: dict[str, _ResultSpool] = {}


def job_id_of(cfg: Mapping[str, Any] | None) -> str:
    """Job id stamped on the source cfg so CALL spool is not process-global."""
    if not isinstance(cfg, Mapping):
        return ""
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
    return str(cfg.get("job_id") or extra.get("job_id") or "").strip()


def stamp_callable_job_id(cfg: dict[str, Any], job_id: str | None) -> dict[str, Any]:
    """Copy ``job_id`` onto the reader cfg so paging and cleanup share one spool."""
    jid = str(job_id or "").strip()
    if not jid:
        return cfg
    out = dict(cfg)
    out["job_id"] = jid
    extra = dict(out.get("extra") or {}) if isinstance(out.get("extra"), dict) else {}
    extra["job_id"] = jid
    out["extra"] = extra
    return out


def source_read_mode_of(source: Any) -> str:
    """Resolve ``table`` | ``query`` | ``procedure`` from endpoint or cfg dict."""
    extra: Mapping[str, Any] = {}
    if source is None:
        return MODE_TABLE
    if hasattr(source, "extra"):
        extra = getattr(source, "extra", None) or {}
        root = extra
    elif isinstance(source, Mapping):
        nested = source.get("extra") if isinstance(source.get("extra"), Mapping) else {}
        root = {**dict(nested or {}), **{k: v for k, v in source.items() if k != "extra"}}
        extra = root
    else:
        return MODE_TABLE
    raw = str(extra.get("source_read_mode") or "").strip().lower()
    if raw in {MODE_TABLE, MODE_QUERY, MODE_PROCEDURE}:
        return raw
    if extra.get("procedure_call") or extra.get("source_procedure"):
        return MODE_PROCEDURE
    if extra.get("source_query"):
        return MODE_QUERY
    return MODE_TABLE


def is_callable_source(source: Any) -> bool:
    return source_read_mode_of(source) in CALLABLE_MODES


def callable_identity_token(source: Any) -> str:
    """Watermark identity for a CALL/SELECT — stream name plus SQL+binds digest.

    Two extracts that share a stream label (``get_orders``) but differ in SQL
    or bound params must not share a cursor. A colliding physical table name
    is not part of this token.
    """
    spec = parse_callable_source(
        procedure_text_of(source),
        dialect=dialect_of(source),
        mode=source_read_mode_of(source),
        params=procedure_params_of(source),
    )
    payload = json.dumps(
        {"sql": spec.sql, "params": spec.params},
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{stream_name_for_callable(spec)}:{digest}"


def source_object_for_cursor(source: Any, fallback: str = "") -> str:
    """Cursor-key object: callable identity token, else the table/stream name."""
    if not is_callable_source(source):
        return fallback
    try:
        return callable_identity_token(source)
    except ProcedureSourceError:
        return fallback or "procedure_result"


def procedure_text_of(source: Any) -> str:
    extra: Mapping[str, Any] = {}
    if hasattr(source, "extra"):
        extra = getattr(source, "extra", None) or {}
    elif isinstance(source, Mapping):
        nested = source.get("extra") if isinstance(source.get("extra"), Mapping) else {}
        extra = {**dict(nested or {}), **source}
    return str(
        extra.get("procedure_call")
        or extra.get("source_procedure")
        or extra.get("source_query")
        or ""
    ).strip()


def procedure_params_of(source: Any) -> dict[str, Any]:
    extra: Mapping[str, Any] = {}
    if hasattr(source, "extra"):
        extra = getattr(source, "extra", None) or {}
    elif isinstance(source, Mapping):
        nested = source.get("extra") if isinstance(source.get("extra"), Mapping) else {}
        extra = {**dict(nested or {}), **source}
    raw = extra.get("procedure_params") or extra.get("source_query_params") or {}
    if not isinstance(raw, Mapping):
        return {}
    return {str(k): v for k, v in raw.items()}


def dialect_of(source: Any) -> str:
    if hasattr(source, "format"):
        fmt = str(getattr(source, "format", "") or "").lower()
        extra = getattr(source, "extra", None) or {}
        return str(extra.get("type") or fmt).lower()
    if isinstance(source, Mapping):
        return str(source.get("type") or source.get("format") or "").lower()
    return ""


def stream_name_for_callable(spec: CallableSpec) -> str:
    leaf = spec.identifier.split(".")[-1] if spec.identifier else "procedure_result"
    return re.sub(r"[^A-Za-z0-9_]+", "_", leaf) or "procedure_result"


#: Sync modes that treat the source as a table identity or a WAL/binlog.
#: A CALL/SELECT result is a snapshot — refuse these so we do not delete dest
#: rows the procedure never listed, close SCD2 windows, or claim CDC.
CALLABLE_REFUSED_SYNC_MODES = frozenset({"cdc", "scd2", "mirror"})


def callable_sync_refusal(
    sync_mode: str,
    source: Any = None,
    *,
    source_read_mode: str = "",
) -> str | None:
    """Why this sync mode cannot drive a CALL/SELECT extract, or None if allowed."""
    if source is not None:
        if not is_callable_source(source):
            return None
    else:
        mode = (source_read_mode or MODE_TABLE).strip().lower()
        if mode not in CALLABLE_MODES:
            return None
    from services.sync_cursor import normalize_sync_mode

    sync = normalize_sync_mode(sync_mode, default="")
    if sync == "cdc":
        return (
            "Stored-procedure and custom-SQL sources are a result-set snapshot, "
            "not a CDC log. Use Full refresh (or incremental only when the "
            "procedure itself is cursor-stable). CDC stays at-least-once on "
            "table sources until dest-engine exactly-once is proven."
        )
    if sync in {"scd2", "mirror"}:
        return (
            f"{sync.upper()} versions or deletes against a table identity. "
            "A CALL/SELECT result is a snapshot, not a keyed table population — "
            "refusing so we do not close SCD2 windows or delete dest rows the "
            "procedure never listed. Use Full refresh or incremental when the "
            "procedure is cursor-stable."
        )
    return None


def assert_callable_sync_allowed(sync_mode: str, source: Any) -> None:
    """CDC / SCD2 / mirror cannot be driven by a CALL result set."""
    reason = callable_sync_refusal(sync_mode, source)
    if reason:
        raise ProcedureSourceError(reason)


def parse_callable_source(
    text: str,
    *,
    dialect: str = "",
    mode: str = MODE_PROCEDURE,
    params: Mapping[str, Any] | None = None,
) -> CallableSpec:
    """Parse and refuse unsafe CALL/EXEC/SELECT. Bound params only."""
    dialect_n = (dialect or "").strip().lower()
    mode_n = (mode or MODE_PROCEDURE).strip().lower()
    if mode_n not in CALLABLE_MODES:
        raise ProcedureSourceError(f"Unknown source read mode {mode!r}")

    stripped = _strip_comments(text)
    if not stripped:
        raise ProcedureSourceError(
            "Paste a single CALL / EXEC, or a read-only SELECT, then continue."
        )
    if ";" in stripped.rstrip().rstrip(";"):
        raise ProcedureSourceError(
            "Only one statement is allowed — remove extra semicolons."
        )
    if _DENIED_TOKENS.search(stripped):
        raise ProcedureSourceError(
            "This statement is not an extract — DDL, DML, and admin calls are blocked."
        )

    if mode_n == MODE_QUERY:
        return _parse_query(stripped, dialect_n, params or {})

    if dialect_n in QUERY_ONLY_DIALECTS:
        raise ProcedureSourceError(
            f"{dialect_n or 'This engine'} has no stored procedures — "
            "use Table or a read-only SELECT."
        )
    if dialect_n and dialect_n not in PROCEDURE_DIALECTS and dialect_n != "generic_sql":
        # generic_sql catalog tiles still flow as generic_sql; unknown leaf
        # dialects are allowed only when the operator typed CALL/EXEC explicitly.
        if not re.match(r"^\s*(CALL|EXEC|EXECUTE|SELECT)\b", stripped, re.IGNORECASE):
            raise ProcedureSourceError(
                f"Stored-procedure extract is not offered for '{dialect_n}'."
            )

    wrapped = _wrap_bare_identifier(stripped, dialect_n)
    spec = _parse_procedure(wrapped, dialect_n, params or {})
    _deny_dangerous_identifier(spec.identifier)
    return spec


def compile_callable_sql(spec: CallableSpec) -> tuple[str, dict[str, Any]]:
    """SQLAlchemy ``text()`` SQL plus bind params (never string-concat values)."""
    return spec.sql, dict(spec.params)


def peek_callable_schema(
    headers: list[str],
    rows: list[list[Any]] | list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Infer result-set types from peeked rows — same choke point as playground."""
    from services.schema_inference import infer_schema_map

    samples: dict[str, list[str]] = {h: [] for h in headers}
    for row in rows:
        if isinstance(row, Mapping):
            for h in headers:
                val = row.get(h)
                if val is not None and str(val) != "":
                    samples[h].append(str(val))
        else:
            for i, h in enumerate(headers):
                if i < len(row) and row[i] is not None and str(row[i]) != "":
                    samples[h].append(str(row[i]))
    schema, intel = infer_schema_map(samples)
    for h in headers:
        schema.setdefault(h, "VARCHAR")
    return schema, intel


def map_callable_result(
    source_columns: list[str],
    dest_columns: list[str],
    *,
    source_types: Mapping[str, str] | None = None,
    dest_types: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Map SP/query result columns through Map SSOT + dest-exists shape."""
    from services.semantic_mapper import map_columns
    from services.shape_contract import (
        COL_ADD,
        COL_PENDING,
        COL_UNACCOUNTED,
        classify_dest_exists_shape,
    )

    # Dest-exists must be True when dest columns are known — names-only
    # Hungarian without that flag pins order_id→order_amt (false friend).
    # Do not pass source_schemas here: typed schemas currently derail the
    # same pair. map_columns is Map SSOT; types refine later on Validate.
    mappings = map_columns(
        list(source_columns),
        list(dest_columns),
        destination_table_exists=bool(dest_columns),
    )
    for m in mappings:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        if source_types and src in source_types:
            m.setdefault("source_type", source_types[src])
        if dest_types and tgt in dest_types:
            m.setdefault("target_type", dest_types[tgt])
    table_exists = bool(dest_columns)
    contract = classify_dest_exists_shape(
        destination_table_exists=table_exists if dest_columns else False,
        source_columns=list(source_columns),
        dest_columns=list(dest_columns),
        mappings=list(mappings),
    )
    extra_kinds = {COL_ADD, COL_PENDING, COL_UNACCOUNTED}
    extra = [
        str(col.get("source"))
        for col in (contract.get("columns") or [])
        if col.get("kind") in extra_kinds and col.get("source")
    ]
    for name in contract.get("unaccounted_sources") or []:
        if name not in extra:
            extra.append(str(name))
    return {
        "mappings": mappings,
        "shape_contract": contract,
        "extra_source_columns": extra,
        "write_by": contract.get("write_by"),
    }


def read_callable_batch(
    cfg: Mapping[str, Any],
    *,
    offset: int = 0,
    limit: int = 10_000,
    peek: bool = False,
    columns: list[str] | None = None,
    cursor_column: str | None = None,
    cursor_after: Any = None,
) -> Any:
    """Execute CALL/SELECT once; page from a disk spool so CALL is not replayed.

    Re-executing a procedure at OFFSET N is wrong (side effects, different
    result). Peek fetches at most ``PEEK_ROW_LIMIT`` and does not spool.

    Incremental: filter the spool with ``cursor > cursor_after`` *before*
    OFFSET/LIMIT. The spool is not cursor-sorted, so the caller must pass the
    **run watermark** (not an advancing page max) or later pages lose rows.
    Missing cursor column is fail-closed. Delivery stays at-least-once.
    Peek does not apply the cursor filter — it is schema discovery.
    """
    from connectors.base import ReadBatch

    spec = parse_callable_source(
        procedure_text_of(cfg),
        dialect=str(cfg.get("type") or cfg.get("format") or ""),
        mode=source_read_mode_of(cfg),
        params=procedure_params_of(cfg),
    )
    if peek:
        headers, rows, schema = _execute_live(cfg, spec, limit=min(int(limit or PEEK_ROW_LIMIT), PEEK_ROW_LIMIT))
        if columns:
            headers, rows = _project(headers, rows, columns)
        return ReadBatch(
            headers=headers,
            rows=rows,
            offset=0,
            total_rows=len(rows),
            meta={"native_types": schema, "source_read_mode": spec.mode, "procedure": spec.identifier},
        )

    key = _spool_key(cfg, spec)
    spool = _get_spool(key)
    if spool is None:
        if offset != 0:
            raise ProcedureSourceError(
                "Procedure result was not opened at offset 0 — cannot page a CALL mid-stream."
            )
        spool = _fill_spool(cfg, spec, key)
    headers, rows, matched = _read_spool_page(
        spool,
        offset=offset,
        limit=limit,
        cursor_column=cursor_column,
        cursor_after=cursor_after,
    )
    if columns:
        headers, rows = _project(headers, rows, columns)
    return ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=matched,
        meta={
            "native_types": spool.schema,
            "source_read_mode": spec.mode,
            "procedure": spec.identifier,
            "cursor_column": (cursor_column or "").strip() or None,
            "cursor_filtered": bool((cursor_column or "").strip() and cursor_after not in (None, "")),
        },
    )


def callable_sync_gate(
    *,
    sync_mode: str,
    source_read_mode: str,
    pass_status: str,
    block_status: str,
) -> dict[str, Any] | None:
    """Optional g9 detail — None when the source is a table or the mode is allowed."""
    reason = callable_sync_refusal(sync_mode, source_read_mode=source_read_mode)
    if not reason:
        return None
    mode = (source_read_mode or MODE_TABLE).strip().lower()
    from services.sync_cursor import normalize_sync_mode

    sync = normalize_sync_mode(sync_mode, default="")
    return {
        "id": "g9_sync_contract",
        "status": block_status,
        "message": "Stored-procedure / SQL extract cannot drive this sync mode",
        "duration_ms": 0,
        "details": {
            "issues": [reason],
            "sync_mode": sync,
            "source_read_mode": mode,
        },
    }


# ---------------------------------------------------------------------------
# Parse internals
# ---------------------------------------------------------------------------


def _strip_comments(text: str) -> str:
    raw = str(text or "")
    raw = _COMMENT_BLOCK.sub(" ", raw)
    raw = _COMMENT_LINE.sub(" ", raw)
    return " ".join(raw.split()).strip()


def _wrap_bare_identifier(text: str, dialect: str) -> str:
    m = _BARE_IDENT_RE.match(text)
    if not m:
        return text
    ident = m.group(1)
    if dialect in {"sqlserver", "mssql", "azure_sql_database", "microsoft_sql_server", "amazon_rds_sql_server"}:
        return f"EXEC {ident}"
    if dialect in {"postgresql", "postgres", "pgvector", "redshift", "greenplum", "cockroachdb", "timescaledb"}:
        return f"SELECT * FROM {ident}()"
    return f"CALL {ident}()"


def _deny_dangerous_identifier(ident: str) -> None:
    leaf = ident.split(".")[-1].lower()
    for prefix in _DENIED_NAME_PREFIXES:
        if leaf.startswith(prefix) or ident.lower().startswith(prefix):
            raise ProcedureSourceError(
                f"Procedure `{ident}` is blocked — admin / extended procedures are not extracts."
            )


def _query_is_safe(text: str) -> bool:
    """Read-only SELECT-class gate — same deny set as Query Playground fallback.

    Imported ``query_router._is_safe_sql`` pulls FastAPI via ``src.routers``
    package init; keep the extract path import-light and fail closed.
    """
    if not re.match(
        r"^\s*(SELECT|WITH|EXPLAIN|SHOW|DESCRIBE|DESC|ANALYZE|PRAGMA|VALUES)\b",
        text,
        re.IGNORECASE,
    ):
        return False
    if _DENIED_TOKENS.search(text):
        return False
    if re.search(r"\bINTO\s+(OUTFILE|DUMPFILE)\b", text, re.IGNORECASE):
        return False
    if re.search(r"\bSELECT\b[\s\S]*\bINTO\b", text, re.IGNORECASE):
        return False
    return True


def _parse_query(text: str, dialect: str, params: Mapping[str, Any]) -> CallableSpec:
    if not _query_is_safe(text):
        raise ProcedureSourceError(
            "Query source allows one read-only SELECT/WITH — CALL/EXEC belong in Stored procedure mode."
        )
    binds = {str(k): v for k, v in params.items()}
    return CallableSpec(
        mode=MODE_QUERY,
        dialect=dialect,
        verb="SELECT",
        identifier=_first_relation(text),
        sql=text,
        params=binds,
        stream_name=_first_relation(text) or "query_result",
    )


def _first_relation(text: str) -> str:
    m = re.search(r"\bFROM\s+(" + _QUALIFIED + r")", text, re.IGNORECASE)
    return m.group(1) if m else "query_result"


def _parse_procedure(text: str, dialect: str, params: Mapping[str, Any]) -> CallableSpec:
    call = _CALL_RE.match(text)
    if call:
        ident = call.group(2)
        args = _parse_args(call.group(3) or "", params)
        sql, binds = _render_call("CALL", ident, args, dialect)
        return CallableSpec(
            mode=MODE_PROCEDURE,
            dialect=dialect,
            verb="CALL",
            identifier=ident,
            sql=sql,
            params=binds,
            stream_name=ident.split(".")[-1],
        )
    sel = _SELECT_FUNC_RE.match(text)
    if sel:
        ident = sel.group(1)
        args = _parse_args(sel.group(2) or "", params)
        sql, binds = _render_select_func(ident, args)
        return CallableSpec(
            mode=MODE_PROCEDURE,
            dialect=dialect,
            verb="SELECT",
            identifier=ident,
            sql=sql,
            params=binds,
            stream_name=ident.split(".")[-1],
        )
    exe = _EXEC_RE.match(text)
    if exe:
        ident = exe.group(2)
        args = _parse_exec_tail(exe.group(3) or "", params)
        sql, binds = _render_exec(ident, args)
        return CallableSpec(
            mode=MODE_PROCEDURE,
            dialect=dialect,
            verb="EXEC",
            identifier=ident,
            sql=sql,
            params=binds,
            stream_name=ident.split(".")[-1],
        )
    raise ProcedureSourceError(
        "Could not parse a stored-procedure extract. Use "
        "`CALL schema.name(:p)`, `EXEC schema.name`, or `SELECT * FROM schema.name(:p)` "
        "(PostgreSQL set-returning functions)."
    )


@dataclass
class _Arg:
    kind: str  # bind | literal | placeholder
    name: str = ""
    value: Any = None


def _parse_args(raw: str, params: Mapping[str, Any]) -> list[_Arg]:
    parts = _split_args(raw)
    out: list[_Arg] = []
    for i, part in enumerate(parts):
        token = part.strip()
        if not token:
            continue
        if token == "?":
            key = f"p{i}"
            if key not in params and str(i) not in params:
                raise ProcedureSourceError(f"Placeholder ? at position {i + 1} has no bound value.")
            out.append(_Arg("bind", key, params.get(key, params.get(str(i)))))
            continue
        if _BIND_NAME.match(token) or _BIND_AT.match(token):
            key = token.lstrip(":@")
            if key not in params:
                raise ProcedureSourceError(f"Parameter :{key} is not bound.")
            out.append(_Arg("bind", key, params[key]))
            continue
        lit = _literal(token)
        out.append(_Arg("literal", f"p{i}", lit))
    return out


def _parse_exec_tail(raw: str, params: Mapping[str, Any]) -> list[_Arg]:
    text = (raw or "").strip()
    if not text:
        return []
    # EXEC name @since = :since, @limit = 10
    if "=" in text:
        args: list[_Arg] = []
        for part in _split_args(text):
            if "=" not in part:
                raise ProcedureSourceError(f"Could not parse EXEC argument `{part}`.")
            left, right = part.split("=", 1)
            name = left.strip().lstrip("@")
            if not re.match(rf"^{_IDENT}$", name):
                raise ProcedureSourceError(f"Invalid EXEC parameter name `{left}`.")
            token = right.strip()
            if _BIND_NAME.match(token) or _BIND_AT.match(token):
                key = token.lstrip(":@")
                if key not in params:
                    raise ProcedureSourceError(f"Parameter :{key} is not bound.")
                args.append(_Arg("bind", name, params[key]))
            else:
                args.append(_Arg("literal", name, _literal(token)))
        return args
    return _parse_args(text, params)


def _split_args(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    in_str = False
    for ch in raw or "":
        if ch == "'":
            in_str = not in_str
            buf.append(ch)
            continue
        if ch == "," and not in_str:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf))
    if in_str:
        raise ProcedureSourceError("Unclosed string literal in procedure arguments.")
    return parts


def _literal(token: str) -> Any:
    upper = token.upper()
    if upper == "NULL":
        return None
    if upper in {"TRUE", "FALSE"}:
        return upper == "TRUE"
    if _NUMBER.match(token):
        if any(c in token for c in ".eE"):
            return float(token)
        return int(token)
    if _STRING.match(token):
        return token[1:-1].replace("''", "'")
    raise ProcedureSourceError(
        f"Argument `{token}` is not a literal or bound parameter. "
        "Use :name and procedure_params, or a quoted/numeric literal."
    )


def _render_call(verb: str, ident: str, args: list[_Arg], dialect: str) -> tuple[str, dict[str, Any]]:
    binds: dict[str, Any] = {}
    pieces: list[str] = []
    for i, arg in enumerate(args):
        key = arg.name or f"p{i}"
        binds[key] = arg.value
        pieces.append(f":{key}")
    inner = ", ".join(pieces)
    return f"{verb} {ident}({inner})", binds


def _render_select_func(ident: str, args: list[_Arg]) -> tuple[str, dict[str, Any]]:
    binds: dict[str, Any] = {}
    pieces: list[str] = []
    for i, arg in enumerate(args):
        key = arg.name or f"p{i}"
        binds[key] = arg.value
        pieces.append(f":{key}")
    return f"SELECT * FROM {ident}({', '.join(pieces)})", binds


def _render_exec(ident: str, args: list[_Arg]) -> tuple[str, dict[str, Any]]:
    binds: dict[str, Any] = {}
    if not args:
        return f"EXEC {ident}", binds
    pieces: list[str] = []
    for i, arg in enumerate(args):
        key = arg.name or f"p{i}"
        binds[key] = arg.value
        pieces.append(f"@{key} = :{key}")
    return f"EXEC {ident} {', '.join(pieces)}", binds


# ---------------------------------------------------------------------------
# Execute + spool
# ---------------------------------------------------------------------------


def _spool_key(cfg: Mapping[str, Any], spec: CallableSpec) -> str:
    payload = {
        "job_id": job_id_of(cfg),
        "host": cfg.get("host"),
        "port": cfg.get("port"),
        "database": cfg.get("database"),
        "sql": spec.sql,
        "params": spec.params,
        "connector_id": cfg.get("connector_id"),
    }
    blob = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _spool_dir(job_id: str) -> Path:
    root = Path(tempfile.gettempdir()) / "dataflow-callable"
    if job_id:
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", job_id)[:80] or "job"
        root = root / safe
    root.mkdir(parents=True, exist_ok=True)
    return root


def close_callable_spool(key: str | None = None, *, job_id: str | None = None) -> None:
    """Drop CALL spools for one key, one job, or every process-local file.

    Job-scoped files live under ``{tmp}/dataflow-callable/{job_id}/``. A
    restart still cannot page a missing file — that is fail-closed, not
    exactly-once.
    """
    jid = str(job_id or "").strip()
    with _SPOOL_LOCK:
        if key:
            keys = [key]
        elif jid:
            keys = [k for k, spool in _SPOOLS.items() if spool.job_id == jid]
        else:
            keys = list(_SPOOLS)
        for item in keys:
            if not item:
                continue
            spool = _SPOOLS.pop(item, None)
            if spool is None:
                continue
            with contextlib.suppress(OSError):
                spool.path.unlink(missing_ok=True)
            parent = spool.path.parent
            if parent.name and parent.name != "dataflow-callable":
                with contextlib.suppress(OSError):
                    parent.rmdir()


def _get_spool(key: str) -> _ResultSpool | None:
    with _SPOOL_LOCK:
        return _SPOOLS.get(key)


def _fill_spool(cfg: Mapping[str, Any], spec: CallableSpec, key: str) -> _ResultSpool:
    jid = job_id_of(cfg)
    fd, name = tempfile.mkstemp(
        prefix="df-proc-", suffix=".jsonl", dir=str(_spool_dir(jid))
    )
    os.close(fd)
    path = Path(name)
    headers, total, schema = _execute_to_jsonl(cfg, spec, path)
    spool = _ResultSpool(
        path=path, headers=headers, total=total, schema=schema, job_id=jid
    )
    with _SPOOL_LOCK:
        _SPOOLS[key] = spool
    return spool


def _cursor_header_index(headers: list[str], cursor_column: str) -> int:
    want = cursor_column.strip().lower()
    for i, header in enumerate(headers):
        if str(header).lower() == want:
            return i
    raise ProcedureSourceError(
        f"Cursor column `{cursor_column}` is not in the procedure result — "
        "incremental on a CALL/SELECT cannot invent a watermark field."
    )


def _cursor_cell(row: list[Any], idx: int) -> str | None:
    if idx < 0 or idx >= len(row):
        return None
    val = row[idx]
    if val is None:
        return None
    text = str(val)
    return text if text != "" else None


def _row_after_cursor(row: list[Any], idx: int, cursor_after: Any) -> bool:
    from services.sync_cursor import compare_cursor_values

    return compare_cursor_values(_cursor_cell(row, idx), cursor_after) > 0


def _row_from_spool_line(obj: Any, headers: list[str]) -> list[str]:
    if isinstance(obj, dict):
        return ["" if obj.get(h) is None else str(obj.get(h)) for h in headers]
    return ["" if v is None else str(v) for v in obj]


def _read_spool_page(
    spool: _ResultSpool,
    *,
    offset: int,
    limit: int,
    cursor_column: str | None = None,
    cursor_after: Any = None,
) -> tuple[list[str], list[list[str]], int]:
    """Page the spool. Cursor filter runs before OFFSET so incremental cannot duplicate.

    ``cursor_after`` must be the **run watermark**. The spool is not sorted;
    paging by an advancing page-max would drop unsorted rows (silent loss).
    """
    cursor_col = str(cursor_column or "").strip()
    cursor_idx: int | None = None
    apply_filter = False
    if cursor_col:
        cursor_idx = _cursor_header_index(spool.headers, cursor_col)
        apply_filter = cursor_after not in (None, "")
    rows: list[list[str]] = []
    start = max(0, int(offset))
    end = start + max(0, int(limit))
    matched = 0
    with spool.path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if not apply_filter:
                if i < start:
                    continue
                if i >= end:
                    break
                obj = json.loads(line)
                rows.append(_row_from_spool_line(obj, spool.headers))
                continue
            obj = json.loads(line)
            row = _row_from_spool_line(obj, spool.headers)
            if cursor_idx is None or not _row_after_cursor(row, cursor_idx, cursor_after):
                continue
            if start <= matched < end:
                rows.append(row)
            matched += 1
    if apply_filter:
        return list(spool.headers), rows, matched
    if cursor_col:
        return list(spool.headers), rows, spool.total
    return list(spool.headers), rows, spool.total


def _project(
    headers: list[str],
    rows: list[list[Any]],
    columns: list[str],
) -> tuple[list[str], list[list[Any]]]:
    idx = [headers.index(c) for c in columns if c in headers]
    keep = [headers[i] for i in idx]
    projected = [[row[i] if i < len(row) else "" for i in idx] for row in rows]
    return keep, projected


def _open_callable_result(cfg: Mapping[str, Any], spec: CallableSpec, *, peek: bool):
    from connectors.generic_sql import SQLALCHEMY_AVAILABLE, _engine
    from services.engine_pool import release_engine

    if not SQLALCHEMY_AVAILABLE:
        raise ProcedureSourceError("SQLAlchemy is not installed — cannot execute a procedure extract.")

    import sqlalchemy as sa

    engine = _engine(dict(cfg))
    timeout = PEEK_TIMEOUT_S if peek else int(
        os.environ.get("DATAFLOW_PROCEDURE_TIMEOUT_S") or DEFAULT_TIMEOUT_S
    )
    conn = engine.connect()
    try:
        _apply_timeout(conn, spec.dialect or str(cfg.get("type") or ""), timeout)
        result = conn.execute(sa.text(spec.sql), spec.params)
        if result.returns_rows is False:
            raise ProcedureSourceError(
                f"`{spec.identifier}` did not return a result set — "
                "the extract needs a SELECT-shaped output to map and write."
            )
        headers = list(result.keys())
        if not headers:
            raise ProcedureSourceError(
                f"`{spec.identifier}` returned no columns — cannot map an empty result."
            )
        return engine, conn, result, headers
    except Exception:
        conn.close()
        release_engine(engine)
        raise


def _execute_live(
    cfg: Mapping[str, Any],
    spec: CallableSpec,
    *,
    limit: int | None,
) -> tuple[list[str], list[list[str]], dict[str, str]]:
    from services.engine_pool import release_engine

    engine = None
    conn = None
    try:
        engine, conn, result, headers = _open_callable_result(cfg, spec, peek=limit is not None)
        cap = int(limit) if limit is not None else None
        fetched = result.fetchmany(cap) if cap is not None else result.fetchall()
        rows = [[_cell(v) for v in row] for row in fetched]
    except ProcedureSourceError:
        raise
    except Exception as exc:
        raise ProcedureSourceError(f"Procedure extract failed: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
        if engine is not None:
            release_engine(engine)

    schema, _intel = peek_callable_schema(headers, rows)
    return headers, rows, schema


def _execute_to_jsonl(
    cfg: Mapping[str, Any],
    spec: CallableSpec,
    path: Path,
) -> tuple[list[str], int, dict[str, str]]:
    """Drain CALL once into JSONL — never hold the full result as a Python list."""
    from services.engine_pool import release_engine

    engine = None
    conn = None
    sample: list[list[str]] = []
    total = 0
    try:
        engine, conn, result, headers = _open_callable_result(cfg, spec, peek=False)
        with path.open("w", encoding="utf-8") as fh:
            while True:
                chunk = result.fetchmany(2_000)
                if not chunk:
                    break
                for row in chunk:
                    cells = [_cell(v) for v in row]
                    if len(sample) < PEEK_ROW_LIMIT:
                        sample.append(cells)
                    fh.write(json.dumps(cells, default=str))
                    fh.write("\n")
                    total += 1
    except ProcedureSourceError:
        raise
    except Exception as exc:
        raise ProcedureSourceError(f"Procedure extract failed: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
        if engine is not None:
            release_engine(engine)

    schema, _intel = peek_callable_schema(headers, sample)
    return headers, total, schema


def _apply_timeout(conn: Any, dialect: str, timeout_s: int) -> None:
    if timeout_s <= 0:
        return
    ms = int(timeout_s * 1000)
    try:
        if dialect in {"postgresql", "postgres", "pgvector", "redshift", "greenplum"}:
            conn.execute(__import__("sqlalchemy").text(f"SET LOCAL statement_timeout = {ms}"))
        elif dialect in {"mysql", "mariadb"}:
            conn.execute(__import__("sqlalchemy").text(f"SET SESSION max_execution_time = {ms}"))
    except Exception:
        logger.debug("procedure timeout pragma skipped for %s", dialect, exc_info=True)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)
