"""Dialect-aware SQL identifier sanitization and quoting.

Canonical helpers for SELECT/FROM clauses. Values must still use bind parameters;
only identifiers go through these functions.
"""

from __future__ import annotations

import re

_IDENT_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_identifier(name: str, preserve_case: bool = False, *, max_len: int = 63) -> str:
    """Make an identifier legal without making two distinct names one.

    Only characters that are actually illegal are replaced. Runs of underscores
    and trailing underscores are legal in every dialect we write to, and
    normalizing them away was not cosmetic: it merged distinct columns.
    ``first__name`` and ``first_name`` both became ``first_name``, so one source
    column silently overwrote the other in the destination, and ``value_`` and
    ``value`` collided the same way.

    It also renamed data that needed no renaming. Every Salesforce custom field
    ends in ``__c``, so ``ExternalKey__c`` landed as ``ExternalKey_c`` — the
    values transferred, but under a column name the source never had, which is
    enough for a checksum to disagree with a transfer that in fact moved every
    row correctly.
    """
    cleaned = (name or "").strip() if preserve_case else (name or "").strip().lower()
    s = _IDENT_RE.sub("_", cleaned)
    if not s or s[0].isdigit():
        s = f"col_{s or 'field'}"
    return s[:max_len]


def snowflake_fold_identifier(name: str) -> str:
    """Fold Snowflake identifiers the way unquoted SQL does (UPPER).

    Prefer ``services.dialect_profiles.fold_identifier("snowflake", name)`` for
    new code — this wrapper remains for existing Snowflake call sites.
    """
    try:
        from services.dialect_profiles import fold_identifier

        return fold_identifier("snowflake", name)
    except Exception:
        raw = (name or "").strip()
        if not raw:
            return raw
        if raw != raw.upper() and raw != raw.lower():
            return raw
        return raw.upper()


def require_safe_identifier(
    name: str,
    *,
    preserve_case: bool = False,
    max_len: int = 63,
    allow_raw: bool = False,
) -> str:
    """Sanitize and reject empty identifiers.

    When ``allow_raw`` is True, only strip/length-check (for warehouses that
    preserve mixed-case quoted names after sanitize would destroy meaning).
    Prefer sanitize for untrusted input.
    """
    raw = (name or "").strip()
    if not raw:
        raise ValueError("SQL identifier is empty")
    if allow_raw:
        if len(raw) > max_len:
            raise ValueError(f"SQL identifier exceeds {max_len} characters")
        if any(c in raw for c in ("\x00", "\n", "\r", ";")):
            raise ValueError("SQL identifier contains forbidden characters")
        return raw
    s = sanitize_identifier(raw, preserve_case=preserve_case, max_len=max_len)
    if not s:
        raise ValueError("SQL identifier is empty after sanitization")
    return s


def quote_sql_identifier(name: str, quote_char: str = '"') -> str:
    """Quote a SQL identifier and escape embedded quote characters.

    T-SQL brackets are asymmetric: passing ``"["`` used to emit ``[col[``,
    which is not valid SQL anywhere. Only the closing bracket needs doubling.
    ``quote_table_ref`` already handled this for table names; column quoting
    did not, so the two disagreed for SQL Server.
    """
    if quote_char == "[":
        escaped = str(name).replace("]", "]]")
        return f"[{escaped}]"
    escaped = str(name).replace(quote_char, quote_char + quote_char)
    return f"{quote_char}{escaped}{quote_char}"


def quote_column_list(columns: list[str] | None, *, quote_char: str = '"') -> str:
    if not columns:
        return "*"
    return ", ".join(quote_sql_identifier(c, quote_char) for c in columns)


def _unquote_sql_ident(part: str) -> str:
    """Strip one layer of SQL identifier quotes, including doubled escapes."""
    raw = (part or "").strip()
    if len(raw) >= 2:
        if raw[0] == '"' and raw[-1] == '"':
            return raw[1:-1].replace('""', '"')
        if raw[0] == "`" and raw[-1] == "`":
            return raw[1:-1].replace("``", "`")
        if raw[0] == "[" and raw[-1] == "]":
            return raw[1:-1].replace("]]", "]")
    return raw


def _ident_parts(name: str) -> list[str]:
    """Split ``schema.table`` on dots that are not inside quotes."""
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i = 0
    n = len(name)
    while i < n:
        ch = name[i]
        if quote is None:
            if ch in {'"', "`"}:
                quote = ch
                buf.append(ch)
            elif ch == "[":
                quote = "]"
                buf.append(ch)
            elif ch == ".":
                part = "".join(buf).strip()
                if part:
                    parts.append(part)
                buf = []
            else:
                buf.append(ch)
            i += 1
            continue
        buf.append(ch)
        closer = quote
        if ch == closer:
            nxt = name[i + 1] if i + 1 < n else ""
            if closer == "]" and nxt == "]":
                buf.append(nxt)
                i += 2
                continue
            if closer in {'"', "`"} and nxt == closer:
                buf.append(nxt)
                i += 2
                continue
            quote = None
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def split_qualified_table(
    table: str,
    schema: str | None = None,
) -> tuple[str | None, str]:
    """Return ``(schema, table)`` without double-prefixing a qualified name.

    Studio and schedules often store ``public.case_a_src`` while the connector
    already has ``schema=public``. Callers that then do
    ``sa.table(table, schema=schema)`` or ``quote_table_ref(table, schema)``
    addressed ``public."public.case_a_src"`` — a relation that does not exist —
    so uniqueness and collision probes failed closed on a table the operator
    had already chosen.

    A quoted identifier that contains a dot (``"foo.bar"``) is one name and is
    not split. Unquoted ``schema.table`` (and ``catalog.schema.table``) yields
    the last segment as the table and the preceding segment as the schema.
    The table's own qualifier wins over ``schema`` when both are set.
    """
    fallback = (schema or "").strip() or None
    raw = (table or "").strip()
    if not raw:
        return fallback, raw
    parts = _ident_parts(raw)
    if len(parts) <= 1:
        return fallback, _unquote_sql_ident(parts[0] if parts else raw)
    tbl = _unquote_sql_ident(parts[-1])
    sch = _unquote_sql_ident(parts[-2])
    return sch or fallback, tbl


def quote_table_ref(
    table: str,
    schema: str | None = None,
    *,
    dialect: str = "ansi",
    project: str | None = None,
    dataset: str | None = None,
    sanitize: bool = True,
    preserve_case: bool = False,
) -> str:
    """Build a dialect-aware quoted table reference for FROM clauses.

    ``dialect``: ansi | postgresql | snowflake | sqlite | duckdb | mysql | bigquery
    """
    schema, table = split_qualified_table(table, schema)
    dialect = (dialect or "ansi").lower()
    try:
        from services.dialect_profiles import normalize_driver

        dialect = normalize_driver(dialect) or dialect
    except Exception:
        pass
    if dialect in ("mysql", "mariadb", "clickhouse", "databricks"):
        q = "`"
        tbl = require_safe_identifier(table, preserve_case=True) if sanitize else require_safe_identifier(
            table, allow_raw=True, max_len=64
        )
        if schema:
            sch = require_safe_identifier(schema, preserve_case=True) if sanitize else require_safe_identifier(
                schema, allow_raw=True, max_len=64
            )
            return f"{quote_sql_identifier(sch, q)}.{quote_sql_identifier(tbl, q)}"
        return quote_sql_identifier(tbl, q)

    if dialect in ("sqlserver", "mssql"):
        # SQL Server: [schema].[table]
        def _bracket(ident: str) -> str:
            safe = require_safe_identifier(ident, preserve_case=True) if sanitize else require_safe_identifier(
                ident, allow_raw=True, max_len=128
            )
            return f"[{safe.replace(']', ']]')}]"

        if schema:
            return f"{_bracket(schema)}.{_bracket(table)}"
        return _bracket(table)

    if dialect in ("bigquery", "bq"):
        # BigQuery: `project.dataset.table` — sanitize each segment.
        parts: list[str] = []
        for part in (project, dataset or schema, table):
            if not part:
                continue
            if sanitize:
                parts.append(require_safe_identifier(part, preserve_case=True, max_len=1024))
            else:
                parts.append(require_safe_identifier(part, allow_raw=True, max_len=1024))
        if not parts:
            raise ValueError("BigQuery table reference is empty")
        joined = ".".join(parts)
        return f"`{joined}`"

    # ANSI / Postgres / Snowflake / SQLite / DuckDB / Oracle
    q = '"'
    preserve = preserve_case or dialect in ("snowflake", "postgresql", "postgres", "redshift", "oracle")
    if sanitize:
        tbl = require_safe_identifier(table, preserve_case=preserve)
        sch = require_safe_identifier(schema, preserve_case=preserve) if schema else None
    else:
        tbl = require_safe_identifier(table, allow_raw=True)
        sch = require_safe_identifier(schema, allow_raw=True) if schema else None
    # Dialect fold (Snowflake UPPER, etc.) — never leak Postgres lowercase into
    # warehouses. ``preserve_case`` means the caller resolved the stored spelling
    # from the catalog: folding it again would address a different object.
    if not preserve_case and dialect in (
        "snowflake",
        "oracle",
        "postgresql",
        "postgres",
        "redshift",
    ):
        try:
            from services.dialect_profiles import fold_identifier

            tbl = fold_identifier(dialect, tbl)
            if sch:
                sch = fold_identifier(dialect, sch)
        except Exception:
            if dialect == "snowflake":
                tbl = snowflake_fold_identifier(tbl)
                if sch:
                    sch = snowflake_fold_identifier(sch)
    if sch:
        return f"{quote_sql_identifier(sch, q)}.{quote_sql_identifier(tbl, q)}"
    return quote_sql_identifier(tbl, q)
