"""Dialect-aware SQL identifier sanitization and quoting.

Canonical helpers for SELECT/FROM clauses. Values must still use bind parameters;
only identifiers go through these functions.
"""

from __future__ import annotations

import re

_IDENT_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_identifier(name: str, preserve_case: bool = False, *, max_len: int = 63) -> str:
    cleaned = (name or "").strip() if preserve_case else (name or "").strip().lower()
    s = _IDENT_RE.sub("_", cleaned)
    s = re.sub(r"_+", "_", s).rstrip("_")
    if not s or s[0].isdigit():
        s = f"col_{s or 'field'}"
    return s[:max_len]


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
    """Quote a SQL identifier and escape embedded quote characters."""
    escaped = str(name).replace(quote_char, quote_char + quote_char)
    return f"{quote_char}{escaped}{quote_char}"


def quote_column_list(columns: list[str] | None, *, quote_char: str = '"') -> str:
    if not columns:
        return "*"
    return ", ".join(quote_sql_identifier(c, quote_char) for c in columns)


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
    dialect = (dialect or "ansi").lower()
    if dialect in ("mysql", "mariadb"):
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

    # ANSI / Postgres / Snowflake / SQLite / DuckDB
    q = '"'
    # Preserve case for Snowflake/PG quoted identifiers when sanitize strips case.
    preserve = preserve_case or dialect in ("snowflake", "postgresql", "postgres", "redshift")
    if sanitize:
        tbl = require_safe_identifier(table, preserve_case=preserve)
        sch = require_safe_identifier(schema, preserve_case=preserve) if schema else None
    else:
        tbl = require_safe_identifier(table, allow_raw=True)
        sch = require_safe_identifier(schema, allow_raw=True) if schema else None
    if sch:
        return f"{quote_sql_identifier(sch, q)}.{quote_sql_identifier(tbl, q)}"
    return quote_sql_identifier(tbl, q)
