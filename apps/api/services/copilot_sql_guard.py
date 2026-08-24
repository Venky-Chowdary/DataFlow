"""Phase D6 — allow-list SQL identifiers against introspected schema for Pilot.

``_is_safe_sql`` blocks writes; this module blocks *invented* column/table names
so LLM-influenced SELECT cannot probe arbitrary identifiers.
"""

from __future__ import annotations

import re
from typing import Any

_IDENT = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
_SQL_KEYWORDS = frozenset(
    {
        "select",
        "from",
        "where",
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "as",
        "on",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "full",
        "cross",
        "group",
        "by",
        "order",
        "having",
        "limit",
        "offset",
        "distinct",
        "all",
        "union",
        "except",
        "intersect",
        "case",
        "when",
        "then",
        "else",
        "end",
        "true",
        "false",
        "like",
        "ilike",
        "between",
        "exists",
        "cast",
        "coalesce",
        "nullif",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "asc",
        "desc",
        "with",
        "over",
        "partition",
        "row",
        "rows",
        "unbounded",
        "preceding",
        "following",
        "current",
        "show",
        "describe",
        "desc",
        "explain",
        "table",
        "tables",
        "schema",
        "schemas",
        "database",
        "databases",
        "interval",
        "date",
        "time",
        "timestamp",
        "extract",
        "trim",
        "upper",
        "lower",
        "length",
        "substr",
        "substring",
        "concat",
        "greatest",
        "least",
        "values",
        "lateral",
        "using",
        "natural",
        "fetch",
        "first",
        "next",
        "only",
        "top",
        "into",  # rejected by safe_sql anyway
    }
)


def _normalize_ident(name: str) -> str:
    return str(name or "").strip().strip('"').strip("`").strip("[]").lower()


def extract_sql_identifiers(sql: str) -> set[str]:
    """Best-effort identifier tokens from SQL (excluding string literals).

    Result aliases (``COUNT(*) AS n``) and table aliases (``FROM orders AS o``)
    are not schema identifiers — omit them so Pilot SELECT is not fail-closed
    on invented alias names that never touch the wire catalog.
    """
    # Strip simple quoted strings so literals are not treated as columns.
    scrubbed = re.sub(r"'(?:''|[^'])*'", " ", sql or "")
    scrubbed = re.sub(r'"(?:""|[^"])*"', " ", scrubbed)
    aliases = {
        m.group(1).lower()
        for m in re.finditer(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", scrubbed, flags=re.I)
    }
    found: set[str] = set()
    for match in _IDENT.finditer(scrubbed):
        tok = match.group(1)
        low = tok.lower()
        if low in _SQL_KEYWORDS or low in aliases:
            continue
        if tok.isdigit():
            continue
        found.add(low)
    return found


def schema_allowlist(columns: list[Any] | None, tables: list[Any] | None = None) -> set[str]:
    allowed: set[str] = set()
    for col in columns or []:
        if isinstance(col, dict):
            name = col.get("name") or col.get("column_name") or col.get("field")
        else:
            name = col
        n = _normalize_ident(str(name or ""))
        if n:
            allowed.add(n)
    for table in tables or []:
        if isinstance(table, dict):
            name = table.get("name") or table.get("table") or table.get("id")
        else:
            name = table
        n = _normalize_ident(str(name or ""))
        if n:
            allowed.add(n)
            # common schema.table → allow table part
            if "." in n:
                allowed.add(n.split(".")[-1])
    return allowed


def assert_identifiers_allowed(
    sql: str,
    *,
    allowed: set[str],
) -> None:
    """Raise ValueError when SQL references identifiers outside ``allowed``.

    When ``allowed`` is empty (schema unavailable), refuse rather than fail open.
    """
    if not allowed:
        raise ValueError(
            "Schema allow-list unavailable — refuse Pilot SQL until introspection succeeds."
        )
    unknown = sorted(extract_sql_identifiers(sql) - allowed)
    if unknown:
        preview = ", ".join(unknown[:12])
        more = f" (+{len(unknown) - 12} more)" if len(unknown) > 12 else ""
        raise ValueError(
            f"SQL references identifiers not in introspected schema: {preview}{more}"
        )
