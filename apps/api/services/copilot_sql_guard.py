"""Phase D6 — allow-list SQL identifiers against introspected schema for Pilot.

``_is_safe_sql`` blocks writes; this module blocks *invented* column/table names
so LLM-influenced SELECT cannot probe arbitrary identifiers.
"""

from __future__ import annotations

import re
from typing import Any

_IDENT = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")
# Engine catalogs are not columns of the table being read. Refusing them
# blocked ``SELECT * FROM sqlite_master`` (QA R2-090) and the information_schema
# reads every query console legitimately runs (D17/M23).
_CATALOG_RELATIONS = frozenset({
    "sqlite_master",
    "sqlite_schema",
    "sqlite_temp_master",
    "sqlite_temp_schema",
    "information_schema",
    "pg_catalog",
    "mysql",
    "sys",
    "performance_schema",
})
#: Read-only catalog objects reachable inside a catalog schema — a SELECT may
#: reference them without their names existing in the introspected schema.
_CATALOG_OBJECTS = frozenset({
    # information_schema.*
    "columns", "tables", "schemata", "views", "statistics",
    "key_column_usage", "table_constraints", "referential_constraints",
    "routines", "parameters", "triggers", "events", "check_constraints",
    "constraint_column_usage", "sequences",
    # pg_catalog.*
    "pg_class", "pg_namespace", "pg_attribute", "pg_type", "pg_index",
    "pg_constraint", "pg_am", "pg_proc", "pg_tables", "pg_views",
    "pg_indexes", "pg_matviews", "pg_enum", "pg_range", "pg_attrdef",
    "pg_stat_user_tables", "pg_sequences", "pg_roles", "pg_settings",
    # sqlite PRAGMA-table functions
    "pragma_table_info", "pragma_table_xinfo", "pragma_index_list",
    "pragma_index_info", "pragma_foreign_key_list",
    # duckdb / generic
    "duckdb_columns", "duckdb_tables", "duckdb_schemas",
    "current_schemas", "current_schema", "current_database",
    "session_user", "current_user",
    # catalog column vocabulary — column_name/table_name inside an
    # information_schema/pg_catalog read are catalog fields, not user columns
    "column_name", "table_name", "table_schema", "table_catalog",
    "ordinal_position", "data_type", "is_nullable", "column_default",
    "character_maximum_length", "numeric_precision", "numeric_scale",
    "udt_name", "constraint_name", "constraint_type", "routine_name",
    "specific_name", "trigger_name", "event_object_table",
    "relname", "attname", "nspname", "typname", "indexdef",
    "schemaname", "tablename", "viewname", "indexname", "seqrelid",
    "attnum", "attnotnull", "atttypid", "conname", "conrelid",
    "indisunique", "indisprimary", "relkind", "relnamespace",
    "oid", "name", "type", "sql", "tbl_name", "rootpage",
})
#: Catalog objects that hold credentials, other sessions' SQL text or raw
#: large-object bytes. Pilot never needs them, so they are refused even when
#: a same-named relation sits in the introspected allow-list.
#: PostgreSQL resolves ``pg_catalog`` implicitly, so these match unqualified.
_SECRET_CATALOG_OBJECTS = frozenset({
    "pg_authid", "pg_shadow", "pg_auth_members", "pg_user_mapping",
    "pg_user_mappings", "pg_largeobject", "pg_stat_activity",
    "pg_stat_statements", "pg_hba_file_rules", "pg_ident_file_mappings",
    "pg_file_settings", "pg_subscription",
})
#: Secret-bearing objects whose bare names are common user table names
#: (``user``, ``db``), refused only when qualified with their catalog schema.
_SECRET_QUALIFIED_CATALOG_OBJECTS: dict[str, frozenset[str]] = {
    "mysql": frozenset({
        "user", "global_priv", "db", "tables_priv", "columns_priv",
        "procs_priv", "proxies_priv", "password_history", "role_edges",
        "default_roles", "servers", "slave_master_info",
        "replication_connection_configuration", "general_log", "slow_log",
    }),
    "sys": frozenset({
        "sql_logins", "syslogins", "credentials", "database_credentials",
        "database_scoped_credentials", "linked_logins", "server_principals",
        "dm_exec_sql_text", "dm_exec_requests", "dm_exec_sessions",
        "dm_exec_connections", "processlist", "x$processlist", "session",
        "x$session",
    }),
    "performance_schema": frozenset({
        "threads", "processlist", "events_statements_current",
        "events_statements_history", "events_statements_history_long",
        "events_statements_summary_by_digest", "replication_connection_configuration",
    }),
    "information_schema": frozenset({"processlist", "user_attributes"}),
}
#: SQL Server catalog views Pilot needs to introspect (``sys.objects``,
#: ``sys.databases``). Their bare names are too ordinary to join the global
#: column vocabulary above, so they pass only when ``sys``-qualified. Every
#: other unlisted catalog relation still has to be allow-listed by name.
_SAFE_QUALIFIED_CATALOG_OBJECTS: dict[str, frozenset[str]] = {
    "sys": frozenset({
        "objects", "all_objects", "schemas", "indexes", "index_columns",
        "databases", "types", "all_columns", "all_views", "foreign_keys",
        "foreign_key_columns", "key_constraints", "identity_columns",
        "computed_columns", "default_constraints", "check_constraints",
    }),
}
_QUALIFIED_NAME = re.compile(
    r"[A-Za-z_][\w$]*(?:\s*\.\s*[A-Za-z_][\w$]*)*"
)


def _is_secret_catalog_object(schema: str, name: str) -> bool:
    if name in _SECRET_CATALOG_OBJECTS:
        return True
    return name in _SECRET_QUALIFIED_CATALOG_OBJECTS.get(schema, frozenset())


def secret_catalog_references(sql: str) -> set[str]:
    """Secret-bearing catalog relations named anywhere in ``sql``.

    Parser-independent: literals and comments are removed and identifier
    quotes dropped, so ``"pg_catalog"."pg_authid"`` and ``mysql . user``
    are caught on both the sqlglot and the regex path.
    """
    from services.sql_literals import sql_call_views

    found: set[str] = set()
    # Lexed once per engine flavour (comments blanked, literals masked, quoted
    # identifiers kept by name): a quote inside a comment cannot hide a name.
    for text in sql_call_views(sql or ""):
        for match in _QUALIFIED_NAME.finditer(text):
            parts = [p.strip().lower() for p in match.group(0).split(".")]
            for i, name in enumerate(parts):
                schema = parts[i - 1] if i else ""
                if _is_secret_catalog_object(schema, name):
                    found.add(f"{schema}.{name}" if schema else name)
    return found


#: Date/time field names and unit literals that are arguments to functions
#: (extract/datediff/date_part), never schema identifiers (D17).
_SQL_NON_IDENT_TOKENS = frozenset({
    "epoch", "epoch_microsecond", "epoch_millisecond", "microsecond",
    "millisecond", "second", "minute", "hour", "day", "dow", "isodow", "doy",
    "week", "month", "quarter", "year", "decade", "century", "millennium",
    "timezone", "zone", "utc", "localtime", "localtimestamp",
    "true", "false", "null", "default", "collate",
})
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


def _sqlglot_identifiers(sql: str, dialect: str = "") -> set[str] | None:
    """Real referenced names via sqlglot — or ``None`` when it cannot parse.

    Tables yield their last part; columns yield their unqualified name; CTE
    names and aliases are query-defined, not schema identifiers (D17/M23).
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return None
    # E'...' / N'...' / B'...' / X'...' prefixed literals are literals, not
    # identifiers — sqlglot reads the bare prefix letter as a column (D17).
    sql = re.sub(r"\b[ENBX]'(?:''|[^'])*'", " ", sql or "")
    dialect_arg = dialect or None
    try:
        tree = sqlglot.parse_one(sql, read=dialect_arg)
    except Exception:
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            return None
    if tree is None:
        return None
    query_defined = {
        str(cte.alias).lower()
        for cte in tree.find_all(exp.CTE)
        if getattr(cte, "alias", None)
    }
    query_defined.update(
        str(a.alias).lower()
        for a in tree.find_all(exp.Alias)
        if getattr(a, "alias", None)
    )
    query_defined.update(
        str(t.alias).lower()
        for t in tree.find_all(exp.Table)
        if getattr(t, "alias", None)
    )
    cte_names = {
        str(cte.alias).lower()
        for cte in tree.find_all(exp.CTE)
        if getattr(cte, "alias", None)
    }
    found: set[str] = set()
    for table in tree.find_all(exp.Table):
        name = str(table.name or "").strip().lower()
        schema = str(table.db or "").strip().lower()
        catalog = str(table.catalog or "").strip().lower()
        if schema in _CATALOG_RELATIONS or catalog in _CATALOG_RELATIONS:
            # information_schema.columns / pg_catalog.pg_class are read-only
            # catalog views the allowlist owns. Any other catalog-qualified
            # relation (pg_catalog.pg_authid, mysql.user) or table function
            # (pg_catalog.pg_read_file(...)) must be allow-listed by its
            # qualified name — the bare name may be an unrelated user table.
            if not name and isinstance(table.this, exp.Func):
                name = str(table.this.name or "").strip().lower()
            owner = schema or catalog
            if _is_secret_catalog_object(owner, name):
                found.add(f"{owner}.{name}")
                continue
            if name in _CATALOG_OBJECTS or name in _SAFE_QUALIFIED_CATALOG_OBJECTS.get(
                owner, frozenset()
            ):
                continue
            found.add(f"{schema or catalog}.{name or '?'}")
            continue
        # Only a CTE can stand in for a relation; a table alias equal to its
        # own name (``FROM t AS t``) must not hide the relation from the check.
        if name and name not in cte_names:
            found.add(name)
    for col in tree.find_all(exp.Column):
        name = str(col.name or "").strip().lower()
        if not name or name == "*":
            continue
        if name in query_defined:
            continue
        if col.table and str(col.table).lower() in _CATALOG_RELATIONS:
            continue
        found.add(name)
    return found


def extract_sql_identifiers(sql: str, dialect: str = "") -> set[str]:
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
    # Bare (non-AS) aliases are equally not schema identifiers:
    #   FROM orders o / JOIN order_items oi   — token after a relation name
    #   FROM (...) q / count(*) n             — token after a closing paren
    #   WITH cte AS (...)                     — the CTE head name is query-defined
    # Without these exclusions, standard SQL aliases are refused as invented
    # identifiers (QA R23).
    aliases.update(
        m.group(1).lower()
        for m in re.finditer(
            r"\b(?:from|join)\s+[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?\s+([A-Za-z_][\w]*)",
            scrubbed,
            flags=re.I,
        )
    )
    aliases.update(
        m.group(1).lower()
        for m in re.finditer(r"\)\s*(?:as\s+)?([A-Za-z_][\w]*)", scrubbed, flags=re.I)
    )
    aliases.update(
        m.group(1).lower()
        for m in re.finditer(r"\b([A-Za-z_][\w]*)\s+as\s*\(", scrubbed, flags=re.I)
    )
    # ``t.col`` — the qualifier is a query alias, not a catalog column
    # (QA R2-091: ORDER BY / UNION rejected the alias ``t``).
    aliases.update(
        m.group(1).lower()
        for m in re.finditer(
            r"\b([A-Za-z_][\w]*)\s*\.\s*[A-Za-z_]",
            scrubbed,
        )
    )
    # Bare select-list alias with no AS: ``SELECT id t FROM orders``.
    # One identifier only — ``SELECT invented FROM t`` must still be checked.
    aliases.update(
        m.group(1).lower()
        for m in re.finditer(
            r"\bselect\s+[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)?\s+([A-Za-z_][\w]*)\s+from\b",
            scrubbed,
            flags=re.I,
        )
    )
    # date_trunc( and round( are calls. ::numeric is a cast. Neither is a column
    # the catalog would list. A bare `SELECT round FROM t` is still checked.
    function_calls = {
        m.group(1).lower()
        for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", scrubbed)
    }
    cast_types = {
        m.group(1).lower()
        for m in re.finditer(r"::\s*([A-Za-z_][A-Za-z0-9_]*)", scrubbed)
    }
    found: set[str] = set()
    for match in _IDENT.finditer(scrubbed):
        tok = match.group(1)
        low = tok.lower()
        if low in _SQL_KEYWORDS or low in _SQL_NON_IDENT_TOKENS or low in aliases or low in function_calls or low in cast_types:
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
    dialect: str = "",
) -> None:
    """Raise ValueError when SQL references identifiers outside ``allowed``.

    When ``allowed`` is empty (schema unavailable), refuse rather than fail open.
    """
    if not allowed:
        raise ValueError(
            "Schema allow-list unavailable — refuse Pilot SQL until introspection succeeds."
        )
    parsed = _sqlglot_identifiers(sql, dialect)
    if parsed is None:
        # The parser could not read the SQL — fall back to the regex pass
        # rather than fail open or refuse valid syntax outright.
        parsed = extract_sql_identifiers(sql, dialect)
    unknown = sorted(
        (
            parsed - allowed - _CATALOG_RELATIONS - _CATALOG_OBJECTS
            - _SQL_NON_IDENT_TOKENS
        )
        | secret_catalog_references(sql)
    )
    if unknown:
        preview = ", ".join(unknown[:12])
        more = f" (+{len(unknown) - 12} more)" if len(unknown) > 12 else ""
        raise ValueError(
            f"SQL references identifiers not in introspected schema: {preview}{more}"
        )
