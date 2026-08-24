"""Unique and primary key introspection, per engine catalog.

Uniqueness is a physical guarantee, not a naming convention: a destination that
silently loses ``UNIQUE (lower(email))`` will happily accept ``Abc`` next to
``abc``. Each dialect hides part of that truth in a different catalog
(``pg_index`` expressions, ``ALL_IND_EXPRESSIONS``, ``sys.computed_columns``),
so the reads live together here rather than inside the introspection walk.
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier

logger = logging.getLogger(__name__)


def _pg_fetch_unique_keys(cur: Any, schema: str, table: str) -> dict[str, Any]:
    """Return PRIMARY KEY + UNIQUE constraints and unique indexes (incl. expressions).

    ``UNIQUE (lower(email))`` is invisible in ``information_schema`` alone — we
    also read ``pg_index`` / ``pg_get_expr`` so Validate casefolds like the engine.
    """
    from services.type_system import parse_case_insensitive_index_expression

    pk: list[str] = []
    unique_keys: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    try:
        cur.execute(
            """
            SELECT tc.constraint_name,
                   tc.constraint_type,
                   kcu.column_name,
                   kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_schema = kcu.constraint_schema
             AND tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            ORDER BY tc.constraint_type,
                     tc.constraint_name,
                     kcu.ordinal_position
            """,
            (schema, table),
        )
        for name, ctype, col, _ord in cur.fetchall() or []:
            key = str(name)
            bucket = by_name.setdefault(
                key,
                {
                    "name": key,
                    "columns": [],
                    "primary": str(ctype).upper() == "PRIMARY KEY",
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                },
            )
            bucket["columns"].append(str(col))
    except Exception:
        return {"primary_key_columns": [], "unique_keys": []}

    # Unique indexes (including expression / functional indexes).
    try:
        cur.execute(
            """
            SELECT a.attnum, a.attname
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class t ON t.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = %s
              AND t.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            """,
            (schema, table),
        )
        attmap = {int(num): str(name) for num, name in (cur.fetchall() or [])}

        try:
            cur.execute(
                """
                SELECT ic.relname AS index_name,
                       i.indisprimary AS is_primary,
                       COALESCE(pg_get_expr(i.indexprs, i.indrelid), '') AS exprs,
                       COALESCE(pg_get_expr(i.indpred, i.indrelid), '') AS pred,
                       pg_get_indexdef(i.indexrelid) AS indexdef,
                       i.indkey,
                       COALESCE(i.indnullsnotdistinct, false) AS nulls_not_distinct
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s
                  AND t.relname = %s
                  AND i.indisunique
                  AND i.indisvalid
                """,
                (schema, table),
            )
        except Exception:
            # PG < 15 lacks indnullsnotdistinct.
            cur.execute(
                """
                SELECT ic.relname AS index_name,
                       i.indisprimary AS is_primary,
                       COALESCE(pg_get_expr(i.indexprs, i.indrelid), '') AS exprs,
                       COALESCE(pg_get_expr(i.indpred, i.indrelid), '') AS pred,
                       pg_get_indexdef(i.indexrelid) AS indexdef,
                       i.indkey,
                       false AS nulls_not_distinct
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s
                  AND t.relname = %s
                  AND i.indisunique
                  AND i.indisvalid
                """,
                (schema, table),
            )
        for (
            idx_name,
            is_primary,
            exprs,
            pred,
            indexdef,
            indkey,
            nulls_not_distinct,
        ) in cur.fetchall() or []:
            key = str(idx_name)
            bucket = by_name.setdefault(
                key,
                {
                    "name": key,
                    "columns": [],
                    "primary": bool(is_primary),
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                    "filter_predicate": "",
                    "nulls_not_distinct": False,
                },
            )
            bucket["primary"] = bool(is_primary) or bool(bucket.get("primary"))
            bucket["nulls_not_distinct"] = bool(nulls_not_distinct)
            if pred:
                bucket["filter_predicate"] = str(pred).strip()
            expr_text = str(exprs or "").strip() or str(indexdef or "")
            if exprs:
                bucket["expression"] = str(exprs).strip()
            ci_cols = parse_case_insensitive_index_expression(expr_text)
            if ci_cols:
                bucket["case_insensitive"] = True
                for c in ci_cols:
                    if c not in bucket["expression_columns"]:
                        bucket["expression_columns"].append(c)
            # Resolve simple column attnums when constraints did not already list them.
            if not bucket["columns"] and indkey is not None:
                try:
                    attnums = [
                        int(x)
                        for x in str(indkey).split()
                        if str(x).lstrip("-").isdigit() and int(x) > 0
                    ]
                except Exception:
                    attnums = []
                for attnum in attnums:
                    attname = attmap.get(attnum)
                    if attname and attname not in bucket["columns"]:
                        bucket["columns"].append(attname)
    except Exception:
        # Constraints alone are still useful when pg_index probe fails.
        pass

    for bucket in by_name.values():
        if bucket.get("primary"):
            pk = list(bucket.get("columns") or [])
        unique_keys.append(bucket)
    return {"primary_key_columns": pk, "unique_keys": unique_keys}


def _sqlserver_fetch_unique_keys(conn: Any, schema: str, table: str) -> dict[str, Any]:
    """Return PRIMARY KEY + UNIQUE indexes from ``sys.indexes``.

    Also resolves computed-column definitions (``LOWER(email)``) so Validate
    casefolds like the engine when uniqueness is on a computed CI column.
    """
    import sqlalchemy as sa
    from services.type_system import parse_case_insensitive_index_expression

    pk: list[str] = []
    unique_keys: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            sa.text(
                """
                SELECT
                  i.name AS index_name,
                  i.is_primary_key,
                  c.name AS column_name,
                  ic.key_ordinal,
                  CONVERT(nvarchar(4000), cc.definition) AS computed_def,
                  CONVERT(nvarchar(4000), i.filter_definition) AS filter_def
                FROM sys.indexes i
                JOIN sys.index_columns ic
                  ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c
                  ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT JOIN sys.computed_columns cc
                  ON cc.object_id = c.object_id AND cc.column_id = c.column_id
                JOIN sys.tables t ON t.object_id = i.object_id
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = :schema
                  AND t.name = :table
                  AND i.is_unique = 1
                  AND ic.is_included_column = 0
                  AND i.is_hypothetical = 0
                ORDER BY i.name, ic.key_ordinal
                """
            ),
            {"schema": schema, "table": table},
        ).fetchall()
    except Exception:
        return {"primary_key_columns": [], "unique_keys": []}

    grouped: dict[str, dict[str, Any]] = {}
    for idx_name, is_pk, col, _ord, computed_def, filter_def in rows or []:
        if not idx_name:
            continue
        key = str(idx_name)
        bucket = grouped.setdefault(
            key,
            {
                "name": key,
                "columns": [],
                "primary": bool(is_pk),
                "expression": "",
                "expression_columns": [],
                "case_insensitive": False,
                "filter_predicate": "",
            },
        )
        bucket["primary"] = bool(is_pk) or bool(bucket.get("primary"))
        bucket["columns"].append(str(col))
        if filter_def and not bucket.get("filter_predicate"):
            bucket["filter_predicate"] = str(filter_def).strip()
        expr = str(computed_def or "").strip()
        if expr:
            bucket["expression"] = (
                f"{bucket['expression']}; {expr}" if bucket["expression"] else expr
            )
            # SQL Server brackets: LOWER([email]) → LOWER(email)
            normalized = expr.replace("[", "").replace("]", "")
            ci_cols = parse_case_insensitive_index_expression(normalized)
            if ci_cols:
                bucket["case_insensitive"] = True
                for c in ci_cols:
                    if c not in bucket["expression_columns"]:
                        bucket["expression_columns"].append(c)
    for bucket in grouped.values():
        if bucket.get("primary"):
            pk = list(bucket.get("columns") or [])
        unique_keys.append(bucket)
    return {"primary_key_columns": pk, "unique_keys": unique_keys}


def _oracle_unique_constraint_rows(conn: Any, owner: str, table: str) -> list[Any]:
    """PRIMARY/UNIQUE constraint columns for one exact owner/table spelling."""
    import sqlalchemy as sa

    return list(
        conn.execute(
            sa.text(
                """
                SELECT
                  ac.constraint_name,
                  ac.constraint_type,
                  acc.column_name,
                  acc.position
                FROM all_constraints ac
                JOIN all_cons_columns acc
                  ON ac.owner = acc.owner
                 AND ac.constraint_name = acc.constraint_name
                 AND ac.table_name = acc.table_name
                WHERE ac.owner = :owner
                  AND ac.table_name = :table
                  AND ac.constraint_type IN ('P', 'U')
                  AND ac.status = 'ENABLED'
                ORDER BY ac.constraint_name, acc.position
                """
            ),
            {"owner": owner, "table": table},
        ).fetchall()
    )


def _oracle_fetch_unique_keys(conn: Any, owner: str, table: str) -> dict[str, Any]:
    """Return PRIMARY/UNIQUE constraints + function-based unique indexes.

    ``CREATE UNIQUE INDEX … (UPPER(email))`` lives in ``ALL_IND_EXPRESSIONS``,
    not ``ALL_CONSTRAINTS`` — must be read or Validate false-greens Abc/abc.
    """
    import sqlalchemy as sa
    from services.type_system import parse_case_insensitive_index_expression

    pk: list[str] = []
    unique_keys: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    # Oracle keeps quoted identifiers verbatim, so folding the spelling the
    # caller already resolved found no constraints on a lower-case table:
    # Validate then saw "no destination PK" and skipped the duplicate gate.
    # Try the exact spelling first, upper case only as the fallback.
    owner_u = (owner or "").upper()
    table_u = (table or "").upper()
    attempts = [(str(owner or ""), str(table or ""))]
    if (owner_u, table_u) != attempts[0]:
        attempts.append((owner_u, table_u))
    try:
        rows: list[Any] = []
        for owner_try, table_try in attempts:
            rows = _oracle_unique_constraint_rows(conn, owner_try, table_try)
            if rows:
                owner_u, table_u = owner_try, table_try
                break
    except Exception:
        return {"primary_key_columns": [], "unique_keys": []}

    for name, ctype, col, _pos in rows or []:
        key = str(name)
        bucket = by_name.setdefault(
            key,
            {
                "name": key,
                "columns": [],
                "primary": str(ctype).upper() == "P",
                "expression": "",
                "expression_columns": [],
                "case_insensitive": False,
                "filter_predicate": "",
            },
        )
        bucket["columns"].append(str(col))
    # Unique function-based indexes (UPPER/LOWER) — not constraint-backed.
    try:
        fbi_rows = conn.execute(
            sa.text(
                """
                SELECT
                  ai.index_name,
                  TO_CHAR(aie.column_expression) AS column_expression,
                  aie.column_position
                FROM all_indexes ai
                JOIN all_ind_expressions aie
                  ON ai.owner = aie.index_owner
                 AND ai.index_name = aie.index_name
                WHERE ai.table_owner = :owner
                  AND ai.table_name = :table
                  AND ai.uniqueness = 'UNIQUE'
                ORDER BY ai.index_name, aie.column_position
                """
            ),
            {"owner": owner_u, "table": table_u},
        ).fetchall()
        for idx_name, expr, _pos in fbi_rows or []:
            key = str(idx_name)
            bucket = by_name.setdefault(
                key,
                {
                    "name": key,
                    "columns": [],
                    "primary": False,
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                    "filter_predicate": "",
                },
            )
            expr_text = str(expr or "").strip()
            if expr_text:
                bucket["expression"] = (
                    f"{bucket['expression']}; {expr_text}"
                    if bucket["expression"]
                    else expr_text
                )
                # Oracle quotes identifiers: UPPER("EMAIL")
                normalized = expr_text.replace('"', "")
                ci_cols = parse_case_insensitive_index_expression(normalized)
                if ci_cols:
                    bucket["case_insensitive"] = True
                    for c in ci_cols:
                        if c not in bucket["expression_columns"]:
                            bucket["expression_columns"].append(c)
    except Exception:
        pass

    for bucket in by_name.values():
        if bucket.get("primary"):
            pk = list(bucket.get("columns") or [])
        unique_keys.append(bucket)
    return {"primary_key_columns": pk, "unique_keys": unique_keys}


def _snowflake_fetch_unique_keys(cur: Any, schema: str, table: str) -> dict[str, Any]:
    """Return PRIMARY KEY / UNIQUE from Snowflake INFORMATION_SCHEMA.

    Hybrid tables enforce these at write time; standard tables often declare
    ``NOT ENFORCED`` constraints — surface ``enforced`` so Validate does not
    invent blockers for advisory-only keys (Snowflake honesty bar).
    """
    pk: list[str] = []
    unique_keys: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    try:
        try:
            cur.execute(
                """
                SELECT tc.constraint_name,
                       tc.constraint_type,
                       kcu.column_name,
                       kcu.ordinal_position,
                       COALESCE(tc.enforced, 'YES') AS enforced
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_catalog = kcu.constraint_catalog
                 AND tc.constraint_schema = kcu.constraint_schema
                 AND tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE UPPER(tc.table_schema) = UPPER(%s)
                  AND tc.table_name = %s
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position
                """,
                (schema, table),
            )
        except Exception:
            # Older SF builds may lack ENFORCED — treat as YES (fail-closed).
            cur.execute(
                """
                SELECT tc.constraint_name,
                       tc.constraint_type,
                       kcu.column_name,
                       kcu.ordinal_position,
                       'YES' AS enforced
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_catalog = kcu.constraint_catalog
                 AND tc.constraint_schema = kcu.constraint_schema
                 AND tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE UPPER(tc.table_schema) = UPPER(%s)
                  AND tc.table_name = %s
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position
                """,
                (schema, table),
            )
        for name, ctype, col, _ord, enforced in cur.fetchall() or []:
            key = str(name)
            is_primary = str(ctype).upper() == "PRIMARY KEY"
            bucket = by_name.setdefault(
                key,
                {
                    "name": key,
                    "columns": [],
                    "primary": is_primary,
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                    "filter_predicate": "",
                    "enforced": str(enforced or "YES").upper() != "NO",
                },
            )
            bucket["primary"] = is_primary or bool(bucket.get("primary"))
            if str(enforced or "YES").upper() == "NO":
                bucket["enforced"] = False
            if col:
                bucket["columns"].append(str(col))
    except Exception:
        return {"primary_key_columns": [], "unique_keys": []}

    for bucket in by_name.values():
        if bucket.get("primary"):
            pk = list(bucket.get("columns") or [])
        unique_keys.append(bucket)
    return {"primary_key_columns": pk, "unique_keys": unique_keys}


def _mysql_fetch_unique_keys(cur: Any, schema: str, table: str) -> dict[str, Any]:
    """Return PRIMARY / UNIQUE indexes from ``STATISTICS`` (incl. functional exprs)."""
    from services.type_system import parse_case_insensitive_index_expression

    pk: list[str] = []
    unique_keys: list[dict[str, Any]] = []
    try:
        # MySQL 8.0.13+ exposes EXPRESSION for functional indexes; older builds
        # lack the column — fall back to COLUMN_NAME only.
        try:
            cur.execute(
                """
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, EXPRESSION
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND NON_UNIQUE = 0
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (schema, table),
            )
            rows = cur.fetchall() or []
            has_expr = True
        except Exception:
            cur.execute(
                """
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND NON_UNIQUE = 0
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (schema, table),
            )
            rows = [(*r, None) for r in (cur.fetchall() or [])]
            has_expr = False
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            idx_name, col, _seq, _nu = row[0], row[1], row[2], row[3]
            expr = row[4] if has_expr and len(row) > 4 else None
            key = str(idx_name)
            bucket = grouped.setdefault(
                key,
                {
                    "name": key,
                    "columns": [],
                    "primary": key.upper() == "PRIMARY",
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                    "filter_predicate": "",
                },
            )
            if col:
                bucket["columns"].append(str(col))
            expr_text = str(expr or "").strip()
            if expr_text:
                bucket["expression"] = (
                    f"{bucket['expression']}; {expr_text}"
                    if bucket["expression"]
                    else expr_text
                )
                ci_cols = parse_case_insensitive_index_expression(expr_text)
                if ci_cols:
                    bucket["case_insensitive"] = True
                    for c in ci_cols:
                        if c not in bucket["expression_columns"]:
                            bucket["expression_columns"].append(c)
        for bucket in grouped.values():
            if bucket["primary"]:
                pk = list(bucket["columns"])
            unique_keys.append(bucket)
    except Exception:
        return {"primary_key_columns": [], "unique_keys": []}
    return {"primary_key_columns": pk, "unique_keys": unique_keys}


def _sqlite_fetch_unique_keys(
    cur: Any, table_quoted: str, info_rows: list[Any]
) -> dict[str, Any]:
    """Return SQLite PRIMARY KEY + UNIQUE indexes (enforced at write)."""
    pk_ord: list[tuple[int, str]] = []
    for row in info_rows or []:
        # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
        try:
            name = str(row[1])
            pk_flag = int(row[5] or 0)
        except Exception:
            continue
        if pk_flag > 0:
            pk_ord.append((pk_flag, name))
    pk_ord.sort(key=lambda x: x[0])
    pk_cols = [name for _, name in pk_ord]

    unique_keys: list[dict[str, Any]] = []
    if pk_cols:
        unique_keys.append(
            {
                "name": "PRIMARY",
                "columns": list(pk_cols),
                "primary": True,
                "expression": "",
                "expression_columns": [],
                "case_insensitive": False,
                "filter_predicate": "",
                "enforced": True,
            }
        )
    try:
        cur.execute(f"PRAGMA index_list({table_quoted})")
        for idx in cur.fetchall() or []:
            # seq, name, unique, origin, partial
            try:
                idx_name = str(idx[1])
                is_unique = int(idx[2] or 0) == 1
                origin = str(idx[3] or "").lower() if len(idx) > 3 else ""
            except Exception:
                continue
            if not is_unique:
                continue
            # PK already covered via table_info.
            if origin == "pk":
                continue
            cur.execute(f"PRAGMA index_info({quote_sql_identifier(idx_name)})")
            cols: list[str] = []
            for info in cur.fetchall() or []:
                try:
                    col = info[2]
                except Exception:
                    col = None
                if col:
                    cols.append(str(col))
            if not cols:
                continue
            unique_keys.append(
                {
                    "name": idx_name,
                    "columns": cols,
                    "primary": False,
                    "expression": "",
                    "expression_columns": [],
                    "case_insensitive": False,
                    "filter_predicate": "",
                    "enforced": True,
                }
            )
    except Exception as exc:
        logger.debug("sqlite unique index introspect failed: %s", exc, exc_info=exc)
    return {"primary_key_columns": pk_cols, "unique_keys": unique_keys}
