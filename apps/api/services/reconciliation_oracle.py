"""Oracle-specific Gate-8 read-back.

Oracle is the one supported destination where the *identity* of the object the
writer filled is not knowable from the name the operator typed: unquoted DDL
folds to upper case while quoted DDL is stored verbatim, so ``users`` and
``"users"`` are two tables. Reconciliation that folds the name reads the wrong
object — a written table then reports "sample unavailable" or, worse, a clean
count from a table this job never touched. Every read here resolves the stored
spelling through :mod:`services.sql_object_identity` first.

Split out of :mod:`services.reconciliation` so the dialect's rules (LOB
comparison limits, ``FETCH FIRST``, catalog identity) live in one place.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from services.reconciliation import (
    canonical_checksum_from_iter,
    keyed_readback_sa_clause,
    keyed_readback_scope,
    sa_streaming_result,
)

logger = logging.getLogger(__name__)

#: Destination type strings routed to this module.
ORACLE_DB_TYPES: Final = frozenset(
    {
        "oracle",
        "oracledb",
        "oracle_db",
        "oracle_autonomous",
        "oracle_autonomous_warehouse",
        "amazon_rds_oracle",
    }
)

#: Oracle column types that cannot appear in ORDER BY / IN comparisons.
_ORACLE_LOB_TYPES: Final = {"CLOB", "NCLOB", "BLOB", "LONG", "LONG RAW", "XMLTYPE"}


def is_oracle_destination(db_type: str, dest: dict[str, Any]) -> bool:
    """True when this destination is Oracle, including generic SQL URLs."""
    if db_type in ORACLE_DB_TYPES:
        return True
    return db_type == "generic_sql" and str(
        dest.get("connection_string") or ""
    ).lower().startswith("oracle")


def oracle_catalog_identity(
    conn: Any,
    table_name: str,
    schema: str | None,
    columns: list[str] | None = None,
) -> tuple[str | None, str | None, dict[str, str]]:
    """Resolve the stored spelling of an Oracle table, owner and columns.

    Guessing the fold made Gate-8 read back ``"USERS"`` from a table the writer
    had just filled as ``"users"``: rows landed, then the job failed as "sample
    unavailable". Both sides now ask the catalog what the object is actually
    called instead of folding.

    Returns ``(owner, table, {requested_column: stored_column})``. Anything the
    catalog does not know is returned unresolved — never invented.
    """
    from services.sql_object_identity import resolve_object_identity

    ident = resolve_object_identity(conn, table_name, schema, columns=columns)
    if not ident.exists:
        return None, None, {}
    return ident.schema, ident.table, dict(ident.columns)


def oracle_comparable_expr(
    conn: Any, table_name: str, schema: str | None, column: str
) -> str:
    """SQL expression for ``column`` usable as an Oracle comparison key.

    LOB columns raise ORA-22848 in ORDER BY / IN, so they are compared on their
    leading 4000 characters. Keys that differ only beyond that prefix would
    collide; Oracle offers no wider comparison and refusing the read outright
    would leave a written table unproven.
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    quoted = quote_sql_identifier(require_safe_identifier(column, preserve_case=True))
    try:
        # Case-insensitive match: a quoted lower-case table/column created by an
        # older run is the same physical object as the folded spelling.
        row = conn.execute(
            sa.text(
                "SELECT data_type FROM all_tab_columns "
                "WHERE UPPER(table_name) = :t AND UPPER(column_name) = :c "
                "AND (:s IS NULL OR UPPER(owner) = :s)"
            ),
            {
                "t": str(table_name or "").upper(),
                "c": str(column or "").upper(),
                "s": str(schema).upper() if schema else None,
            },
        ).first()
    except Exception:
        return quoted
    data_type = str((row or [""])[0] or "").upper()
    if data_type in _ORACLE_LOB_TYPES:
        return f"DBMS_LOB.SUBSTR({quoted}, 4000, 1)"
    return quoted


def verify_oracle_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent Oracle read-back for Gate-8 (write-location ''≡NULL fingerprints).

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "oracle",
            "host": host or "",
            "port": int(port or 1521),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or username or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
        with engine.connect() as conn:
            # Verify the object the writer filled — see oracle_catalog_identity.
            owner, stored_table, pk_map = oracle_catalog_identity(
                conn, table_name, sch, [pk_column] if pk_column else None
            )
            if stored_table:
                table_ref = quote_table_ref(
                    stored_table,
                    schema=owner or sch,
                    dialect="oracle",
                    preserve_case=True,
                )
            if pk_column and pk_map.get(pk_column):
                pk_column = pk_map[pk_column]
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="oracle")
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                (tuple(row) for row in result),
                columns,
                limit=limit,
                dest_db_type="oracle",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Oracle reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def read_oracle_target_sample(
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[Any],
    sort_key: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Ordered destination sample from the Oracle object the writer filled."""
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_column_list, quote_table_ref, require_safe_identifier

    lim = max(1, int(limit or 50))
    ora_col_sql = (
        "*"
        if cols == ["*"]
        else quote_column_list(
            [require_safe_identifier(c, preserve_case=True) for c in cols]
        )
    )
    sch = (schema or dest.get("schema") or dest.get("username") or "").strip() or None
    table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
    engine = get_sqlalchemy_engine(
        {
            "type": "oracle",
            "host": dest.get("host", ""),
            "port": int(dest.get("port") or 1521),
            "database": dest.get("database", ""),
            "username": dest.get("username", ""),
            "password": dest.get("password", ""),
            "connection_string": dest.get("connection_string", ""),
            "schema": schema or dest.get("schema") or "",
        }
    )
    with engine.connect() as conn:
        # Read back the object the writer actually filled: ask the catalog for
        # its stored spelling instead of folding the name.
        owner, stored_table, col_map = oracle_catalog_identity(
            conn,
            table_name,
            sch,
            None if cols == ["*"] else list(cols),
        )
        read_table = stored_table or table_name
        read_owner = owner or sch
        if stored_table:
            table_ref = quote_table_ref(
                read_table, schema=read_owner, dialect="oracle", preserve_case=True
            )
        if col_map and cols != ["*"]:
            ora_col_sql = quote_column_list(
                [
                    require_safe_identifier(col_map.get(c, c), preserve_case=True)
                    for c in cols
                ]
            )
        sort_key_stored = col_map.get(sort_key, sort_key) if sort_key else ""
        # Oracle rejects a LOB wherever it needs a comparison key (ORA-22848),
        # so ordering or ``IN``-filtering on a CLOB — what an unbounded source
        # text column becomes here — made a written table read back as "sample
        # unavailable". Compare on the leading 4000 characters instead, and
        # order by ROWID when the caller supplied no key at all.
        key_expr = (
            oracle_comparable_expr(conn, read_table, read_owner, sort_key_stored)
            if sort_key
            else ""
        )
        ora_order = key_expr or "ROWID"
        if keys and sort_key:
            params: dict[str, Any] = {f"k{i}": k for i, k in enumerate(keys)}
            params["lim"] = lim
            placeholders = ",".join(f":k{i}" for i in range(len(keys)))
            sql = (
                f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                f"WHERE {key_expr} IN ({placeholders}) "
                f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
            )
        else:
            params = {"lim": lim}
            sql = (
                f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
            )
        result = conn.execute(sa.text(sql), params)
        names = list(cols) if cols and cols != ["*"] else list(result.keys())
        rows = result.fetchall()
        return [dict(zip(names, tuple(row))) for row in rows]
