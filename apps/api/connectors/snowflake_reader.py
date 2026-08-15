"""Snowflake table reader — batched extraction for warehouse migrations."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.snowflake_conn import (
    get_connection,
    normalize_account,
    resolve_or_fold_snowflake_table,
    snowflake_qualified_table,
)
from connectors.sql_identifiers import (
    quote_column_list,
    quote_sql_identifier,
    require_safe_identifier,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def snapshot_order_sql(columns: list[str], primary_key: str | None = "") -> str:
    """ORDER BY first column plus PK so OFFSET pages cannot skip/duplicate rows."""
    order_cols: list[str] = []
    if columns:
        order_cols.append(str(columns[0]))
    pk = (primary_key or "").strip()
    if pk and pk.lower() not in {c.lower() for c in order_cols}:
        order_cols.append(pk)
    if not order_cols:
        raise RuntimeError("Snowflake table has no columns for stable pagination")
    return quote_column_list(
        [require_safe_identifier(str(c), preserve_case=True) for c in order_cols]
    )


def _use_warehouse(cur, warehouse: str) -> None:
    if not warehouse:
        return
    wh = require_safe_identifier(warehouse, preserve_case=True)
    from connectors.sql_identifiers import snowflake_fold_identifier

    # Warehouse names are usually uppercase; fold all-lower defaults safely.
    wh = snowflake_fold_identifier(wh) if wh == wh.lower() or wh == wh.upper() else wh
    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")


def _snowflake_schema(schema: str | None) -> str:
    from connectors.sql_identifiers import snowflake_fold_identifier

    return snowflake_fold_identifier((schema or "PUBLIC").strip() or "PUBLIC")


def _table_ref(cur: Any, schema: str, table: str) -> str:
    """Resolve + quote a Snowflake table (handles legacy lowercase quoted names)."""
    resolved = resolve_or_fold_snowflake_table(cur, schema, table)
    return snowflake_qualified_table(schema, resolved)


def count_table_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    role: str = "",
    private_key: str = "",
) -> int:
    del port
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
        private_key=private_key,
        private_key_passphrase=password if private_key else "",
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def read_table_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 100_000,
    known_total_rows: int | None = None,
    role: str = "",
    private_key: str = "",
    cursor_primary_key: str | None = None,
) -> ReadBatch:
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
        private_key=private_key,
        private_key_passphrase=password if private_key else "",
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            if known_total_rows is not None:
                total = known_total_rows
            else:
                total = count_table_rows(
                    host=host, port=port, database=database, username=username, password=password,
                    schema=schema, connection_string=connection_string, warehouse=warehouse, table=table,
                    role=role,
                    private_key=private_key,
                )
            col_sql = (
                quote_column_list([require_safe_identifier(c, preserve_case=True) for c in columns])
                if columns
                else "*"
            )
            # Stable LIMIT/OFFSET requires ORDER BY (first column + PK tiebreak).
            order_cols = list(columns or [])
            if not order_cols:
                cur.execute(f"SELECT * FROM {table_ref} LIMIT 0")  # nosec B608
                order_cols = [desc[0] for desc in (cur.description or [])]
            order_sql = snapshot_order_sql(order_cols, primary_key=cursor_primary_key)
            cur.execute(
                f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                f"ORDER BY {order_sql} LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v, preserve_sql_null=True) for v in row] for row in cur.fetchall()]
        return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
    finally:
        conn.close()


def read_table_cursor_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table: str,
    cursor_column: str,
    cursor_after: str | None = None,
    columns: list[str] | None = None,
    limit: int = 500,
    role: str = "",
    private_key: str = "",
    cursor_primary_key: str | None = None,
) -> ReadBatch:
    """Read rows where cursor_column > watermark — incremental sync.

    Optional ``cursor_primary_key`` enables lexicographic ``(cursor, pk)`` so
    rows sharing a timestamp watermark are not skipped forever.
    """
    from services.keyset_pagination import split_cursor_bookmark

    del port
    account = normalize_account(host)
    schema = _snowflake_schema(schema)
    conn = get_connection(
        account=account,
        username=username,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse,
        connection_string=connection_string,
        role=role,
        private_key=private_key,
        private_key_passphrase=password if private_key else "",
    )
    try:
        with conn.cursor() as cur:
            _use_warehouse(cur, warehouse)
            table_ref = _table_ref(cur, schema, table)
            col_sql = (
                quote_column_list([require_safe_identifier(c, preserve_case=True) for c in columns])
                if columns
                else "*"
            )
            cursor_q = quote_sql_identifier(require_safe_identifier(cursor_column, preserve_case=True))
            pk = (cursor_primary_key or "").strip()
            pk_q = (
                quote_sql_identifier(require_safe_identifier(pk, preserve_case=True))
                if pk and pk != cursor_column
                else ""
            )
            if cursor_after:
                if pk_q:
                    cur_val, pk_val = split_cursor_bookmark(
                        cursor_after, has_tiebreak=True
                    )
                    cur.execute(
                        f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE ({cursor_q}, {pk_q}) > (%s, %s) "
                        f"ORDER BY {cursor_q}, {pk_q} LIMIT %s",
                        (cur_val, pk_val, limit),
                    )
                else:
                    bare, _ = split_cursor_bookmark(cursor_after, has_tiebreak=False)
                    cur.execute(
                        f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {cursor_q} > %s ORDER BY {cursor_q} LIMIT %s",
                        (bare, limit),
                    )
            else:
                if pk_q:
                    cur.execute(
                        f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {cursor_q}, {pk_q} LIMIT %s",
                        (limit,),
                    )
                else:
                    cur.execute(
                        f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {cursor_q} LIMIT %s",
                        (limit,),
                    )
            headers = [desc[0] for desc in cur.description]
            rows = [[cell_to_string(v, preserve_sql_null=True) for v in row] for row in cur.fetchall()]
        # Keyset pages are not a cardinality bound — page length must never
        # trip stream early-stop (fetch_offset >= total_rows).
        return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)
    finally:
        conn.close()
