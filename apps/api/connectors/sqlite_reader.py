"""SQLite reader — batch reads from a local SQLite file."""

from __future__ import annotations

import sqlite3
from typing import Any

from connectors.base import ReadBatch
from connectors.sqlite_common import sqlite_file_path
from connectors.writer_common import quote_sql_identifier
from services.value_serializer import cell_to_string


def _cell(value: Any) -> str:
    """One SQLite cell. Same wire as PostgreSQL / Iceberg / procedure extract.

    Native INTEGER / REAL / BLOB / NULL used to leave the reader as Python
    types. ``str(1)`` later invented a second spelling from ``cell_to_string``,
    BLOB stayed raw bytes, and NULL stayed ``None`` instead of the SQL NULL
    sentinel.
    """
    return cell_to_string(value, preserve_sql_null=True)


def _row(row: Any) -> tuple[str, ...]:
    return tuple(_cell(row[i]) for i in range(len(row)))


def read_table_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
    limit: int = 100_000,
    offset: int = 0,
    known_total_rows: int | None = None,
    conn: Any | None = None,
) -> ReadBatch:
    """Read a batch of rows from a SQLite table."""
    del port, username, password, schema, ssl
    from services.source_snapshot import get_source_snapshot_conn

    path = sqlite_file_path(database, connection_string, host)
    if not path:
        raise ValueError("SQLite path is required (database or connection_string).")
    if not table:
        raise ValueError("SQLite source table name required.")

    table_quoted = quote_sql_identifier(table)
    shared = conn if conn is not None else get_source_snapshot_conn()
    close_conn = shared is None
    if shared is None:
        shared = sqlite3.connect(path, timeout=8)
    shared.row_factory = sqlite3.Row
    try:
        cur = shared.cursor()
        if known_total_rows is not None:
            total = known_total_rows
        else:
            cur.execute(f"SELECT COUNT(*) FROM {table_quoted}")  # nosec B608
            total = cur.fetchone()[0]

        # rowid gives a stable total order for ordinary SQLite tables, preventing
        # duplicate/missing rows when paging with LIMIT/OFFSET.
        cur.execute(
            f"SELECT * FROM {table_quoted} ORDER BY rowid LIMIT ? OFFSET ?",  # nosec B608
            (limit, offset),
        )
        rows = cur.fetchall()
        if rows:
            headers = list(rows[0].keys())
        else:
            cur.execute(f"PRAGMA table_info({table_quoted})")
            headers = [row[1] for row in cur.fetchall()]
        return ReadBatch(
            headers=headers,
            rows=[_row(row) for row in rows],
            total_rows=total,
        )
    finally:
        if close_conn:
            shared.close()


def read_table_scan_batch(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 100_000,
    known_total_rows: int | None = None,
    scan_state: dict[str, Any],
    conn: Any | None = None,
) -> ReadBatch:
    """Page one ``SELECT … ORDER BY rowid`` with ``fetchmany`` — no OFFSET."""
    del port, username, password, schema, ssl, columns
    from connectors.sql_snapshot_scan import close_table_scan
    from services.source_snapshot import get_source_snapshot_conn

    path = sqlite_file_path(database, connection_string, host)
    if not path:
        raise ValueError("SQLite path is required (database or connection_string).")
    if not table:
        raise ValueError("SQLite source table name required.")

    table_quoted = quote_sql_identifier(table)
    if not scan_state.get("started"):
        shared = conn if conn is not None else get_source_snapshot_conn()
        close_conn = shared is None and conn is None
        if shared is None:
            shared = sqlite3.connect(path, timeout=8)
        shared.row_factory = sqlite3.Row
        cur = shared.cursor()
        try:
            if known_total_rows is not None:
                total = known_total_rows
            else:
                cur.execute(f"SELECT COUNT(*) FROM {table_quoted}")  # nosec B608
                total = cur.fetchone()[0]
            try:
                cur.execute(f"SELECT * FROM {table_quoted} ORDER BY rowid")  # nosec B608
            except sqlite3.OperationalError:
                # WITHOUT ROWID tables have no rowid — still one SELECT, no OFFSET.
                cur.execute(f"SELECT * FROM {table_quoted}")  # nosec B608
            headers = [d[0] for d in (cur.description or [])]
        except Exception:
            try:
                cur.close()
            except Exception:
                pass
            if close_conn:
                try:
                    shared.close()
                except Exception:
                    pass
            raise
        scan_state.update(
            started=True,
            conn=shared if close_conn else None,
            cur=cur,
            headers=headers,
            total=total,
        )
    cur = scan_state["cur"]
    raw = cur.fetchmany(max(1, int(limit)))
    headers = list(scan_state.get("headers") or [])
    total = scan_state.get("total")
    if not raw:
        if not headers:
            try:
                cur.execute(f"PRAGMA table_info({table_quoted})")
                headers = [row[1] for row in cur.fetchall()]
            except Exception:
                headers = []
        close_table_scan(scan_state)
        return ReadBatch(headers=headers, rows=[], offset=offset, total_rows=total)
    return ReadBatch(
        headers=headers,
        rows=[_row(row) for row in raw],
        offset=offset,
        total_rows=total,
    )
