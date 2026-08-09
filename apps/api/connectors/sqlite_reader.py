"""SQLite reader — batch reads from a local SQLite file."""

from __future__ import annotations

import sqlite3
from typing import Any

from connectors.base import ReadBatch
from connectors.sqlite_common import sqlite_file_path
from connectors.writer_common import quote_sql_identifier


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
            rows=[tuple(row) for row in rows],
            total_rows=total,
        )
    finally:
        if close_conn:
            shared.close()
