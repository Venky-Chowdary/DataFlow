"""SQLite reader — batch reads from a local SQLite file."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from connectors.writer_common import quote_sql_identifier


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[tuple]
    total_rows: int


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
) -> ReadBatch:
    """Read a batch of rows from a SQLite table."""
    del port, username, password, schema, ssl
    path = connection_string or database or host
    if not path:
        raise ValueError("SQLite path is required (database or connection_string).")
    if not table:
        raise ValueError("SQLite source table name required.")

    table_quoted = quote_sql_identifier(table)
    conn = sqlite3.connect(path, timeout=8)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_quoted}")
        total = cur.fetchone()[0]

        cur.execute(
            f"SELECT * FROM {table_quoted} LIMIT ? OFFSET ?",
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
        conn.close()
