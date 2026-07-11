"""SQLite connector — file-based local database probe."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.base import ConnectResult


def test_sqlite(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
) -> ConnectResult:
    """Probe a SQLite database file. `database` is the path to the .db file."""
    del port, username, password, schema, ssl
    path = connection_string or database or host
    if not path:
        return ConnectResult(ok=False, tables=[], error="SQLite path is required (database or connection_string).")
    try:
        Path(path).resolve()
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=f"Invalid SQLite path: {exc}")

    try:
        conn = sqlite3.connect(path, timeout=8)
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 50")
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables)"],
            message=f"SQLite connected — {len(tables)} tables",
            driver="sqlite3",
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="sqlite3")
