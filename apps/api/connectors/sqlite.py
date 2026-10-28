"""SQLite connector — file-based local database probe."""

from __future__ import annotations

import sqlite3

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
    require_object: bool = True,
) -> ConnectResult:
    """Probe a SQLite database file. ``database`` is the path to the .db file.

    A missing file whose directory exists is created: that is how a new
    destination connector is initialized. A missing *source* is refused by
    ``require_sqlite_source_file`` on the read path, which does not create the
    file (R3-H3). ``require_object`` is accepted so destination probes can say
    the object is about to be created; a directory that is not on this server
    still fails closed.
    """
    del port, username, password, schema, ssl, require_object
    from pathlib import Path

    from connectors.sqlite_common import sqlite_file_path

    try:
        path = sqlite_file_path(database, connection_string, host)
    except ValueError as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc))
    if not path:
        return ConnectResult(ok=False, tables=[], error="SQLite path is required (database or connection_string).")

    if path != ":memory:" and not Path(path).is_file():
        parent = Path(path).expanduser().parent
        if not parent.is_dir():
            return ConnectResult(
                ok=False,
                tables=[],
                error=(
                    f"SQLite file {path} is not on this server. "
                    "The directory does not exist on this instance. "
                    "Put the database on storage this server can write."
                ),
            )

    try:
        conn = sqlite3.connect(path, timeout=8)
        with conn:
            cur = conn.cursor()
            # Views are listed beside tables, each with its kind (M18).
            cur.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 50"
            )
            fetched = cur.fetchall()
            tables = [row[0] for row in fetched]
            table_types = {row[0]: row[1] for row in fetched}
        conn.close()
        return ConnectResult(
            ok=True,
            tables=tables or ["(no tables)"],
            message=f"SQLite connected — {len(tables)} tables",
            driver="sqlite3",
            table_types=table_types,
        )
    except Exception as exc:
        return ConnectResult(ok=False, tables=[], error=str(exc), driver="sqlite3")
