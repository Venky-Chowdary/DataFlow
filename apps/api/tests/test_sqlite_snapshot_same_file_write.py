"""A SQLite read snapshot must not lock out a write to the same database file.

Property 3 binds one transaction across every full-refresh page. Under the
default rollback journal that reader holds SHARED for the whole read, so a
mirror/SCD2 job writing a table beside its source waited out the busy timeout
and failed with "database is locked". The snapshot now runs in WAL and restores
the operator's journal mode when it ends.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.source_snapshot import (  # noqa: E402
    begin_sqlite_snapshot,
    end_sqlite_snapshot,
)


def _seed(path: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE src (id TEXT, name TEXT)")
        conn.executemany(
            "INSERT INTO src (id, name) VALUES (?, ?)",
            [(str(i), f"Item {i}") for i in range(5)],
        )


def test_write_to_snapshot_file_is_not_locked_out():
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "same.db")
        _seed(path)

        conn, meta = begin_sqlite_snapshot(database=path)
        try:
            assert conn.execute("SELECT COUNT(*) FROM src").fetchone()[0] == 5
            assert meta["journal_mode"] == "wal"

            writer = sqlite3.connect(path, timeout=3)
            try:
                with writer:
                    writer.execute("CREATE TABLE dst (id TEXT, name TEXT)")
                    writer.execute("INSERT INTO dst VALUES ('0', 'Item 0')")
            finally:
                writer.close()

            # The snapshot keeps its own consistent view of the source.
            assert conn.execute("SELECT COUNT(*) FROM src").fetchone()[0] == 5
        finally:
            end_sqlite_snapshot(conn)

        with sqlite3.connect(path) as after:
            assert after.execute("SELECT COUNT(*) FROM dst").fetchone()[0] == 1
            mode = after.execute("PRAGMA journal_mode").fetchone()[0]
        assert str(mode).lower() == "delete"
