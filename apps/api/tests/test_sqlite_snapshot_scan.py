"""SQLite snapshot extract is one SELECT + fetchmany, not OFFSET pages."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.sqlite_reader import read_table_scan_batch
from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES


def test_sqlite_is_a_snapshot_scan_source():
    assert "sqlite" in SNAPSHOT_SCAN_SOURCES


def test_sqlite_scan_reads_all_pages_without_offset(tmp_path: Path) -> None:
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"r{i}") for i in range(1, 6)])

    state: dict = {}
    pages: list[list] = []
    for offset in (0, 2, 4, 6):
        batch = read_table_scan_batch(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table="t",
            offset=offset,
            limit=2,
            scan_state=state,
        )
        pages.append(list(batch.rows))

    assert pages[0] == [(1, "r1"), (2, "r2")]
    assert pages[1] == [(3, "r3"), (4, "r4")]
    assert pages[2] == [(5, "r5")]
    assert pages[3] == []
    assert not state.get("started")
