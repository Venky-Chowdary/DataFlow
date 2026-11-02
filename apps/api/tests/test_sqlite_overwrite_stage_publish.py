"""SQLite full-refresh overwrite publishes its stage on the last chunk only.

Chunks are 1-based. The stage was published when ``idx + 1 >= total`` — on
chunk 1 of 2 — so the previous table was already replaced by the first chunk
and the tail wrote straight onto the live table. A failure in the tail then
left a prefix where the old table had been (found with D34's multi-file
overwrite, but true of any multi-chunk overwrite).
"""

from __future__ import annotations

import sqlite3

from connectors.sqlite_writer import write_mapped_rows

MAPPINGS = [{"source": "id", "target": "id"}, {"source": "v", "target": "v"}]


def _write(path, rows, *, idx, total):
    return write_mapped_rows(
        host="",
        port=0,
        database=str(path),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="t",
        headers=["id", "v"],
        data_rows=rows,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "INTEGER"},
        write_mode="insert",
        atomic_overwrite=True,
        file_batch_idx=idx,
        total_chunks=total,
    )


def _live(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute("SELECT id, v FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


def test_first_of_two_chunks_leaves_the_previous_table_live(tmp_path):
    path = tmp_path / "ow.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER, v INTEGER)")
    conn.execute("INSERT INTO t VALUES (100, 1), (101, 1)")
    conn.commit()
    conn.close()

    first = _write(path, [["1", "10"], ["2", "20"]], idx=1, total=2)
    assert first.ok, first.error
    assert _live(path) == [(100, 1), (101, 1)]

    last = _write(path, [["3", "30"]], idx=2, total=2)
    assert last.ok, last.error
    assert _live(path) == [(1, 10), (2, 20), (3, 30)]


def test_single_write_still_publishes(tmp_path):
    """Index 0 (buffered adapter) and index 1 with one chunk publish at once."""
    for idx in (0, 1):
        path = tmp_path / f"one_{idx}.sqlite"
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (id INTEGER, v INTEGER)")
        conn.execute("INSERT INTO t VALUES (100, 1)")
        conn.commit()
        conn.close()
        result = _write(path, [["1", "10"]], idx=idx, total=1)
        assert result.ok, result.error
        assert _live(path) == [(1, 10)]
