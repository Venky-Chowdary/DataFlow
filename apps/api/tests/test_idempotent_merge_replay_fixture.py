"""Named fixture: sqlite PK + _df_lsn upsert does not double-count on replay.

At-least-once CDC. Stale LSN is skipped. Dest COUNT stays 1.
Not leftover MERGE. Not platform exactly-once.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.lsn_guards import DF_LSN_COL
from connectors.sqlite_writer import write_mapped_rows


MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "v", "target": "v", "confidence": 1.0},
    {"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1.0},
]
TYPES = {"id": "string", "v": "string", DF_LSN_COL: "string"}


def _write(path: str, rows: list[list[str]], *, create_table: bool):
    return write_mapped_rows(
        host="",
        port=0,
        database=path,
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["id", "v", DF_LSN_COL],
        data_rows=rows,
        mappings=MAPPINGS,
        column_types=TYPES,
        create_table=create_table,
        write_mode="upsert",
        conflict_columns=["id"],
    )


def test_sqlite_pk_df_lsn_replay_no_double_count(tmp_path: Path):
    path = str(tmp_path / "replay.db")
    first = _write(path, [["1", "a", "0/10"]], create_table=True)
    assert first.ok, first.error
    newer = _write(path, [["1", "b", "0/20"]], create_table=False)
    assert newer.ok, newer.error
    stale = _write(path, [["1", "stale", "0/10"]], create_table=False)
    assert stale.ok, stale.error

    conn = sqlite3.connect(path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        v, lsn = conn.execute(
            f'SELECT v, "{DF_LSN_COL}" FROM orders WHERE id = ?', ("1",)
        ).fetchone()
    finally:
        conn.close()

    assert count == 1
    assert v == "b"
    assert str(lsn) == "0/20"
    assert int(getattr(stale, "rows_skipped", 0) or 0) >= 1 or stale.rows_written == 0
