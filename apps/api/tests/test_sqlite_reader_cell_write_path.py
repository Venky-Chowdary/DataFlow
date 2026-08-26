"""SQLite reader cells use cell_to_string, not native Python types.

INTEGER/REAL left the reader as int/float. BLOB stayed raw bytes.
NULL stayed None instead of the SQL NULL sentinel. PostgreSQL already
uses cell_to_string; SQLite batch and scan must match.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sqlite_reader import _cell, read_table_batch, read_table_scan_batch  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402

BLOB = bytes([0xFF, 0xFE, 0x00])


def _db(tmp_path: Path) -> Path:
    db = tmp_path / "cells.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER, note TEXT, amt REAL, blob BLOB)")
    conn.execute(
        "INSERT INTO t VALUES (?, ?, ?, ?)",
        (1, None, 1.5, BLOB),
    )
    conn.execute("INSERT INTO t VALUES (?, ?, ?, ?)", (2, "", 1.25, None))
    conn.commit()
    conn.close()
    return db


def _cfg(db: Path) -> dict:
    return {
        "host": "",
        "port": 0,
        "database": str(db),
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": "",
        "ssl": False,
        "table": "t",
    }


def test_cell_matches_sql_reader():
    assert _cell(None) == SQL_NULL_SENTINEL
    assert _cell("") == ""
    assert _cell(None) != _cell("")
    assert _cell(1) == "1"
    assert _cell(1.5) == "1.5"
    assert _cell(BLOB) == cell_to_string(BLOB, preserve_sql_null=True)
    assert _cell(True) == "true"


def test_batch_null_blob_and_real_are_wire(tmp_path: Path):
    batch = read_table_batch(**_cfg(_db(tmp_path)), limit=10, offset=0)
    assert batch.headers == ["id", "note", "amt", "blob"]
    assert batch.rows[0][0] == "1"
    assert type(batch.rows[0][0]) is str
    assert batch.rows[0][1] == SQL_NULL_SENTINEL
    assert batch.rows[0][2] == "1.5"
    assert batch.rows[0][3] == cell_to_string(BLOB, preserve_sql_null=True)
    assert batch.rows[1][1] == ""
    assert batch.rows[1][3] == SQL_NULL_SENTINEL


def test_scan_matches_batch_wire(tmp_path: Path):
    cfg = _cfg(_db(tmp_path))
    state: dict = {}
    scan = read_table_scan_batch(**cfg, offset=0, limit=10, scan_state=state)
    batch = read_table_batch(**cfg, limit=10, offset=0)
    assert scan.rows == batch.rows
    assert scan.rows[0][1] == SQL_NULL_SENTINEL
    assert scan.rows[1][1] == ""
