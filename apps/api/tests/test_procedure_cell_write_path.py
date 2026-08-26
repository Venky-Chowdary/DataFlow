"""Procedure CALL/SELECT cells use cell_to_string, not str(value).

str(True) invented True. str(Decimal('1E+2')) invented scientific.
str(bytes) invented a Python b'...' repr. str(datetime) used a space
instead of ISO T. None became '' so SQL NULL and empty string collapsed.
PostgreSQL / Iceberg already use cell_to_string; the CALL result must match.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.procedure_source import (  # noqa: E402
    _ResultSpool,
    _cell,
    _read_spool_page,
    close_callable_spool,
    peek_callable_schema,
    read_callable_batch,
)
from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string  # noqa: E402

LONG = "1.234567890123456789"
BLOB = bytes([0xFF, 0xFE, 0x00])
TS = datetime(2024, 1, 2, 3, 4, 5)


def test_cell_matches_sql_reader():
    assert _cell(None) == SQL_NULL_SENTINEL
    assert _cell("") == ""
    assert _cell(None) != _cell("")
    assert _cell(True) == "true"
    assert _cell(True) != str(True)
    assert _cell(Decimal(LONG)) == LONG
    assert _cell(Decimal("1E+2")) == "100"
    assert str(Decimal("1E+2")) == "1E+2"
    assert _cell(TS) == "2024-01-02T03:04:05"
    assert _cell(BLOB) == cell_to_string(BLOB, preserve_sql_null=True)


def test_spool_reread_keeps_json_number_digits(tmp_path: Path):
    path = tmp_path / "call.jsonl"
    path.write_text(f'[{LONG}, 1.5, 1]\n', encoding="utf-8")
    spool = _ResultSpool(path=path, headers=["amt", "n", "id"], total=1, schema={})
    headers, rows, total = _read_spool_page(spool, offset=0, limit=10)
    assert headers == ["amt", "n", "id"]
    assert total == 1
    assert rows[0][0] == LONG
    assert rows[0][0] != str(json.loads(f"[{LONG}]")[0])
    assert rows[0][1] == "1.5"
    assert rows[0][2] == "1"


def test_spool_json_null_is_sql_null_not_empty(tmp_path: Path):
    path = tmp_path / "nulls.jsonl"
    path.write_text('[1, null]\n[2, ""]\n', encoding="utf-8")
    spool = _ResultSpool(path=path, headers=["id", "note"], total=2, schema={})
    _headers, rows, total = _read_spool_page(spool, offset=0, limit=10)
    assert total == 2
    assert rows[0][1] == SQL_NULL_SENTINEL
    assert rows[1][1] == ""
    assert rows[0][1] != rows[1][1]


def test_peek_schema_skips_sql_null_sentinel():
    schema, _intel = peek_callable_schema(
        ["amt", "note"],
        [[LONG, SQL_NULL_SENTINEL], [LONG, ""]],
    )
    assert schema["amt"]
    assert SQL_NULL_SENTINEL not in json.dumps(schema)


def test_sqlite_query_null_stays_distinct_from_empty(tmp_path: Path):
    db = tmp_path / "cells.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER, note TEXT, amt TEXT)")
    conn.execute("INSERT INTO t VALUES (1, NULL, ?)", (LONG,))
    conn.execute("INSERT INTO t VALUES (2, '', '1.50')")
    conn.commit()
    conn.close()
    cfg = {
        "type": "sqlite",
        "database": str(db),
        "source_read_mode": "query",
        "source_query": "SELECT id, note, amt FROM t ORDER BY id",
        "host": "",
        "port": 0,
        "username": "",
        "password": "",
        "schema": "",
        "connection_string": f"sqlite:///{db}",
        "ssl": False,
    }
    try:
        peek = read_callable_batch(cfg, offset=0, limit=10, peek=True)
        assert peek.rows[0][1] == SQL_NULL_SENTINEL
        assert peek.rows[1][1] == ""
        assert peek.rows[0][2] == LONG
        full = read_callable_batch(cfg, offset=0, limit=10, peek=False)
        assert full.rows[0][1] == SQL_NULL_SENTINEL
        assert full.rows[1][1] == ""
        assert full.rows[0][2] == LONG
        assert full.rows[1][2] == "1.50"
    finally:
        close_callable_spool()
