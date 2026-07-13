"""MySQL writer deduplicates duplicate upsert keys within a single batch."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_writer import write_mapped_rows  # noqa: E402


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_mysql_writer_upsert_dedupes_within_batch():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL not reachable on localhost:3306")

    table_name = "mysql_writer_dedup_" + uuid.uuid4().hex[:8]
    common = {
        "host": "localhost",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "dataflow",
        "connection_string": "",
        "ssl": False,
        "table_name": table_name,
        "headers": ["id", "amount"],
        "mappings": [_mapping("id", "id"), _mapping("amount", "amount")],
        "column_types": {"id": "INTEGER", "amount": "DECIMAL"},
    }

    r1 = write_mapped_rows(
        **common,
        data_rows=[["1", "1000.00"], ["2", "2000.50"]],
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r1.ok, r1.error

    r2 = write_mapped_rows(
        **common,
        data_rows=[["1", "1111.00"], ["1", "2222.00"], ["3", "3000.00"]],
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r2.ok, r2.error
    assert r2.rows_written == 2
    assert r2.rejected_rows == 1

    import pymysql
    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, amount FROM `{table_name}` ORDER BY id")
            rows = cur.fetchall()
    finally:
        conn.close()
    assert [(r[0], float(r[1])) for r in rows] == [(1, 2222.0), (2, 2000.5), (3, 3000.0)]
