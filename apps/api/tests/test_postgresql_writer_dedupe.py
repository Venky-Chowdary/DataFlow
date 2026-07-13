"""PostgreSQL writer deduplicates duplicate upsert keys within a single batch."""

from __future__ import annotations

import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_writer import write_mapped_rows  # noqa: E402


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_postgresql_writer_upsert_dedupes_within_batch():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL not reachable on localhost:5432")

    table_name = "pg_writer_dedup_" + uuid.uuid4().hex[:8]
    common = {
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
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
        write_mode="insert",
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

    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, amount FROM public."{table_name}" ORDER BY id')
        rows = cur.fetchall()
    conn.close()
    assert rows == [(1, Decimal("2222.00")), (2, Decimal("2000.50")), (3, Decimal("3000.00"))]
