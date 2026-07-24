"""Integration: older CDC LSN must not overwrite a newer MySQL row (native writer)."""

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
from connectors.writer_common import DF_LSN_COL  # noqa: E402


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_mysql_upsert_rejects_older_lsn():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL not reachable on localhost:3306")

    table_name = "mysql_cdc_lsn_" + uuid.uuid4().hex[:8]
    common = {
        "host": "localhost",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "",
        "connection_string": "",
        "ssl": False,
        "table_name": table_name,
        "headers": ["id", "amount", DF_LSN_COL],
        "mappings": [
            _mapping("id", "id"),
            _mapping("amount", "amount"),
            _mapping(DF_LSN_COL, DF_LSN_COL),
        ],
        "column_types": {"id": "INTEGER", "amount": "TEXT", DF_LSN_COL: "TEXT"},
    }

    r1 = write_mapped_rows(
        **common,
        data_rows=[["1", "new", "0/16B3748"]],
        create_table=True,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r1.ok, r1.error

    r2 = write_mapped_rows(
        **common,
        data_rows=[["1", "stale", "0/16B3700"]],
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
    )
    assert r2.ok, r2.error

    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT amount, `{DF_LSN_COL}` FROM `{table_name}` WHERE id = 1")
            row = cur.fetchone()
            cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.commit()
    finally:
        conn.close()

    assert row is not None
    assert str(row[0]) == "new"
    assert str(row[1]) == "0/16B3748"
