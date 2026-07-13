"""MySQL/MariaDB → MySQL schema drift: backfill_new_fields adds a new column."""

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

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_mysql_to_mysql_backfill_new_fields():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL/MariaDB not reachable on localhost:3306")

    src_table = f"mysql_src_backfill_{uuid.uuid4().hex[:8]}"
    dst_table = f"mysql_dst_backfill_{uuid.uuid4().hex[:8]}"

    import pymysql
    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
            cur.execute(f"CREATE TABLE `{src_table}` (id INT PRIMARY KEY, amount DECIMAL(18,2))")
            cur.execute(f"CREATE TABLE `{dst_table}` (id INT PRIMARY KEY, amount DECIMAL(18,2))")
            cur.execute(f"INSERT INTO `{src_table}` (id, amount) VALUES (1, 1000.00), (2, 2000.50)")
            cur.execute(f"INSERT INTO `{dst_table}` (id, amount) VALUES (1, 1000.00)")
            conn.commit()
    finally:
        conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql", host="localhost", port=3306,
            database="dataflow", username="dataflow", password="dataflow",
            schema="dataflow", table=src_table,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql", host="localhost", port=3306,
            database="dataflow", username="dataflow", password="dataflow",
            schema="dataflow", table=dst_table,
        ),
        sync_mode="upsert",
        stream_contracts=[{
            "name": "payments",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        backfill_new_fields=True,
        skip_preflight=True,
    )
    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{src_table}` ADD COLUMN currency VARCHAR(10)")
            cur.execute(f"UPDATE `{src_table}` SET currency = %s WHERE id = %s", ("USD", 1))
            cur.execute(f"UPDATE `{src_table}` SET currency = %s WHERE id = %s", ("EUR", 2))
            conn.commit()
    finally:
        conn.close()

    result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 2

    conn = pymysql.connect(
        host="localhost", port=3306, database="dataflow",
        user="dataflow", password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, amount, currency FROM `{dst_table}` ORDER BY id")
            rows = cur.fetchall()
            assert list(rows) == [(1, Decimal("1000.00"), "USD"), (2, Decimal("2000.50"), "EUR")]
    finally:
        conn.close()
