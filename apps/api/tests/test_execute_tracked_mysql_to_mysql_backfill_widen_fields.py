"""MySQL/MariaDB → MySQL schema drift: backfill widens an existing column."""

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


def test_mysql_to_mysql_backfill_widens_varchar_and_decimal():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL/MariaDB not reachable on localhost:3306")

    src_table = f"mysql_src_widen_{uuid.uuid4().hex[:8]}"
    dst_table = f"mysql_dst_widen_{uuid.uuid4().hex[:8]}"

    import pymysql

    conn = pymysql.connect(
        host="localhost",
        port=3306,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
            cur.execute(
                f"CREATE TABLE `{src_table}` "
                f"(id INT PRIMARY KEY, note VARCHAR(5), amount DECIMAL(8,2))"
            )
            cur.execute(
                f"CREATE TABLE `{dst_table}` "
                f"(id INT PRIMARY KEY, note VARCHAR(5), amount DECIMAL(8,2))"
            )
            cur.execute(
                f"INSERT INTO `{src_table}` (id, note, amount) VALUES (%s, %s, %s), (%s, %s, %s)",
                (1, "hello", "1234.56", 2, "world", "7890.12"),
            )
            cur.execute(
                f"INSERT INTO `{dst_table}` (id, note, amount) VALUES (%s, %s, %s)",
                (1, "hi", "10.00"),
            )
            conn.commit()
    finally:
        conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="mysql",
            host="localhost",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="dataflow",
            table=src_table,
        ),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            host="localhost",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="dataflow",
            table=dst_table,
        ),
        sync_mode="upsert",
        stream_contracts=[
            {
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }
        ],
        backfill_new_fields=True,
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    conn = pymysql.connect(
        host="localhost",
        port=3306,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{src_table}` MODIFY COLUMN note VARCHAR(50)")
            cur.execute(f"ALTER TABLE `{src_table}` MODIFY COLUMN amount DECIMAL(12,2)")
            cur.execute(
                f"UPDATE `{src_table}` SET note = %s, amount = %s WHERE id = %s",
                ("a-much-longer-value-that-exceeds-five", "1234567890.12", 1),
            )
            conn.commit()
    finally:
        conn.close()

    result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 2

    conn = pymysql.connect(
        host="localhost",
        port=3306,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, note, amount FROM `{dst_table}` ORDER BY id")
            rows = cur.fetchall()
            assert list(rows) == [
                (1, "a-much-longer-value-that-exceeds-five", Decimal("1234567890.12")),
                (2, "world", Decimal("7890.12")),
            ]
    finally:
        conn.close()
