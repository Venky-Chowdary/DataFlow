"""Real (non-mocked) MySQL binlog CDC integration test.

Runs against a live MySQL with ``binlog_format=ROW`` and a user holding
``REPLICATION SLAVE``/``REPLICATION CLIENT`` (see docker-compose ``mysql``
service and CI ``mysql`` service). It proves the actual binlog reader — not a
mock — captures inserts, updates, and deletes from a captured resume position.

Skips cleanly when MySQL / ROW binlog is not reachable so laptops without a
local MySQL stay green while CI and docker-compose environments exercise it.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_change_stream import MySqlChangeStreamCdc  # noqa: E402
from connectors.mysql_conn import get_connection  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _connect():
    return get_connection(
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _mysql_binlog_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        return False
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row) and str(row[1]).upper() == "ROW"
        finally:
            conn.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _mysql_binlog_ready(),
    reason="MySQL with ROW binlog + REPLICATION grants not reachable on localhost:3306",
)


def _exec(sql: str) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def test_mysql_binlog_snapshot_then_poll_captures_real_cdc():
    table = "cdc_orders_" + uuid.uuid4().hex[:8]
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount DECIMAL(10,2))")
    _exec(f"INSERT INTO {table} (id, amount) VALUES (1, 10.00), (2, 20.00)")

    try:
        cdc = MySqlChangeStreamCdc(CFG, table=table, primary_key="id", max_wait_seconds=8.0)

        # Real binlog capability probe (opens a BinLogStreamReader under the hood).
        assert cdc.is_available() is True

        # Snapshot the existing rows and capture the binlog handoff position.
        batches = list(cdc.snapshot())
        snap_inserts = [r for b in batches for r in b.inserts]
        assert len(snap_inserts) == 2, snap_inserts
        resume = batches[-1].resume_token
        assert resume and resume.get("file") and resume.get("pos") is not None, resume

        # Apply real DML *after* the captured position.
        _exec(f"INSERT INTO {table} (id, amount) VALUES (3, 30.00)")
        _exec(f"UPDATE {table} SET amount = 99.00 WHERE id = 1")
        _exec(f"DELETE FROM {table} WHERE id = 2")

        # Poll the binlog from the captured resume token — this is the real reader.
        cdc_resume = MySqlChangeStreamCdc(
            CFG, table=table, primary_key="id", resume_token=resume, max_wait_seconds=8.0
        )
        changes = list(cdc_resume.poll())

        inserts = [r for b in changes for r in b.inserts]
        updates = [r for b in changes for r in b.updates]
        deletes = [d for b in changes for d in b.deletes]

        assert any(str(r.get("id")) == "3" for r in inserts), f"insert not captured: {inserts}"
        assert any(
            str(r.get("id")) == "1" and str(r.get("amount")).startswith("99") for r in updates
        ), f"update not captured: {updates}"
        assert "2" in deletes, f"delete not captured: {deletes}"
    finally:
        _exec(f"DROP TABLE IF EXISTS {table}")
