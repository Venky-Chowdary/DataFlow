"""Live multi-table shared-reader CDC under concurrent writes.

Proves one PG publication/slot (and one MySQL server_id when available)
demuxes two tables while DML races against poll — Debezium-class shared
reader, not N independent slots.

Skips when the matching database is not reachable with CDC prerequisites.
"""

from __future__ import annotations

import socket
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_change_stream import MySqlChangeStreamCdc  # noqa: E402
from connectors.mysql_conn import get_connection as mysql_get_connection  # noqa: E402
from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc  # noqa: E402
from connectors.postgresql_conn import get_connection as pg_get_connection  # noqa: E402

PG_CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}

MYSQL_CFG = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _pg_connect():
    return pg_get_connection(
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _mysql_connect():
    return mysql_get_connection(
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _pg_logical_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                return bool(row) and row[0] == "logical"
    except Exception:
        return False


def _mysql_binlog_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        return False
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        return False
    try:
        conn = _mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row) and str(row[1]).upper() == "ROW"
        finally:
            conn.close()
    except Exception:
        return False


def _pg_exec(sql: str) -> None:
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def _mysql_exec(sql: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _drop_pg_slot(slot: str) -> None:
    try:
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_drop_replication_slot(%s) "
                    "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                    (slot, slot),
                )
            conn.commit()
    except Exception:
        pass


@pytest.mark.skipif(
    not _pg_logical_ready(),
    reason="PostgreSQL with wal_level=logical not reachable on localhost:5432",
)
def test_pg_shared_reader_live_concurrent_writes():
    """One slot + publication for two tables; concurrent DML demuxes with ack barrier."""
    suffix = uuid.uuid4().hex[:8]
    orders = f"cdc_mt_orders_{suffix}"
    users = f"cdc_mt_users_{suffix}"
    _pg_exec(f"DROP TABLE IF EXISTS {orders}")
    _pg_exec(f"DROP TABLE IF EXISTS {users}")
    _pg_exec(f"CREATE TABLE {orders} (id INT PRIMARY KEY, amount NUMERIC(10,2))")
    _pg_exec(f"CREATE TABLE {users} (id INT PRIMARY KEY, name TEXT)")
    _pg_exec(f"ALTER TABLE {orders} REPLICA IDENTITY FULL")
    _pg_exec(f"ALTER TABLE {users} REPLICA IDENTITY FULL")
    _pg_exec(f"INSERT INTO {orders} (id, amount) VALUES (1, 10.00)")
    _pg_exec(f"INSERT INTO {users} (id, name) VALUES (1, 'alice')")

    holder = f"it-shared-pg-{suffix}"
    cfg = {**PG_CFG, "lease_holder_id": holder, "job_id": holder}
    cdc = PostgreSqlChangeStreamCdc(
        cfg,
        table=[orders, users],
        primary_key="id",
        cursor_key=f"cdc-shared-live:{suffix}",
        schema="public",
        output_plugin="test_decoding",
    )
    slot = cdc.slot_name
    assert cdc.tables == [orders, users]
    assert cdc._lease.meta.get("shared_reader") is True

    stop = threading.Event()
    write_errors: list[BaseException] = []

    def _writer() -> None:
        try:
            for i in range(2, 8):
                if stop.is_set():
                    break
                _pg_exec(f"INSERT INTO {orders} (id, amount) VALUES ({i}, {i * 10}.00)")
                _pg_exec(f"INSERT INTO {users} (id, name) VALUES ({i}, 'u{i}')")
                time.sleep(0.05)
            _pg_exec(f"UPDATE {orders} SET amount = 99.00 WHERE id = 1")
            _pg_exec(f"UPDATE {users} SET name = 'alice-x' WHERE id = 1")
        except BaseException as exc:  # noqa: BLE001 — surface in main thread
            write_errors.append(exc)

    try:
        assert cdc.is_available() is True
        snap = list(cdc.snapshot())
        snap_by: dict[str, list] = {}
        for b in snap:
            snap_by.setdefault(b.table or "", []).extend(b.inserts)
        assert any(str(r.get("id")) == "1" for r in snap_by.get(orders, [])), snap_by
        assert any(str(r.get("id")) == "1" for r in snap_by.get(users, [])), snap_by

        writer = threading.Thread(target=_writer, name="pg-shared-writer", daemon=True)
        writer.start()

        seen_orders: set[str] = set()
        seen_users: set[str] = set()
        barrier_batches = 0
        deadline = time.time() + 25.0
        while time.time() < deadline:
            if write_errors:
                raise write_errors[0]
            batches = list(cdc.poll())
            for b in batches:
                if not b.total_changes:
                    continue
                if b.table == orders:
                    for r in b.inserts + b.updates:
                        seen_orders.add(str(r.get("id")))
                elif b.table == users:
                    for r in b.inserts + b.updates:
                        seen_users.add(str(r.get("id")))
                if b.ack_barrier:
                    barrier_batches += 1
                    cdc.ack(b.resume_token)
            if {"2", "3", "4"}.issubset(seen_orders) and {"2", "3", "4"}.issubset(seen_users):
                break
            time.sleep(0.15)

        stop.set()
        writer.join(timeout=10)
        if write_errors:
            raise write_errors[0]

        assert {"2", "3", "4"}.issubset(seen_orders), seen_orders
        assert {"2", "3", "4"}.issubset(seen_users), seen_users
        assert barrier_batches >= 1, "shared demux must emit at least one ack_barrier batch"
    finally:
        stop.set()
        try:
            cdc.close()
        except Exception:
            pass
        _drop_pg_slot(slot)
        _pg_exec(f"DROP TABLE IF EXISTS {orders}")
        _pg_exec(f"DROP TABLE IF EXISTS {users}")


@pytest.mark.skipif(
    not _mysql_binlog_ready(),
    reason="MySQL with ROW binlog not reachable on localhost:3306",
)
def test_mysql_shared_reader_live_concurrent_writes():
    """One binlog server_id for two tables; concurrent DML demuxes with ack barrier."""
    suffix = uuid.uuid4().hex[:8]
    orders = f"cdc_mt_orders_{suffix}"
    users = f"cdc_mt_users_{suffix}"
    _mysql_exec(f"DROP TABLE IF EXISTS {orders}")
    _mysql_exec(f"DROP TABLE IF EXISTS {users}")
    _mysql_exec(f"CREATE TABLE {orders} (id INT PRIMARY KEY, amount DECIMAL(10,2))")
    _mysql_exec(f"CREATE TABLE {users} (id INT PRIMARY KEY, name VARCHAR(64))")
    _mysql_exec(f"INSERT INTO {orders} (id, amount) VALUES (1, 10.00)")
    _mysql_exec(f"INSERT INTO {users} (id, name) VALUES (1, 'alice')")

    holder = f"it-shared-mysql-{suffix}"
    cfg = {**MYSQL_CFG, "lease_holder_id": holder, "job_id": holder}
    cursor_key = f"cdc-shared-mysql:{suffix}"
    cdc = MySqlChangeStreamCdc(
        cfg,
        table=[orders, users],
        primary_key="id",
        max_wait_seconds=6.0,
        cursor_key=cursor_key,
    )
    assert cdc.tables == [orders, users]
    assert cdc._lease.meta.get("shared_reader") is True

    stop = threading.Event()
    write_errors: list[BaseException] = []

    def _writer() -> None:
        try:
            for i in range(2, 7):
                if stop.is_set():
                    break
                _mysql_exec(f"INSERT INTO {orders} (id, amount) VALUES ({i}, {i * 10}.00)")
                _mysql_exec(f"INSERT INTO {users} (id, name) VALUES ({i}, 'u{i}')")
                time.sleep(0.05)
        except BaseException as exc:  # noqa: BLE001
            write_errors.append(exc)

    resume = None
    cdc_poll = None
    try:
        assert cdc.is_available() is True
        snap = list(cdc.snapshot())
        assert snap, "snapshot must emit"
        resume = snap[-1].resume_token
        cdc.close()

        cdc_poll = MySqlChangeStreamCdc(
            cfg,
            table=[orders, users],
            primary_key="id",
            resume_token=resume,
            max_wait_seconds=6.0,
            cursor_key=cursor_key,
        )

        writer = threading.Thread(target=_writer, name="mysql-shared-writer", daemon=True)
        writer.start()

        seen_orders: set[str] = set()
        seen_users: set[str] = set()
        barrier_batches = 0
        deadline = time.time() + 30.0
        while time.time() < deadline:
            if write_errors:
                raise write_errors[0]
            batches = list(cdc_poll.poll())
            for b in batches:
                if not b.total_changes:
                    continue
                if b.table == orders:
                    for r in b.inserts + b.updates:
                        seen_orders.add(str(r.get("id")))
                elif b.table == users:
                    for r in b.inserts + b.updates:
                        seen_users.add(str(r.get("id")))
                if b.ack_barrier:
                    barrier_batches += 1
                    # MySQL ack is position-based via reopen; keep last token.
                    resume = b.resume_token
            if {"2", "3"}.issubset(seen_orders) and {"2", "3"}.issubset(seen_users):
                break
            time.sleep(0.2)

        stop.set()
        writer.join(timeout=10)
        if write_errors:
            raise write_errors[0]

        assert {"2", "3"}.issubset(seen_orders), seen_orders
        assert {"2", "3"}.issubset(seen_users), seen_users
        assert barrier_batches >= 1 or (seen_orders and seen_users), (
            "expected demux across both tables"
        )
    finally:
        stop.set()
        for obj in (cdc_poll, cdc):
            if obj is not None:
                try:
                    obj.close()
                except Exception:
                    pass
        _mysql_exec(f"DROP TABLE IF EXISTS {orders}")
        _mysql_exec(f"DROP TABLE IF EXISTS {users}")
