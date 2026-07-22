"""Cross SQL engine end-to-end: PostgreSQL, MySQL, SQLite any-to-any."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _pg_source(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql",
        host="localhost", port=5432, database="dataflow",
        username="dataflow", password="dataflow",
        schema="public", table=table,
    )


def _pg_dest(table: str) -> EndpointConfig:
    return _pg_source(table)


def _mysql_source(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mysql",
        host="localhost", port=3306, database="dataflow",
        username="dataflow", password="dataflow",
        schema="dataflow", table=table,
    )


def _mysql_dest(table: str) -> EndpointConfig:
    return _mysql_source(table)


def _sqlite_source(path: str, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite",
        connection_string=path, database=path, table=table,
    )


def _sqlite_dest(path: str, table: str) -> EndpointConfig:
    return _sqlite_source(path, table)


def _run(request: TransferRequest) -> None:
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")


def _seed_postgresql(table: str) -> None:
    import psycopg2
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, amount NUMERIC(10,2))")
        cur.execute(f"INSERT INTO {table} VALUES (1,'alice',1000.00),(2,'bob',2000.50)")
    conn.close()


def _seed_mysql(table: str) -> None:
    import pymysql
    conn = pymysql.connect(
        host="localhost", port=3306, user="dataflow",
        password="dataflow", database="dataflow",
    )
    conn.autocommit(True)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(100), amount DECIMAL(10,2))")
        cur.execute(f"INSERT INTO {table} VALUES (1,'alice',1000.00),(2,'bob',2000.50)")
    conn.close()


def _seed_sqlite(path: str, table: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT, amount REAL)")
    conn.execute(f"INSERT INTO {table} VALUES (1,'alice',1000.0)")
    conn.execute(f"INSERT INTO {table} VALUES (2,'bob',2000.5)")
    conn.commit()
    conn.close()


def test_postgresql_to_mysql():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1), socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    src_table = "pg_to_mysql_src_" + uuid.uuid4().hex[:8]
    dst_table = "pg_to_mysql_dst_" + uuid.uuid4().hex[:8]
    _seed_postgresql(src_table)
    _seed_mysql(dst_table)  # ensure dst table exists for overwrite

    request = TransferRequest(
        source=_pg_source(src_table),
        destination=_mysql_dest(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "data", "sync_mode": "full_refresh_overwrite", "primary_key": "id", "selected": True}],
    )
    _run(request)


def test_mysql_to_postgresql():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1), socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    src_table = "mysql_to_pg_src_" + uuid.uuid4().hex[:8]
    dst_table = "mysql_to_pg_dst_" + uuid.uuid4().hex[:8]
    _seed_mysql(src_table)
    _seed_postgresql(dst_table)

    request = TransferRequest(
        source=_mysql_source(src_table),
        destination=_pg_dest(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "data", "sync_mode": "full_refresh_overwrite", "primary_key": "id", "selected": True}],
    )
    _run(request)


def test_postgresql_to_sqlite():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    src_table = "pg_to_sqlite_src_" + uuid.uuid4().hex[:8]
    dst_table = "pg_to_sqlite_dst_" + uuid.uuid4().hex[:8]
    _seed_postgresql(src_table)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        request = TransferRequest(
            source=_pg_source(src_table),
            destination=_sqlite_dest(path, dst_table),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{"name": "data", "sync_mode": "full_refresh_overwrite", "primary_key": "id", "selected": True}],
        )
        _run(request)
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_sqlite_to_postgresql():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    src_table = "sq_to_pg_src_" + uuid.uuid4().hex[:8]
    dst_table = "sq_to_pg_dst_" + uuid.uuid4().hex[:8]
    _seed_postgresql(dst_table)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        _seed_sqlite(path, src_table)
        request = TransferRequest(
            source=_sqlite_source(path, src_table),
            destination=_pg_dest(dst_table),
            sync_mode="full_refresh_overwrite",
            stream_contracts=[{"name": "data", "sync_mode": "full_refresh_overwrite", "primary_key": "id", "selected": True}],
        )
        _run(request)
    finally:
        if os.path.exists(path):
            os.remove(path)
