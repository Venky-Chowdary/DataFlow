"""PostgreSQL multi-stream CDC resume: two tables share one logical slot.

The first job run snapshots both tables and starts streaming. The second run,
using the same job_id, resumes from the persisted shared LSN watermark and must
pick up only the new changes, without duplicating the initial rows.
"""

from __future__ import annotations

import logging
import socket
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_conn import get_connection
from src.transfer.cdc_transfer import run_cdc_database_transfer
from src.transfer.models import EndpointConfig

CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _logical_decoding_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            row = cur.fetchone()
            return bool(row) and row[0] == "logical"
    except psycopg2.Error:
        return False


pytestmark = pytest.mark.skipif(
    not _logical_decoding_ready(),
    reason="PostgreSQL with wal_level=logical not reachable on localhost:5432",
)


def _exec(sql: str) -> None:
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def _select(table: str, schema: str = "public"):
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, amount, name FROM \"{schema}\".\"{table}\" ORDER BY id")
        return cur.fetchall()


def _drop_slot(slot_name: str) -> None:
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_drop_replication_slot(%s) "
            "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
            (slot_name, slot_name),
        )
        conn.commit()


def test_postgres_cdc_multi_stream_resume_is_effectively_once():
    """Two independent tables share one logical slot; resume is per shared LSN."""
    base = "cdc_mt_resume_" + uuid.uuid4().hex[:8]
    orders_table = f"{base}_orders"
    customers_table = f"{base}_customers"
    job_id = "mt-resume-" + uuid.uuid4().hex[:8]

    dest_schema = "mt_dest"
    slot_name = ""
    try:
        _exec(f"DROP SCHEMA IF EXISTS {dest_schema} CASCADE")
        _exec(f"CREATE SCHEMA {dest_schema}")
        _exec(f"DROP TABLE IF EXISTS {orders_table}")
        _exec(f"DROP TABLE IF EXISTS {customers_table}")
        _exec(
            f"CREATE TABLE {orders_table} (id INT PRIMARY KEY, amount NUMERIC(10,2), name TEXT)"
        )
        _exec(
            f"CREATE TABLE {customers_table} (id INT PRIMARY KEY, amount NUMERIC(10,2), name TEXT)"
        )
        _exec(
            f"INSERT INTO {orders_table} (id, amount, name) VALUES (1, 10.00, 'o1'), (2, 20.00, 'o2')"
        )
        _exec(
            f"INSERT INTO {customers_table} (id, amount, name) VALUES (1, 100.00, 'c1'), (2, 200.00, 'c2')"
        )

        src = EndpointConfig(
            kind="database",
            format="postgresql",
            **CFG,
            schema="public",
            table="",
        )
        dst = EndpointConfig(
            kind="database",
            format="postgresql",
            **CFG,
            schema=dest_schema,
            table="",
        )

        shared_mappings = [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "source_type": "NUMERIC", "target_type": "NUMERIC"},
            {"source": "name", "target": "name", "source_type": "TEXT", "target_type": "TEXT"},
        ]
        schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)", "name": "TEXT"}
        stream_contracts = [
            {
                "name": orders_table,
                "selected": True,
                "sync_mode": "cdc",
                "snapshot_mode": "initial",
                "primary_key": "id",
            },
            {
                "name": customers_table,
                "selected": True,
                "sync_mode": "cdc",
                "snapshot_mode": "initial",
                "primary_key": "id",
            },
        ]

        rows1, _, summary1, _ = run_cdc_database_transfer(
            src,
            dst,
            shared_mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream_contracts,
            job_id=job_id,
            limit=0,
        )
        cdc1 = summary1.get("cdc", {})
        slot_name = cdc1.get("cdc_slot_name", "")
        shared_wm1 = cdc1.get("watermark", "")

        assert rows1 == 4, f"expected 4 snapshot rows (2 per stream), got {rows1}"
        assert shared_wm1 and "phase=streaming" in shared_wm1 and "lsn=" in shared_wm1, (
            f"missing shared LSN watermark: {shared_wm1}"
        )
        assert summary1.get("cdc_shared_reader") is True
        assert _select(orders_table, dest_schema) == [(1, 10, "o1"), (2, 20, "o2")]
        assert _select(customers_table, dest_schema) == [(1, 100, "c1"), (2, 200, "c2")]

        # New changes on both tables while the job is "stopped".
        _exec(
            f"INSERT INTO {orders_table} (id, amount, name) VALUES (3, 30.00, 'o3')"
        )
        _exec(
            f"INSERT INTO {customers_table} (id, amount, name) VALUES (3, 300.00, 'c3')"
        )

        rows2, _, summary2, _ = run_cdc_database_transfer(
            src,
            dst,
            shared_mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream_contracts,
            job_id=job_id,
            limit=2,
        )
        cdc2 = summary2.get("cdc", {})
        shared_wm2 = cdc2.get("watermark", "")

        assert rows2 == 2, f"expected 2 resumed rows, got {rows2}"
        assert shared_wm2 and "phase=streaming" in shared_wm2 and "lsn=" in shared_wm2, (
            f"missing shared LSN watermark on resume: {shared_wm2}"
        )
        assert _select(orders_table, dest_schema) == [
            (1, 10, "o1"),
            (2, 20, "o2"),
            (3, 30, "o3"),
        ]
        assert _select(customers_table, dest_schema) == [
            (1, 100, "c1"),
            (2, 200, "c2"),
            (3, 300, "c3"),
        ]

        streams = summary2.get("streams", [])
        assert len(streams) == 2
        for s in streams:
            assert s.get("status") == "completed", s
    finally:
        try:
            _drop_slot(slot_name)
        except psycopg2.Error as exc:
            logging.getLogger(__name__).debug("slot cleanup failed: %s", exc, exc_info=exc)
        for t in (orders_table, customers_table):
            try:
                _exec(f"DROP TABLE IF EXISTS {t}")
            except psycopg2.Error:
                pass
        try:
            _exec(f"DROP SCHEMA IF EXISTS {dest_schema} CASCADE")
        except psycopg2.Error:
            pass
