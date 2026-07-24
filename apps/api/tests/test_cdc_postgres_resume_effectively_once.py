"""Real PostgreSQL CDC end-to-end effectively-once resume test.

Simulates a job that is stopped after the initial snapshot, then restarted.
The second run must resume from the persisted LSN watermark and stream only
new changes without duplicating the rows already loaded by the first run.
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


def _select(table: str):
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, amount FROM {table} ORDER BY id")
        return cur.fetchall()


def _drop_slot(slot_name: str) -> None:
    with get_connection(**CFG) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_drop_replication_slot(%s) "
            "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
            (slot_name, slot_name),
        )
        conn.commit()


def test_postgres_cdc_job_resume_is_effectively_once():
    """Two separate run_cdc_database_transfer calls must load distinct rows."""
    src_table = "cdc_resume_eo_" + uuid.uuid4().hex[:8]
    dst_table = src_table + "_dest"
    job_id = "resume-eo-" + uuid.uuid4().hex[:8]

    slot_name = ""
    try:
        _exec(f"DROP TABLE IF EXISTS {src_table}")
        _exec(f"DROP TABLE IF EXISTS {dst_table}")
        _exec(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, amount NUMERIC(10,2))")
        _exec(
            f"INSERT INTO {src_table} (id, amount) VALUES (1, 10.00), (2, 20.00)"
        )

        src = EndpointConfig(
            kind="database",
            format="postgresql",
            **CFG,
            schema="public",
            table=src_table,
        )
        dst = EndpointConfig(
            kind="database",
            format="postgresql",
            **CFG,
            schema="public",
            table=dst_table,
        )
        mappings = [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "source_type": "NUMERIC", "target_type": "NUMERIC"},
        ]
        schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)"}
        stream = [
            {
                "name": src_table,
                "selected": True,
                "snapshot_mode": "initial",
                "primary_key": "id",
            }
        ]

        rows1, _, summary1, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=2,
        )
        cdc1 = summary1.get("cdc", {})
        slot_name = cdc1.get("cdc_slot_name", "")
        wm1 = cdc1.get("watermark", "")

        assert rows1 == 2, f"expected 2 snapshot rows, got {rows1}"
        assert slot_name, "CDC slot name must be present"
        assert "phase=streaming" in wm1 and "lsn=" in wm1, f"missing LSN watermark: {wm1}"
        assert _select(dst_table) == [(1, 10), (2, 20)]

        # Simulate more source changes while the job is "stopped".
        _exec(
            f"INSERT INTO {src_table} (id, amount) VALUES (3, 30.00), (4, 40.00)"
        )

        # Second run uses the same job_id and therefore the same shared cursor key.
        # It must resume from the LSN stored after run 1, not re-run the snapshot.
        rows2, _, summary2, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=2,
        )
        cdc2 = summary2.get("cdc", {})
        wm2 = cdc2.get("watermark", "")

        assert rows2 == 2, f"expected 2 resumed rows, got {rows2}"
        assert "phase=streaming" in wm2 and "lsn=" in wm2, f"missing LSN watermark: {wm2}"
        assert _select(dst_table) == [
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
        ], "destination must contain exactly the four source rows, no duplicates"
    finally:
        try:
            _drop_slot(slot_name)
        except psycopg2.Error as exc:
            logging.getLogger(__name__).debug("slot cleanup failed: %s", exc, exc_info=exc)
        _exec(f"DROP TABLE IF EXISTS {dst_table}")
        _exec(f"DROP TABLE IF EXISTS {src_table}")
