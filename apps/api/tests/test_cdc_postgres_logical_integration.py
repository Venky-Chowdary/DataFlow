"""Real (non-mocked) PostgreSQL logical-decoding CDC integration test.

Runs against a live PostgreSQL started with ``wal_level=logical`` and enough
replication slots (see docker-compose ``postgres`` service). It creates a real
logical replication slot via the connector, applies real DML, and asserts the
``test_decoding`` parse path captures inserts/updates/deletes.

Skips cleanly when Postgres is unreachable or not configured for logical
decoding, so laptops with a stock Postgres stay green while docker-compose / CI
exercise the real slot.
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

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc  # noqa: E402
from connectors.postgresql_conn import get_connection  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _connect():
    return get_connection(
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _logical_decoding_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                return bool(row) and row[0] == "logical"
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _logical_decoding_ready(),
    reason="PostgreSQL with wal_level=logical not reachable on localhost:5432",
)


def _exec(sql: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def test_postgres_logical_snapshot_handoff_lsn_continuity():
    """Real PG: final snapshot token LSN must equal streaming handoff LSN."""
    from connectors.postgresql_change_stream import decode_pg_resume_token

    table = "cdc_handoff_" + uuid.uuid4().hex[:8]
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount NUMERIC(10,2))")
    _exec(f"INSERT INTO {table} (id, amount) VALUES (1, 10.00), (2, 20.00)")

    cursor_key = f"cdc-handoff-{table}"
    cdc = PostgreSqlChangeStreamCdc(
        CFG,
        table=table,
        primary_key="id",
        cursor_key=cursor_key,
        schema="public",
    )
    slot = cdc.slot_name
    try:
        assert cdc.is_available() is True
        batches = list(cdc.snapshot())
        assert batches, "snapshot must emit batches"
        handoff = str(batches[-1].resume_token)
        _slot, lsn, phase = decode_pg_resume_token(
            handoff,
            database=CFG["database"],
            table=table,
            cursor_key=cursor_key,
        )
        assert phase == "streaming", handoff
        assert lsn, f"handoff missing LSN: {handoff}"
        assert "phase=streaming" in handoff
        assert f"lsn={lsn}" in handoff
        # Mid-dump tokens share the same LSN once captured.
        if len(batches) > 1:
            mid = str(batches[0].resume_token)
            assert f"lsn={lsn}" in mid or "phase=snapshot" in mid

        # Streaming must continue from the same slot without recreating it.
        _exec(f"INSERT INTO {table} (id, amount) VALUES (3, 30.00)")
        changes = list(cdc.poll())
        inserts = [r for b in changes for r in b.inserts]
        assert any(str(r.get("id")) == "3" for r in inserts), inserts
        for batch in changes:
            cdc.ack(batch.resume_token)
    finally:
        try:
            cdc.close()
        except Exception:
            pass
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_drop_replication_slot(%s) "
                        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                        (slot, slot),
                    )
                conn.commit()
        except Exception:
            pass
        _exec(f"DROP TABLE IF EXISTS {table}")


def test_postgres_logical_snapshot_then_poll_captures_real_cdc():
    table = "cdc_orders_" + uuid.uuid4().hex[:8]
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount NUMERIC(10,2))")
    # REPLICA IDENTITY FULL so UPDATE/DELETE expose old-key columns in test_decoding.
    _exec(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    _exec(f"INSERT INTO {table} (id, amount) VALUES (1, 10.00), (2, 20.00)")

    cdc = PostgreSqlChangeStreamCdc(
        CFG,
        table=table,
        primary_key="id",
        cursor_key=f"cdc-test-{table}",
        schema="public",
    )
    slot = cdc.slot_name
    try:
        assert cdc.is_available() is True

        # Snapshot backfills existing rows and creates the logical slot.
        batches = list(cdc.snapshot())
        snap_inserts = [r for b in batches for r in b.inserts]
        assert len(snap_inserts) == 2, snap_inserts

        # Apply real DML the slot must capture.
        _exec(f"INSERT INTO {table} (id, amount) VALUES (3, 30.00)")
        _exec(f"UPDATE {table} SET amount = 99.00 WHERE id = 1")
        _exec(f"DELETE FROM {table} WHERE id = 2")

        changes = list(cdc.poll())
        inserts = [r for b in changes for r in b.inserts]
        updates = [r for b in changes for r in b.updates]
        deletes = [d for b in changes for d in b.deletes]

        assert any(str(r.get("id")) == "3" for r in inserts), f"insert not captured: {inserts}"
        assert any(
            str(r.get("id")) == "1" and str(r.get("amount")).startswith("99") for r in updates
        ), f"update not captured: {updates}"
        assert "2" in deletes, f"delete not captured: {deletes}"

        # Peek must not consume — re-poll should redeliver until ack.
        again = list(cdc.poll())
        assert any(str(r.get("id")) == "3" for b in again for r in b.inserts)
        for batch in changes:
            cdc.ack(batch.resume_token)
        after_ack = list(cdc.poll())
        assert not any(b.total_changes for b in after_ack), after_ack
    finally:
        try:
            cdc.close()
        except Exception:
            pass
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_drop_replication_slot(%s) "
                        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                        (slot, slot),
                    )
                conn.commit()
        except Exception:
            pass
        _exec(f"DROP TABLE IF EXISTS {table}")
