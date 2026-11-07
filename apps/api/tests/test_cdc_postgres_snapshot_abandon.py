"""An abandoned CDC snapshot must not leave a transaction open on the source.

Regression: the snapshot dump runs every table inside one REPEATABLE READ
transaction. A consumer that stops iterating (batch cap, failed write, operator
cancel) closes the generator with ``GeneratorExit`` while that transaction is
still open, and the cleanup restored ``autocommit`` first — which PostgreSQL
rejects inside a transaction block (``set_session cannot be used inside a
transaction``). The connection then went back to the pool still
idle-in-transaction, holding an xmin that blocks vacuum on the source.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import psycopg2
import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
from connectors.postgresql_conn import get_connection

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


def test_abandoned_snapshot_leaves_no_open_transaction(caplog) -> None:
    suffix = uuid.uuid4().hex[:8]
    table = f"cdc_abandon_{suffix}"
    cdc: PostgreSqlChangeStreamCdc | None = None
    try:
        _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, amount INT)")
        _exec(
            f"INSERT INTO {table} (id, amount) VALUES "
            + ",".join(f"({i}, {i * 10})" for i in range(1, 21))
        )
        cdc = PostgreSqlChangeStreamCdc(
            cfg=dict(CFG),
            table=table,
            primary_key="id",
            cursor_key=f"abandon-{suffix}",
            schema="public",
            columns=["id", "amount"],
            batch_size=5,
        )
        stream = cdc.snapshot()
        first = next(stream)
        assert len(first.inserts) == 5
        # Consumer walks away mid-dump: GeneratorExit inside the RR transaction.
        stream.close()

        assert "set_session cannot be used inside a transaction" not in caplog.text

        # The source must have no session left holding a transaction open.
        with get_connection(**CFG) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = %s AND state LIKE 'idle in transaction%%'",
                (CFG["database"],),
            )
            row = cur.fetchone()
            assert row is not None and int(row[0]) == 0, "snapshot left an open txn"
    finally:
        if cdc is not None:
            cdc.close()
            # Replication slots are a hard-capped resource; never leak one.
            with get_connection(**CFG) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE slot_name = %s",
                    (cdc.slot_name,),
                )
                conn.commit()
        _exec(f"DROP TABLE IF EXISTS {table}")
