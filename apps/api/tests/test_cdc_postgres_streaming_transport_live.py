"""Live PostgreSQL proof for the START_REPLICATION CDC transport (Phase F4).

Needs a reachable PostgreSQL with ``wal_level=logical`` and a REPLICATION role
(``localhost:5432`` dataflow/dataflow in the dev compose). Covers what the peek
transport already guarantees, now on the streaming connection:

* acked commits move ``confirmed_flush_lsn`` via feedback (no second connection);
* a killed walsender reconnects from the ack position — no loss, no resend of
  acked work;
* an idle slot releases WAL when only unpublished tables write;
* the runner's forced mid-batch restarts stay duplicate-free on streaming.
"""

from __future__ import annotations

import socket
import sys
import time
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
}


def _ready() -> bool:
    try:
        with socket.create_connection((CFG["host"], CFG["port"]), timeout=1):
            pass
        conn = get_connection(**CFG, connection_string="", ssl=False)
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                return str(cur.fetchone()[0]) == "logical"
        finally:
            conn.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _ready(), reason="PostgreSQL with wal_level=logical not reachable on localhost:5432"
)


def _sql(sql: str, params: tuple = (), fetch: bool = False):
    conn = get_connection(**CFG, connection_string="", ssl=False)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    finally:
        conn.close()


def _slot(slot: str) -> tuple:
    rows = _sql(
        "SELECT confirmed_flush_lsn::text, active, active_pid FROM pg_replication_slots "
        "WHERE slot_name = %s",
        (slot,),
        fetch=True,
    )
    return rows[0] if rows else (None, None, None)


def _lsn_ge(a: str | None, b: str) -> bool:
    from connectors.postgresql_cdc_transport import lsn_to_int

    return bool(a) and lsn_to_int(a) >= lsn_to_int(b)


def _wait(pred, timeout: float = 10.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.1)
    return pred()


def _ids(batches) -> list[str]:
    return [str(r.get("id")) for b in batches for r in b.inserts]


def _poll_until(cdc, want: set[str], timeout: float = 15.0):
    seen: list[str] = []
    batches = []
    end = time.monotonic() + timeout
    while time.monotonic() < end and not want.issubset(seen):
        got = list(cdc.poll())
        batches.extend(got)
        seen.extend(_ids(got))
    return batches, seen


@pytest.fixture()
def stream(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CDC_PG_TRANSPORT", "streaming")
    monkeypatch.setenv("DATAFLOW_CDC_HEARTBEAT_SEC", "0")
    table = "cdc_stream_" + uuid.uuid4().hex[:8]
    _sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, v TEXT)")
    _sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    cdc = PostgreSqlChangeStreamCdc(
        CFG, table=table, primary_key="id", cursor_key=f"stream-{table}", schema="public"
    )
    assert cdc.is_available() is True
    assert len([r for b in cdc.snapshot() for r in b.inserts]) == 2
    try:
        yield table, cdc
    finally:
        cdc.close()
        slot = cdc.slot_name
        _wait(lambda: not _slot(slot)[1], 10)
        _sql("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (slot,))
        _sql(f"DROP PUBLICATION IF EXISTS {cdc.publication_name}")
        _sql(f"DROP TABLE IF EXISTS {table}")


def _last_token(batches):
    toks = [b.resume_token for b in batches if b.resume_token]
    assert toks, "no resume token emitted"
    return toks[-1]


def test_streaming_ack_moves_confirmed_flush_via_feedback(stream):
    table, cdc = stream
    _sql(f"INSERT INTO {table} VALUES (3, 'c')")
    batches, seen = _poll_until(cdc, {"3"})
    assert "3" in seen
    meta = cdc.cdc_metadata()
    assert meta["cdc_transport"] == "streaming", meta
    # Unacked -> redelivered on the next poll (peek semantics on streaming).
    assert "3" in _ids(list(cdc.poll()))
    cdc.ack(_last_token(batches))
    acked = cdc.consistent_point_lsn
    assert _wait(lambda: _lsn_ge(_slot(cdc.slot_name)[0], acked)), (_slot(cdc.slot_name), acked)
    assert "3" not in _ids(list(cdc.poll()))


def test_streaming_reconnects_after_walsender_kill_without_loss_or_resend(stream):
    table, cdc = stream
    _sql(f"INSERT INTO {table} VALUES (3, 'c')")
    batches, _ = _poll_until(cdc, {"3"})
    cdc.ack(_last_token(batches))
    transport = cdc._streaming_transport
    pid = _slot(cdc.slot_name)[2]
    assert pid, "slot should be held by the walsender"
    _sql("SELECT pg_terminate_backend(%s)", (pid,))
    _sql(f"INSERT INTO {table} VALUES (4, 'd')")
    _sql(f"INSERT INTO {table} VALUES (5, 'e')")
    batches, seen = _poll_until(cdc, {"4", "5"}, timeout=30)
    assert {"4", "5"}.issubset(seen), seen
    assert "3" not in seen, f"acked txn was resent after reconnect: {seen}"
    assert transport.reconnects >= 1
    assert cdc.cdc_metadata()["stream_reconnects"] >= 1


def test_streaming_idle_slot_releases_wal_from_unpublished_writes(stream):
    table, cdc = stream
    _sql(f"INSERT INTO {table} VALUES (3, 'c')")
    batches, _ = _poll_until(cdc, {"3"})
    cdc.ack(_last_token(batches))
    noise = table + "_noise"
    _sql(f"CREATE TABLE {noise} (id INT)")
    try:
        _sql(f"INSERT INTO {noise} SELECT g FROM generate_series(1, 5000) g")
        head = _sql("SELECT pg_current_wal_lsn()::text", fetch=True)[0][0]

        def _released():
            cdc._last_heartbeat_at = None
            cdc.heartbeat()
            list(cdc.poll())
            return _lsn_ge(_slot(cdc.slot_name)[0], head)

        assert _wait(_released, 20), (_slot(cdc.slot_name), head)
    finally:
        _sql(f"DROP TABLE IF EXISTS {noise}")


def test_runner_forced_restarts_stay_duplicate_free_on_streaming(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CDC_PG_TRANSPORT", "streaming")
    import tests.test_cdc_exactly_once_postgres_restart_live as restart_live

    restart_live.test_postgres_cdc_exactly_once_survives_forced_mid_batch_restarts(monkeypatch)
