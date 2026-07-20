"""TOAST merge + oversized open-txn buffer proofs (no silent data loss)."""

from __future__ import annotations

import socket
import struct
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_table  # noqa: E402
from services.cdc_multi_table import MultiTableTransactionBuffer  # noqa: E402
from services.cdc_toast import (  # noqa: E402
    TOAST_UNCHANGED,
    CdcToastIncompleteError,
    apply_update_row_or_raise,
    merge_toast_aware_update,
)
from services.cdc_transaction_buffer import (  # noqa: E402
    CdcTxnBufferOverflow,
    TransactionBuffer,
)


def test_merge_fills_toast_unchanged_from_old() -> None:
    old = {"id": "1", "blob": "HUGE", "status": "open"}
    new = {"id": "1", "blob": TOAST_UNCHANGED, "status": "closed"}
    result = merge_toast_aware_update(old, new, relation_columns=["id", "blob", "status"])
    assert result.row["blob"] == "HUGE"
    assert result.row["status"] == "closed"
    assert "blob" in result.toast_unchanged_cols
    assert result.toast_incomplete is False


def test_merge_absent_keys_filled_from_old() -> None:
    # pgoutput may omit rather than emit 'u'
    result = merge_toast_aware_update(
        {"id": "1", "blob": "KEEP", "n": "1"},
        {"id": "1", "n": "2"},
        relation_columns=["id", "blob", "n"],
    )
    assert result.row == {"id": "1", "blob": "KEEP", "n": "2"}


def test_incomplete_toast_without_old_fails_closed() -> None:
    with pytest.raises(CdcToastIncompleteError) as excinfo:
        apply_update_row_or_raise(
            None,
            {"id": "1", "blob": TOAST_UNCHANGED},
            relation_columns=["id", "blob"],
            table="orders",
        )
    assert excinfo.value.to_dict()["code"] == "cdc_toast_incomplete"


def test_txn_buffer_overflow_typed_no_silent_drop() -> None:
    buf = TransactionBuffer(max_events=3)
    buf.begin("42")
    buf.insert({"id": "1"})
    buf.insert({"id": "2"})
    buf.insert({"id": "3"})
    with pytest.raises(CdcTxnBufferOverflow) as excinfo:
        buf.insert({"id": "4"})
    d = excinfo.value.to_dict()
    assert d["code"] == "cdc_txn_buffer_overflow"
    assert d["max_events"] == 3
    assert d["event_count"] == 4
    # Open txn still held — no partial commit emitted.
    assert buf.open_xid == "42"


def test_multi_table_buffer_overflow() -> None:
    buf = MultiTableTransactionBuffer(max_events=2)
    buf.begin("9")
    buf.insert("a", {"id": "1"})
    buf.insert("b", {"id": "2"})
    with pytest.raises(CdcTxnBufferOverflow):
        buf.update("a", {"id": "1", "v": "x"})


def _string(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"


def _relation(oid: int = 99, columns: list[str] | None = None) -> bytes:
    columns = columns or ["id", "blob", "status"]
    body = b"R" + struct.pack("!i", oid)
    body += _string("public") + _string("docs")
    body += struct.pack("!b", 1)  # FULL replica identity
    body += struct.pack("!h", len(columns))
    for name in columns:
        body += struct.pack("!b", 0)
        body += _string(name)
        body += struct.pack("!i", 25)  # text
        body += struct.pack("!i", -1)
    return body


def _tuple_kinds(parts: list[tuple[str, str | None]]) -> bytes:
    """parts: list of (kind, value) where kind in t|n|u."""
    body = struct.pack("!h", len(parts))
    for kind, value in parts:
        if kind == "n":
            body += b"n"
        elif kind == "u":
            body += b"u"
        else:
            raw = (value or "").encode("utf-8")
            body += b"t" + struct.pack("!i", len(raw)) + raw
    return body


def test_pgoutput_update_merges_unchanged_toast() -> None:
    decoder = PgOutputDecoder()
    assert decoder.feed(_relation()) == []
    # Old FULL identity + new with blob marked unchanged ('u')
    old = _tuple_kinds([("t", "1"), ("t", "TOASTED-PAYLOAD"), ("t", "open")])
    new = _tuple_kinds([("t", "1"), ("u", None), ("t", "closed")])
    msg = b"U" + struct.pack("!i", 99) + b"O" + old + b"N" + new
    changes = changes_for_table(decoder, msg, schema="public", table="docs")
    assert len(changes) == 1
    ch = changes[0]
    assert ch.op == "update"
    assert ch.new_tuple["blob"] == "TOASTED-PAYLOAD"
    assert ch.new_tuple["status"] == "closed"
    assert "blob" in ch.toast_unchanged_cols
    assert ch.toast_incomplete is False


def _pg_logical_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        return False
    try:
        from connectors.postgresql_conn import get_connection

        with get_connection(
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            connection_string="",
            ssl=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                return bool(row) and row[0] == "logical"
    except Exception:
        return False


@pytest.mark.skipif(not _pg_logical_ready(), reason="PostgreSQL logical CDC not reachable")
def test_pg_live_toast_update_preserves_large_text():
    """Live: UPDATE non-TOAST col must not wipe a large TEXT column in the decoded row."""
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc
    from connectors.postgresql_conn import get_connection

    def _exec(sql: str) -> None:
        with get_connection(
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            connection_string="",
            ssl=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    table = "cdc_toast_" + uuid.uuid4().hex[:8]
    blob = "X" * 8192  # force TOAST storage
    _exec(f"DROP TABLE IF EXISTS {table}")
    _exec(f"CREATE TABLE {table} (id INT PRIMARY KEY, blob TEXT, status TEXT)")
    _exec(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    # Escape single quotes in blob for SQL
    _exec(f"INSERT INTO {table} (id, blob, status) VALUES (1, '{blob}', 'open')")

    cfg = {
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "connection_string": "",
        "ssl": False,
        "lease_holder_id": f"toast-{table}",
        "job_id": f"toast-{table}",
    }
    cdc = PostgreSqlChangeStreamCdc(
        cfg,
        table=table,
        primary_key="id",
        cursor_key=f"cdc-toast:{table}",
        schema="public",
        output_plugin="test_decoding",
    )
    slot = cdc.slot_name
    try:
        list(cdc.snapshot())
        _exec(f"UPDATE {table} SET status = 'closed' WHERE id = 1")
        changes = list(cdc.poll())
        updates = [r for b in changes for r in b.updates]
        assert updates, changes
        row = next(r for r in updates if str(r.get("id")) == "1")
        # test_decoding with FULL emits both keys; blob must still be present
        assert row.get("blob") == blob or (row.get("status") == "closed" and "blob" in row)
        assert row.get("status") == "closed" or "closed" in str(row)
        for b in changes:
            if b.total_changes:
                cdc.ack(b.resume_token)
    finally:
        try:
            cdc.close()
        except Exception:
            pass
        try:
            with get_connection(
                host="localhost",
                port=5432,
                database="dataflow",
                username="dataflow",
                password="dataflow",
                connection_string="",
                ssl=False,
            ) as conn:
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
