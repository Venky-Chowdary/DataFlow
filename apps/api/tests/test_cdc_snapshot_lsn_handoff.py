"""CDC snapshot → LSN streaming handoff proofs (Debezium-style continuity).

Invariant: after the initial dump, the final resume token must be
``phase=streaming`` at the same LSN captured during the REPEATABLE READ
snapshot — so poll resumes without a gap outside the replication slot.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.postgresql_change_stream import (
    PostgreSqlChangeStreamCdc,
    decode_pg_resume_token,
    encode_pg_resume_token,
)


def test_postgres_snapshot_handoff_token_starts_streaming_at_same_lsn() -> None:
    """Snapshot end LSN must equal the first streaming resume LSN (no gap)."""
    cfg = {
        "host": "localhost",
        "port": 5432,
        "database": "test",
        "username": "",
        "password": "",
        "connection_string": "",
        "ssl": False,
        "schema": "public",
    }
    cursor_key = "pg:test:orders→sql:test:dst:stream"
    cdc = PostgreSqlChangeStreamCdc(
        cfg,
        table="orders",
        primary_key="id",
        cursor_key=cursor_key,
        columns=["id", "amount"],
    )

    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    snapshot_lsn = "0/16B3600"
    cur.fetchone.side_effect = [(snapshot_lsn,), None]
    cur.fetchall.side_effect = [
        [("1", "10.00")],
        [],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.autocommit = True

    with patch("connectors.postgresql_change_stream.get_connection", return_value=conn), \
         patch.object(cdc, "_ensure_slot", return_value="0/16B3500"), \
         patch.object(cdc, "_ensure_decode_schema", return_value={}), \
         patch("connectors.postgresql_reader._order_by_clause", return_value='"id"'):
        batches = list(cdc.snapshot())

    assert batches, "snapshot must emit at least one batch"
    handoff = str(batches[-1].resume_token)
    slot, lsn, phase = decode_pg_resume_token(
        handoff,
        database="test",
        table="orders",
        cursor_key=cursor_key,
    )
    assert phase == "streaming"
    assert lsn == snapshot_lsn
    assert handoff == encode_pg_resume_token(cdc.slot_name, lsn=snapshot_lsn, phase="streaming")
    # Slot name continuity — streaming continues on the same replication slot.
    assert slot == cdc.slot_name
    # Mid-dump tokens stay in snapshot phase at the same LSN.
    assert "phase=snapshot" in str(batches[0].resume_token)
    assert f"lsn={snapshot_lsn}" in str(batches[0].resume_token)
