"""CDC incremental snapshot signal store."""

from __future__ import annotations

from services.cdc_incremental_snapshot import (
    claim_next_signal,
    complete_signal,
    list_signals,
    mark_chunk,
    request_incremental_snapshot,
)


def test_incremental_snapshot_lifecycle() -> None:
    sig = request_incremental_snapshot("src:pg", "orders", primary_key="id", chunk_size=100)
    assert sig.status == "pending"
    claimed = claim_next_signal("src:pg")
    assert claimed is not None
    assert claimed.id == sig.id
    assert claimed.status == "running"
    # Second claim while running should not re-claim same pending
    assert claim_next_signal("src:pg") is None
    mark_chunk(sig.id, last_pk="50", rows=50)
    done = complete_signal(sig.id)
    assert done is not None
    assert done.status == "completed"
    assert done.rows_snapshotted >= 50
    listed = list_signals("src:pg", status="completed")
    assert any(s.id == sig.id for s in listed)


def test_debezium_envelope_parse() -> None:
    from connectors.kafka_debezium_bridge import debezium_to_row, parse_debezium_envelope

    env = {
        "payload": {
            "op": "u",
            "before": {"id": 1, "n": "a"},
            "after": {"id": 1, "n": "b"},
            "source": {"table": "orders", "ts_ms": 123, "lsn": 99},
        }
    }
    change = parse_debezium_envelope(env)
    assert change is not None
    assert change.op == "u"
    assert change.table == "orders"
    row = debezium_to_row(change)
    assert row is not None
    assert row["n"] == "b"
    assert row["__op"] == "u"

    tomb = parse_debezium_envelope({"op": "d", "before": {"id": 1}, "source": {"table": "orders"}})
    assert tomb is not None
    trow = debezium_to_row(tomb)
    assert trow is not None
    assert trow.get("__deleted") is True
