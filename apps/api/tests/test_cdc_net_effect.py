"""CDC txn net-effect coalesce + LSN-guarded delete proofs."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.cdc_effectively_once import (  # noqa: E402
    chaos_stale_delete_after_recreate,
    filter_keys_for_lsn_delete,
    should_apply_pk_delete,
)
from services.cdc_multi_table import MultiTableTransactionBuffer  # noqa: E402
from services.cdc_net_effect import CdcTxnEvent, coalesce_cdc_txn_events  # noqa: E402
from services.cdc_transaction_buffer import TransactionBuffer  # noqa: E402


def test_coalesce_delete_then_insert_emits_insert_only():
    events = [
        CdcTxnEvent(op="d", pk="1"),
        CdcTxnEvent(op="i", pk="1", row={"id": "1", "v": "recreated"}),
    ]
    inserts, updates, deletes = coalesce_cdc_txn_events(events)
    assert inserts == [{"id": "1", "v": "recreated"}]
    assert updates == []
    assert deletes == []


def test_coalesce_insert_then_delete_emits_delete_only():
    events = [
        CdcTxnEvent(op="i", pk="1", row={"id": "1", "v": "gone"}),
        CdcTxnEvent(op="d", pk="1"),
    ]
    inserts, updates, deletes = coalesce_cdc_txn_events(events)
    assert inserts == []
    assert updates == []
    assert deletes == ["1"]


def test_txn_buffer_delete_then_insert_same_pk():
    buf = TransactionBuffer(max_events=100)
    buf.begin("42")
    buf.delete("1")
    buf.insert({"id": "1", "v": "new"})
    batch = buf.commit(resume_token={"lsn": "0/10"})
    assert batch is not None
    assert len(batch.inserts) == 1
    assert batch.inserts[0]["v"] == "new"
    assert batch.deletes == []


def test_txn_buffer_spill_preserves_delete_insert_order(tmp_path: Path):
    buf = TransactionBuffer(max_events=100, spill_after=2, spill_dir=tmp_path)
    buf.begin("spill")
    buf.delete("1")
    buf.insert({"id": "1", "v": "a"})
    buf.insert({"id": "2", "v": "b"})
    batch = buf.commit(resume_token={"lsn": "0/1"})
    assert batch is not None
    assert batch.deletes == []
    by_id = {r["id"]: r for r in batch.inserts}
    assert by_id["1"]["v"] == "a"
    assert by_id["2"]["v"] == "b"


def test_multi_table_buffer_coalesce_per_table():
    buf = MultiTableTransactionBuffer(max_events=100)
    buf.begin("9")
    buf.delete("orders", "1")
    buf.insert("orders", {"id": "1", "v": "ok"})
    buf.insert("users", {"id": "9", "name": "a"})
    batches = buf.commit(resume_token={"lsn": "0/2"}, table_order=["orders", "users"])
    by_table = {b.table: b for b in batches}
    assert by_table["orders"].deletes == []
    assert by_table["orders"].inserts[0]["v"] == "ok"
    assert by_table["users"].inserts[0]["id"] == "9"


def test_stale_delete_rejected_after_recreate():
    assert should_apply_pk_delete(
        existing_lsn="0/200", incoming_lsn="0/100"
    ).applied is False
    assert should_apply_pk_delete(
        existing_lsn="0/50", incoming_lsn="0/100"
    ).applied is True
    sink = chaos_stale_delete_after_recreate("7")
    assert "7" in sink.rows
    assert sink.rows["7"]["v"] == "recreated"
    assert sink.rejected_stale_deletes >= 1


def test_filter_keys_for_lsn_delete():
    kept = filter_keys_for_lsn_delete(
        ["1", "2", "3"],
        {"1": "0/200", "2": "0/50", "3": None},
        "0/100",
    )
    assert kept == ["2", "3"]  # 1 is newer than incoming delete
