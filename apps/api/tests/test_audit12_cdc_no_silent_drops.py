"""Audit finding 12 (M3, part) — the CDC transaction buffer dropped changes silently.

* A DELETE whose key could not be resolved was discarded with no error: the
  row stayed at the destination and the stream moved on.
* An unparsable line of the on-disk transaction spill was skipped: a spilled
  insert, update or delete vanished from the committed transaction.

Both now fail closed, the way the MySQL reader already refuses an unkeyed
delete: the transaction stays open, nothing is emitted, and the cursor is not
advanced past a change that was not applied.
"""

from __future__ import annotations

import pytest

from services.cdc_multi_table import MultiTableTransactionBuffer
from services.cdc_transaction_buffer import (
    CdcSpillCorrupt,
    CdcUnkeyedDelete,
    TransactionBuffer,
    load_cdc_txn_spill_event,
)


@pytest.mark.parametrize("key", ["", None])
def test_unkeyed_delete_is_refused_not_dropped(key):
    buf = TransactionBuffer()
    buf.begin("x1")
    buf.insert({"id": 1, "v": "a"}, pk="1")

    with pytest.raises(CdcUnkeyedDelete) as raised:
        buf.delete(key)

    assert "no resolvable key" in str(raised.value)
    # The transaction is still open and intact: nothing was emitted or lost.
    assert buf.open_xid == "x1"
    batch = buf.commit()
    assert [r["id"] for r in batch.inserts] == [1] and batch.deletes == []


def test_unkeyed_delete_is_refused_by_the_multi_table_buffer():
    buf = MultiTableTransactionBuffer()
    buf.begin("x1")
    buf.insert("orders", {"id": 1}, pk="1")
    with pytest.raises(CdcUnkeyedDelete) as raised:
        buf.delete("orders", "")
    assert "orders" in str(raised.value)
    assert buf.open_xid == "x1" and buf.open_event_count == 1


def test_drain_surfaces_an_unkeyed_delete():
    buf = TransactionBuffer()
    events = [("begin", {"xid": "x"}), ("delete", {"pk": None}), ("commit", {})]
    with pytest.raises(CdcUnkeyedDelete):
        list(buf.drain(iter(events)))


def test_a_keyed_delete_still_flows():
    buf = TransactionBuffer()
    buf.begin("x1")
    buf.delete("7")
    assert buf.commit().deletes == ["7"]


def _spilled(tmp_path, monkeypatch) -> TransactionBuffer:
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    buf = TransactionBuffer(max_events=1000, spill_after=50)
    buf.begin("big")
    for i in range(120):
        buf.insert({"id": i, "v": f"v{i}"}, pk=str(i))
    assert buf._open is not None and buf._open.spill_path
    return buf


@pytest.mark.parametrize(
    "bad_line",
    ['{"op":"i","pk":"7","row":{"id":7,"v":"trunc', "not json at all", '["a list"]',
     '{"op":"x","pk":"1","row":{"id":1}}'],
)
def test_corrupt_spill_line_fails_the_commit_instead_of_losing_the_event(
    bad_line, tmp_path, monkeypatch
):
    buf = _spilled(tmp_path, monkeypatch)
    path = buf._open.spill_path
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(bad_line + "\n")

    with pytest.raises(CdcSpillCorrupt) as raised:
        buf.commit()

    assert "line" in str(raised.value) and "was not applied" in str(raised.value)
    # Fail closed: the transaction is still open for redelivery.
    assert buf.open_xid == "big"
    buf.rollback()


def test_an_intact_spill_still_commits_every_event(tmp_path, monkeypatch):
    buf = _spilled(tmp_path, monkeypatch)
    batch = buf.commit()
    assert len(batch.inserts) == 120


def test_the_spill_line_reader_still_reports_an_unreadable_line_as_none():
    assert load_cdc_txn_spill_event('{"op":"i","pk":"7","row":{"id":7,"v":"trunc') is None
    assert load_cdc_txn_spill_event('{"op":"d","pk":"7"}') == {"op": "d", "pk": "7"}


def test_sqlserver_cdc_classifier_refuses_an_unkeyed_delete():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    keyed = [{"__$operation": 1, "id": 7, "v": "x"}, {"__$operation": 2, "id": 8, "v": "y"}]
    inserts, _updates, deletes = classify_mssql_cdc_rows(keyed, primary_key="id")
    assert deletes == ["7"] and [str(r["id"]) for r in inserts] == ["8"]

    with pytest.raises(CdcUnkeyedDelete):
        classify_mssql_cdc_rows([{"__$operation": 1, "v": "x"}], primary_key="id")
