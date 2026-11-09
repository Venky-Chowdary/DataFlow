"""A primary-key UPDATE must retire the old key (Debezium: delete + create).

Live PG CDC applied only the after-image of ``UPDATE t SET id=30 WHERE id=3``,
so the destination kept both 3 and 30 and reconcile still passed.
"""

from __future__ import annotations

from services.cdc_multi_table import MultiTableTransactionBuffer
from services.cdc_snapshot_window import _retired_key_events
from services.cdc_transaction_buffer import TransactionBuffer


def test_single_table_buffer_deletes_old_key_on_key_change() -> None:
    buf = TransactionBuffer()
    buf.begin("x1")
    buf.update({"id": "30", "name": "c"}, pk="30", old_pk="3")
    batch = buf.commit(lsn="0/1")
    assert batch is not None
    assert batch.deletes == ["3"]
    assert [r["id"] for r in batch.updates] == ["30"]


def test_single_table_buffer_same_key_update_has_no_delete() -> None:
    buf = TransactionBuffer()
    buf.begin("x1")
    buf.update({"id": "2", "name": "B"}, pk="2", old_pk="2")
    batch = buf.commit(lsn="0/1")
    assert batch is not None and batch.deletes == []


def test_multi_table_buffer_deletes_old_key_on_key_change() -> None:
    buf = MultiTableTransactionBuffer()
    buf.begin("x1")
    buf.update("t", {"id": "30", "name": "c"}, pk="30", old_pk="3")
    batches = buf.commit(lsn="0/1")
    deletes = [d for b in batches for d in b.deletes]
    assert deletes == ["3"]


def test_window_events_retire_composite_old_key() -> None:
    events = _retired_key_events("us\x1f3", "us\x1f30", ["region", "id"])
    assert events == [{"op": "d", "pk": "us\x1f3", "row": {"region": "us", "id": "3"}}]
    assert _retired_key_events("us\x1f3", "us\x1f3", ["region", "id"]) == []
    assert _retired_key_events("", "us\x1f3", ["region", "id"]) == []


def test_oracle_redo_key_change_retires_old_key() -> None:
    from connectors.oracle_logminer import classify_sql_redo, pop_retired_key

    _kind, row = classify_sql_redo(
        'update "DATAFLOW"."T" set "ID" = \'30\' where "ID" = \'3\' and "NAME" = \'c\'',
        op="update",
    )
    assert pop_retired_key(row, "id") == "3"
    assert row == {"ID": "30", "NAME": "c"}


def test_oracle_redo_composite_key_change_and_same_key_update() -> None:
    from connectors.oracle_logminer import classify_sql_redo, pop_retired_key, redo_key

    _kind, row = classify_sql_redo(
        'update "T" set "ID" = \'30\' where "REGION" = \'us\' and "ID" = \'3\'',
        op="update",
    )
    assert redo_key(row, "REGION,ID") == "us\x1f30"
    assert pop_retired_key(row, "REGION,ID") == "us\x1f3"
    _kind, same = classify_sql_redo(
        'update "T" set "NAME" = \'B\' where "REGION" = \'eu\' and "ID" = \'2\'',
        op="update",
    )
    assert pop_retired_key(same, "REGION,ID") is None
