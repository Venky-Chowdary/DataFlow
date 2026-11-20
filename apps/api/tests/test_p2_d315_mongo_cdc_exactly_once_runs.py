"""D315 follow-up: Mongo -> PostgreSQL exactly-once CDC must keep running after
its first copy.

The first ``cdc_position`` copy landed (D315), but every later run on the same
route failed with "exactly_once redelivery checksum mismatch" and applied
nothing. Root cause: the Mongo snapshot token is
``{"phase": "snapshot", "last_id": ..., "token": {"_data": ...}}``.
``extract_cdc_phase`` returned the nested change-stream token's *default*
phase ("streaming") before reading the wrapper's explicit ``phase``, so the
snapshot chunk was committed as streaming. The handoff to the change stream at
the same position then looked like a repeat, the dest-owned resume blob never
left the snapshot, and the next run re-read the snapshot tail at the committed
position with different rows. A collection larger than one chunk failed the
same way inside the first run, because the Mongo bookmark is ``last_id`` and
not ``last_pk``.

Live MongoDB replica set + live PostgreSQL for the end-to-end case.
"""

from __future__ import annotations

import uuid

import pytest

from tests.r13_mapping_harness import drop, endpoint, isolated, run_sql  # noqa: F401
from tests.test_d315_mongo_cdc_missing_nested_field_pg_e2e import (
    COLS,
    CFG,
    _doc,
    _expected,
    _landed,
    _ready,
    _run,
)

TOKEN = {"_data": "826AFFBC07000000202B0429296E1404"}


def test_mongo_snapshot_wrapper_keeps_its_explicit_phase():
    from services.cdc_exactly_once import extract_cdc_phase, extract_snapshot_last_pk

    snap = {"phase": "snapshot", "last_id": "30", "token": TOKEN, "collection": "c"}
    assert extract_cdc_phase(snap) == "snapshot"
    assert extract_snapshot_last_pk(snap) == "30"
    # The handoff and every stream token stay streaming.
    assert extract_cdc_phase(TOKEN) == "streaming"
    assert extract_cdc_phase({"phase": "streaming", "txn_held": True, "token": TOKEN}) == "streaming"
    # A wrapper with no phase of its own still defers to its nested token.
    assert extract_cdc_phase({"token": {"phase": "snapshot", "lsn": "0/1"}}) == "snapshot"


def test_mongo_handoff_at_the_snapshot_position_advances_the_phase():
    from services.cdc_exactly_once import decide_eos_apply

    lsn = "mongo:" + TOKEN["_data"]
    action, _ = decide_eos_apply(
        incoming_lsn=lsn, dest_lsn=lsn, incoming_phase="streaming", dest_phase="snapshot"
    )
    assert action == "handoff_phase"
    # Chunk 2 of the snapshot (same handoff position, later _id) applies.
    action, _ = decide_eos_apply(
        incoming_lsn=lsn,
        dest_lsn=lsn,
        incoming_phase="snapshot",
        dest_phase="snapshot",
        incoming_last_pk="14",
        dest_last_pk="7",
    )
    assert action == "apply"


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
def test_exactly_once_mongo_cdc_applies_changes_on_every_later_run(isolated, monkeypatch):  # noqa: F811
    import src.transfer.cdc_transfer as cdc_transfer
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    endpoint("postgresql", "probe")  # skips when PostgreSQL is not configured
    # 30 documents over five snapshot chunks: chunk 2..5 share chunk 1's position.
    monkeypatch.setattr(cdc_transfer, "CHUNK_SIZE", 7)
    name = "p2d315e_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    coll = cdc.coll
    docs = [_doc(i) for i in range(1, 31)]
    coll.insert_many(docs)
    try:
        result, job_id = _run(isolated, name, name, "cdc_position")
        assert result.success, result.error
        assert _landed(name) == {d["_id"]: _expected(d) for d in docs}

        coll.insert_one({"_id": 31, "name": "n31"})
        coll.update_one({"_id": 5}, {"$set": {"name": "renamed"}})
        coll.delete_one({"_id": 6})
        result, job_id = _run(isolated, name, name, "cdc_position")
        assert result.success, result.error
        landed = _landed(name)
        assert 6 not in landed
        assert landed[31] == (None,) * len(COLS)
        assert landed[5] == _expected(docs[4])
        names = dict(run_sql("postgresql", f'SELECT "_id", name FROM "{name}"'))  # nosec B608
        assert names[5] == "renamed" and names[31] == "n31"
        assert len(landed) == 30

        # An idle run is a no-op, not a redelivery failure, and writes nothing twice.
        result, _ = _run(isolated, name, name, "cdc_position")
        assert result.success, result.error
        assert run_sql("postgresql", f'SELECT count(*) FROM "{name}"')[0][0] == 30  # nosec B608
    finally:
        coll.drop()
        cdc.client.close()
        drop("postgresql", name)
        drop("postgresql", f"{name}_df_quarantine")


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
@pytest.mark.parametrize("semantics", ["", "cdc_position"], ids=["at_least_once", "exactly_once"])
def test_mongo_to_mysql_cdc_lands_missing_nested_fields_as_null_and_keeps_running(
    isolated, semantics  # noqa: F811
):
    """The story's second pair: Mongo -> MySQL CDC."""
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    endpoint("mysql", "probe")  # skips when MySQL is not configured
    name = "p2d315m_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    docs = [_doc(i) for i in range(1, 31)]
    cdc.coll.insert_many(docs)

    def run():
        contract = {"name": name, "selected": True, "sync_mode": "cdc", "primary_key": "_id"}
        if semantics:
            contract["cursor_semantics"] = semantics
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="mongodb", table=name, **CFG),
            destination=endpoint("mysql", name),
            sync_mode="cdc",
            validation_mode="balanced",
            stream_contracts=[contract],
            ddl_changes_acknowledged=True,
        )
        job_id = uuid.uuid4().hex[:24]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(request, job_id)

    def landed():
        return {
            int(r[0]): r[1:]
            for r in run_sql("mysql", f"SELECT `_id`, payload_k, payload_s, name FROM `{name}`")  # nosec B608
        }

    try:
        first = run()
        assert first.success, first.error
        rows = landed()
        assert len(rows) == 30
        assert rows[7][:2] == (None, None) and rows[9][:2] == (None, None)
        assert int(rows[8][0]) == 8 and rows[8][1] == "s8"

        cdc.coll.insert_one({"_id": 31, "name": "n31"})
        cdc.coll.update_one({"_id": 5}, {"$set": {"name": "renamed"}})
        cdc.coll.delete_one({"_id": 6})
        second = run()
        assert second.success, second.error
        rows = landed()
        assert 6 not in rows and rows[31][:2] == (None, None) and rows[5][2] == "renamed"
    finally:
        cdc.coll.drop()
        cdc.client.close()
        drop("mysql", name)
        drop("mysql", f"{name}_df_quarantine")
