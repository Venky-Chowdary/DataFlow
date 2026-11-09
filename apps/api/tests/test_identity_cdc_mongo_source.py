"""CDC from a live MongoDB replica set keyed on a business column, into every
live SQL destination, read back with each destination's own driver.

Lifecycle: snapshot, unchanged rerun, insert + update + delete, then an update
that changes the business key. The old key must be retired (Debezium: delete +
create); without a pre-image that update fails closed instead.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

import src.transfer.engine as engine_mod
from connectors.mongodb_change_stream import MongodbChangeStreamCdc
from services.cdc_capability import LogCaptureUnavailable
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.route_cert_readback import read_back
from tests.test_execute_tracked_universal_matrix import _build_db_endpoint, _endpoint_reachable
from tests.test_property2_golden_path_never_blocked import _FakeMongo

ENGINES = ["sqlite", "postgresql", "mysql", "sqlserver", "oracle"]
COLS = ["id", "region", "name", "updated_at"]
CFG = {"host": "localhost", "port": 27017, "database": "dataflow_cdc",
       "connection_string": "mongodb://localhost:27017/dataflow_cdc?directConnection=true",
       "ssl": False}


def _ready() -> bool:
    try:
        return MongodbChangeStreamCdc(CFG, collection="probe", primary_key="_id").is_available()
    except Exception:  # noqa: BLE001 — skip reason below
        return False


def _norm(value) -> str:
    text = "" if value is None else str(value)
    return text[:-2] if text.endswith(".0") else text


def _run(fake: _FakeMongo, coll: str, dest: EndpointConfig, primary_key: str = "id"):
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mongodb", table=coll, **CFG),
        destination=dest,
        sync_mode="cdc",
        validation_mode="strict",
        mappings=[{"source": c, "target": c, "confidence": 1.0, "approved": True,
                   "user_override": True} for c in COLS]
        + [{"source": "_id", "target": "", "confidence": 0.0, "intentional_omit": True}],
        stream_contracts=[{"name": coll, "selected": True, "sync_mode": "cdc", "primary_key": primary_key}],
        ddl_changes_acknowledged=True,
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
@pytest.mark.parametrize("engine", ENGINES)
def test_mongo_cdc_business_key_lifecycle(engine: str, tmp_path: Path, monkeypatch) -> None:
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    name = "cdcid_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="id")
    coll = cdc.coll
    coll.insert_many([{"id": 1, "region": "eu", "name": "a", "updated_at": 1},
                      {"id": 2, "region": "eu", "name": "b", "updated_at": 1}])
    cdc.client[cdc.db_name].command("collMod", name, changeStreamPreAndPostImages={"enabled": True})

    def source() -> list[tuple]:
        return sorted(tuple(_norm(d.get(c)) for c in COLS) for d in coll.find({}, {"_id": 0}))

    try:
        steps = [
            ("snapshot", lambda: None, 2),
            ("unchanged", lambda: None, 0),
            ("insert_update_delete", lambda: (
                coll.update_one({"id": 2}, {"$set": {"name": "B", "updated_at": 2}}),
                coll.insert_one({"id": 3, "region": "us", "name": "c", "updated_at": 2}),
                coll.delete_one({"id": 1})), None),
            ("key_mutation", lambda: coll.update_one({"id": 3}, {"$set": {"id": 30}}), None),
        ]
        for label, dml, written in steps:
            dml()
            result = _run(fake, name, dest)
            assert result.success, f"{label}: {result.error}"
            if written is not None:
                assert int(result.records_transferred or 0) == written, label
            assert (result.reconciliation or {}).get("passed") is not False, (label, result.reconciliation)
            landed = sorted(tuple(_norm(r[c]) for c in COLS) for r in read_back(dest, COLS))
            assert landed == source(), label
    finally:
        coll.drop()
        cdc.client.close()


def _change(op: str, after: dict, before: dict | None = None, touched: tuple = ()) -> dict:
    change = {"operationType": op, "fullDocument": after,
              "updateDescription": {"updatedFields": dict.fromkeys(touched, 0), "removedFields": []}}
    if before is not None:
        change["fullDocumentBeforeChange"] = before
    return change


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
@pytest.mark.parametrize("engine", ENGINES)
def test_mongo_cdc_composite_business_key_lifecycle(engine: str, tmp_path: Path, monkeypatch) -> None:
    """Composite ``(region, id)``: the same ``id`` in two regions is two rows; a
    delete and a change to one key component retire exactly that tuple."""
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    name = "cdccp_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="region,id")
    coll = cdc.coll
    coll.insert_many([{"id": 1, "region": "eu", "name": "a", "updated_at": 1},
                      {"id": 1, "region": "us", "name": "b", "updated_at": 1},
                      {"id": 2, "region": "eu", "name": "c", "updated_at": 1}])
    cdc.client[cdc.db_name].command("collMod", name, changeStreamPreAndPostImages={"enabled": True})

    def source() -> list[tuple]:
        return sorted(tuple(_norm(d.get(c)) for c in COLS) for d in coll.find({}, {"_id": 0}))

    try:
        steps = [
            ("snapshot", lambda: None, 3),
            ("unchanged", lambda: None, 0),
            ("insert_update_delete", lambda: (
                coll.update_one({"region": "us", "id": 1}, {"$set": {"name": "B", "updated_at": 2}}),
                coll.insert_one({"id": 3, "region": "us", "name": "d", "updated_at": 2}),
                coll.delete_one({"region": "eu", "id": 1})), None),
            ("key_component_mutation", lambda: coll.update_one(
                {"region": "eu", "id": 2}, {"$set": {"region": "apac"}}), None),
        ]
        for label, dml, written in steps:
            dml()
            result = _run(fake, name, dest, primary_key="region,id")
            assert result.success, f"{label}: {result.error}"
            if written is not None:
                assert int(result.records_transferred or 0) == written, label
            assert (result.reconciliation or {}).get("passed") is not False, (label, result.reconciliation)
            landed = sorted(tuple(_norm(r[c]) for c in COLS) for r in read_back(dest, COLS))
            assert landed == source(), label
    finally:
        coll.drop()
        cdc.client.close()


def test_composite_key_change_retires_old_tuple() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="region,id")
    events = cdc._row_events(
        _change("update", {"region": "apac", "id": 2}, {"region": "eu", "id": 2}, ("region",)),
        "update", {"region": "apac", "id": 2})
    from services.cdc_snapshot_window import _PK_SEP
    assert [(e.op, e.pk) for e in events] == [("d", f"eu{_PK_SEP}2"), ("u", f"apac{_PK_SEP}2")]


def test_composite_key_component_change_without_pre_image_fails_closed() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="region,id")
    with pytest.raises(LogCaptureUnavailable, match="old key"):
        cdc._row_events(_change("update", {"region": "apac", "id": 2}, None, ("region",)),
                        "update", {"region": "apac", "id": 2})


def test_composite_key_with_missing_component_is_keyless() -> None:
    from services.value_serializer import SQL_NULL_SENTINEL
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="region,id")
    assert cdc._pk_value({"id": 2}) == SQL_NULL_SENTINEL


def test_key_change_with_pre_image_retires_old_key() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="id")
    events = cdc._row_events(_change("update", {"id": 30}, {"id": 3}, ("id",)), "update", {"id": 30})
    assert [(e.op, e.pk) for e in events] == [("d", "3"), ("u", "30")]


def test_same_key_update_does_not_delete() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="id")
    events = cdc._row_events(_change("update", {"id": 3}, {"id": 3}, ("name",)), "update", {"id": 3})
    assert [(e.op, e.pk) for e in events] == [("u", "3")]


def test_key_change_without_pre_image_fails_closed() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="id")
    with pytest.raises(LogCaptureUnavailable, match="old key"):
        cdc._row_events(_change("update", {"id": 30}, None, ("id",)), "update", {"id": 30})


def test_non_key_update_without_pre_image_still_applies() -> None:
    cdc = MongodbChangeStreamCdc(CFG, collection="unit", primary_key="id")
    events = cdc._row_events(_change("update", {"id": 3}, None, ("name",)), "update", {"id": 3})
    assert [(e.op, e.pk) for e in events] == [("u", "3")]


@pytest.mark.skipif(not _ready(), reason="MongoDB replica set (change streams) not reachable")
@pytest.mark.parametrize("engine", ["sqlite", "postgresql"])
def test_mongo_cdc_wrong_typed_document_holds_window_until_fixed(
    engine: str, tmp_path: Path, monkeypatch
) -> None:
    """A schemaless document whose field cannot land in the destination type is
    refused with zero writes (never truncated or silently dropped); once the
    document is corrected at the source, the next run delivers the held window."""
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dest = _build_db_endpoint(engine, tmp_path, "dst", uuid.uuid4().hex[:8])
    if engine != "sqlite" and not _endpoint_reachable(dest):
        pytest.skip(f"{engine} not reachable on its configured host/port")
    name = "cdcwt_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="id")
    coll = cdc.coll
    coll.insert_many([{"id": 1, "region": "eu", "name": "a", "updated_at": 1},
                      {"id": 2, "region": "eu", "name": "b", "updated_at": 1}])
    cdc.client[cdc.db_name].command("collMod", name, changeStreamPreAndPostImages={"enabled": True})

    def landed() -> list[tuple]:
        return sorted(tuple(_norm(r[c]) for c in COLS) for r in read_back(dest, COLS))

    def source() -> list[tuple]:
        return sorted(tuple(_norm(d.get(c)) for c in COLS) for d in coll.find({}, {"_id": 0}))

    try:
        assert _run(fake, name, dest).success
        held = landed()
        coll.insert_one({"id": 3, "region": "eu", "name": "c", "updated_at": "not-a-number"})
        coll.insert_one({"id": 4, "region": "eu", "name": "d", "updated_at": 2})
        refused = _run(fake, name, dest)
        assert not refused.success
        assert "updated_at" in (refused.error or ""), refused.error
        assert landed() == held, "a refused window must write nothing"
        coll.update_one({"id": 3}, {"$set": {"updated_at": 3}})
        resumed = _run(fake, name, dest)
        assert resumed.success, resumed.error
        assert landed() == source(), "the held window was lost"
    finally:
        coll.drop()
        cdc.client.close()
