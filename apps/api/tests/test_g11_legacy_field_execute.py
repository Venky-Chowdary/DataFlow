"""G11 review #3: Execute refuses to write a new field beside its stale old spelling.

Before G11, MongoDB and Elasticsearch writers renamed targets with SQL rules
(``address.city`` -> ``address_city``, ``full name`` -> ``full_name``,
``@timestamp`` -> ``_timestamp``). Targets now keep their names, so an
unchanged upsert/append job into a collection an earlier run filled would
``$set`` ``address.city`` while ``address_city`` stays stale, and Gate-8 (which
reads the mapped paths) stays green. Only Validate warned, and scheduled/API
runs do not Validate.

Now Execute applies the same rule as Validate's ``g23e`` before any write,
for every caller: the old field present + a non-overwrite run -> refused with
both spellings and the remedies (rename the target, one overwrite, or
``$rename``). An overwrite proceeds (nothing mixes). The operator can
acknowledge with ``legacy_field_policy=write_new``; the run records it.

Real path: Studio plan Validate -> approved hashes -> ``/api/v1/transfer/run``;
live MongoDB (throwaway ``g11x_`` databases). Elasticsearch through the bulk /
mapping fakes (no live Elasticsearch here).
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0, "target_type": "long"},
    {"source": "name", "target": "full name", "confidence": 1.0, "target_type": "string"},
    {"source": "city", "target": "address.city", "confidence": 1.0, "target_type": "string"},
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def mongo_db():
    import pymongo

    name = f"g11x_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


@pytest.fixture
def src():
    path = Path(tempfile.mkdtemp(prefix="g11x"))
    db = path / "src.sqlite"
    conn = sqlite3.connect(str(db))
    with conn:
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT)")
        conn.executemany("INSERT INTO customers VALUES (?, ?, ?)",
                         [(1, "Ada", "Paris"), (2, "Bob", "Oslo")])
    conn.close()
    try:
        yield db
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _endpoints(db: str, src: Path, extra: dict | None = None):
    from services.connector_store import create_connector

    dst = create_connector({
        "name": f"g11x-{uuid.uuid4().hex[:6]}", "role": "destination", "workspace_id": "",
        "type": "mongodb", "connection_string": MONGO_URI, "database": db, "ssl": False,
    })
    source = {"kind": "database", "format": "sqlite", "database": str(src), "table": "customers"}
    destination = {"kind": "database", "format": "mongodb", "connector_id": dst.id,
                   "database": db, "table": "customers", "collection": "customers",
                   **({"extra": extra} if extra else {})}
    return source, destination


def _validate(client, source, destination, sync_mode):
    contracts = ([{"name": "customers", "sync_mode": sync_mode, "primary_key": "id",
                   "selected": True}] if sync_mode == "upsert" else None)
    plan_id, pf = flow.validate(
        client, source=source, destination=destination, columns=["id", "name", "city"],
        types={"id": "INTEGER", "name": "TEXT", "city": "TEXT"}, mappings=MAPPINGS,
        sync_mode=sync_mode, stream_contracts=contracts,
    )
    return plan_id, pf, contracts


def _execute(client, plan_id, pf, source, destination, sync_mode, contracts, extra=None):
    return flow.execute(
        client, plan_id=plan_id, pf=pf, source=source, destination=destination,
        mappings=MAPPINGS, sync_mode=sync_mode, stream_contracts=contracts,
        extra_form={"dest_extra_json": json.dumps(extra)} if extra else None,
        expect_status=None,
    )


@live
def test_execute_refuses_when_an_earlier_run_left_the_old_fields(client, mongo_db, src):
    mongo, db = mongo_db
    coll = mongo[db]["customers"]
    coll.insert_one({"id": 9, "note": "app document"})
    source, destination = _endpoints(db, src)
    plan_id, pf, contracts = _validate(client, source, destination, "upsert")
    assert pf.get("passed") is True, flow.blockers(pf)

    # Between Validate and the scheduled run, the old job wrote its flat fields.
    coll.insert_one({"id": 8, "full_name": "Old", "address_city": "Lyon"})
    run = _execute(client, plan_id, pf, source, destination, "upsert", contracts)
    assert run.get("success") is not True, run
    error = str(run.get("error") or "")
    for needle in ("'address_city'", "'address.city'", "'full_name'", "'full name'",
                   "overwrite", "$rename", "legacy_field_policy"):
        assert needle in error, error
    # Nothing written.
    assert sorted(d["id"] for d in coll.find({})) == [8, 9]
    assert coll.count_documents({"address": {"$exists": True}}) == 0


@live
def test_acknowledged_write_new_proceeds_and_is_recorded(client, mongo_db, src):
    mongo, db = mongo_db
    coll = mongo[db]["customers"]
    coll.insert_one({"id": 8, "address_city": "Lyon"})
    ack = {"legacy_field_policy": "write_new"}
    source, destination = _endpoints(db, src, ack)
    plan_id, pf, contracts = _validate(client, source, destination, "upsert")
    assert pf.get("passed") is True, flow.blockers(pf)
    assert flow.gates_of(pf)["g23e_target_legacy_fields"]["status"] == "warn"
    run = _execute(client, plan_id, pf, source, destination, "upsert", contracts, extra=ack)
    assert run.get("success") is True, run.get("error")
    warnings = json.dumps((run.get("destination_summary") or {}).get("warnings") or [])
    assert "address_city kept as stored" in warnings and "write_new" in warnings, warnings
    assert coll.find_one({"id": 1}, {"_id": 0}) == {
        "id": 1, "full name": "Ada", "address": {"city": "Paris"}}


@live
def test_validate_blocks_append_and_execute_lets_an_overwrite_through(client, mongo_db, src):
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    mongo, db = mongo_db
    coll = mongo[db]["customers"]
    coll.insert_one({"id": 8, "address_city": "Lyon"})
    source, destination = _endpoints(db, src)
    _plan, pf, _c = _validate(client, source, destination, "full_refresh_append")
    assert pf.get("passed") is not True
    assert flow.gates_of(pf)["g23e_target_legacy_fields"]["status"] == "block"

    def request(sync_mode: str) -> TransferRequest:
        return TransferRequest(
            mappings=[dict(m) for m in MAPPINGS],
            source=EndpointConfig(kind="database", format="sqlite", database=str(src),
                                  table="customers"),
            destination=EndpointConfig(kind="database", format="mongodb",
                                       connection_string=MONGO_URI, database=db,
                                       collection="customers", table="customers"),
            sync_mode=sync_mode,
        )

    # An overwrite replaces the collection: nothing mixes, the run proceeds.
    assert request_refusal(request("full_refresh_overwrite")) == ""
    for mode in ("full_refresh_append", "upsert", "incremental_append"):
        assert "'address_city'" in request_refusal(request(mode)), mode


def test_elasticsearch_execute_refuses_the_old_renamed_field():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    client = MagicMock()
    client.indices.get_mapping.return_value = {"logs": {"mappings": {"properties": {
        "_timestamp": {"type": "date"}, "level": {"type": "keyword"}}}}}

    def request(extra=None, sync_mode="full_refresh_append"):
        return TransferRequest(
            mappings=[{"source": "ts", "target": "@timestamp"},
                      {"source": "level", "target": "level"}],
            source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
            destination=EndpointConfig(kind="database", format="elasticsearch",
                                       host="localhost", port=9200, table="logs",
                                       extra=dict(extra or {})),
            sync_mode=sync_mode,
            skip_preflight=True,
        )

    with patch("connectors.elasticsearch_reader._client", return_value=client):
        refused = request_refusal(request())
        assert "'_timestamp'" in refused and "'@timestamp'" in refused, refused
        assert request_refusal(request(sync_mode="full_refresh_overwrite")) == ""
        acked = request({"legacy_field_policy": "write_new"})
        assert request_refusal(acked) == ""
        assert acked.destination.extra["legacy_field_ack"] == [
            "_timestamp kept as stored; this run writes @timestamp (legacy_field_policy=write_new)"]
