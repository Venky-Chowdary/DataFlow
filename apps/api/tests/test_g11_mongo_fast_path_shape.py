"""G11 review #2: the MongoDB COPY fast paths write dotted targets as sub-documents.

The identity COPY routes into MongoDB (SQLite / PostgreSQL / MySQL / Oracle /
SQL Server / Iceberg / S3 / Mongo -> Mongo) built each document as
``{target: value}``, so a Map target ``address.city`` was stored as a literal
top-level key ``"address.city"`` while the row path nests it. The Gate-8
read-back (``mongo_field_paths.value_at_path``) read a literal dotted key
first, which masked the wrong shape, so the run reported green.

Now every fast path inserts through ``copy_mongo_sink.insert_many_documents``,
which nests dotted keys the way the row path does, and the read-back reads a
path only through sub-documents: a literal dotted key is not the mapped field.

Driven through Studio's routes (plan Validate -> approved hashes ->
``/api/v1/transfer/run``); live MongoDB, throwaway ``g11f_`` databases.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")
FAST = "select_sqlite_insert_many_mongo"


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

#: As Studio's Map hands them over: each row carries its approved carrier.
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0, "target_type": "long"},
    {"source": "name", "target": "name", "confidence": 1.0, "target_type": "string"},
    {"source": "city", "target": "address.city", "confidence": 1.0, "target_type": "string"},
    {"source": "zip", "target": "address.zip", "confidence": 1.0, "target_type": "string"},
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

    name = f"g11f_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


def _source(path: Path, rows) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT, zip TEXT)")
        conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", rows)
    conn.close()


def _run(client, src: Path, db: str, coll: str, sync_mode: str) -> dict:
    from services.connector_store import create_connector

    dst = create_connector({
        "name": f"g11f-{uuid.uuid4().hex[:6]}", "role": "destination", "workspace_id": "",
        "type": "mongodb", "connection_string": MONGO_URI, "database": db, "ssl": False,
    })
    source = {"kind": "database", "format": "sqlite", "database": str(src), "table": "customers"}
    destination = {"kind": "database", "format": "mongodb", "connector_id": dst.id,
                   "database": db, "table": coll, "collection": coll}
    contracts = [{"name": "customers", "sync_mode": sync_mode, "primary_key": "id",
                  "selected": True}] if sync_mode == "upsert" else None
    plan_id, pf = flow.validate(
        client, source=source, destination=destination, columns=["id", "name", "city", "zip"],
        types={"id": "INTEGER", "name": "TEXT", "city": "TEXT", "zip": "TEXT"},
        mappings=MAPPINGS, sync_mode=sync_mode, stream_contracts=contracts,
    )
    assert pf.get("passed") is True, flow.blockers(pf)
    return flow.execute(
        client, plan_id=plan_id, pf=pf, source=source, destination=destination,
        mappings=MAPPINGS, sync_mode=sync_mode, stream_contracts=contracts,
    )


def _shapes(coll) -> list[dict]:
    return list(coll.find({}, {"_id": 0}).sort("id", 1))


@pytest.fixture
def short_dir():
    """The plan API caps an endpoint ``database`` at 128 characters."""
    import shutil
    import tempfile

    path = Path(tempfile.mkdtemp(prefix="g11f"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@live
def test_fast_path_overwrite_and_append_nest_then_upsert_keeps_one_shape(
    client, mongo_db, short_dir: Path
):
    mongo, db = mongo_db
    src = short_dir / "src.sqlite"
    _source(src, [(1, "Ada", "Paris", "75001"), (2, "Bob", "Oslo", "0150")])

    first = _run(client, src, db, "customers", "full_refresh_overwrite")
    assert first.get("success") is True, first.get("error")
    assert (first.get("destination_summary") or {}).get("load_method") == FAST, first.get(
        "destination_summary")
    assert (first.get("reconciliation") or {}).get("passed") is True, first.get("reconciliation")
    assert _shapes(mongo[db]["customers"]) == [
        {"id": 1, "name": "Ada", "address": {"city": "Paris", "zip": "75001"}},
        {"id": 2, "name": "Bob", "address": {"city": "Oslo", "zip": "0150"}},
    ]

    appended = _run(client, src, db, "archive", "full_refresh_append")
    assert appended.get("success") is True, appended.get("error")
    assert (appended.get("destination_summary") or {}).get("load_method") == FAST
    assert _shapes(mongo[db]["archive"]) == _shapes(mongo[db]["customers"])

    # Row path (upsert) into the collection the fast path filled: one shape.
    _source(src, [(1, "Ada", "Lyon", "75001"), (2, "Bob", "Oslo", "0150"),
                  (3, "Cy", "Rome", "00100")])
    upsert = _run(client, src, db, "customers", "upsert")
    assert upsert.get("success") is True, upsert.get("error")
    assert (upsert.get("destination_summary") or {}).get("load_method") != FAST
    assert (upsert.get("reconciliation") or {}).get("passed") is True, upsert.get("reconciliation")
    assert _shapes(mongo[db]["customers"]) == [
        {"id": 1, "name": "Ada", "address": {"city": "Lyon", "zip": "75001"}},
        {"id": 2, "name": "Bob", "address": {"city": "Oslo", "zip": "0150"}},
        {"id": 3, "name": "Cy", "address": {"city": "Rome", "zip": "00100"}},
    ]


def test_insert_many_documents_nests_dotted_keys():
    from services.copy_mongo_sink import insert_many_documents

    class _Coll:
        def __init__(self):
            self.docs = []

        def insert_many(self, docs, ordered=False):
            self.docs.extend(docs)
            return type("R", (), {"inserted_ids": list(range(len(docs)))})()

    coll = _Coll()
    assert insert_many_documents(coll, [{"id": 1, "address.city": "Paris"}]) == 1
    assert coll.docs == [{"id": 1, "address": {"city": "Paris"}}]


def test_a_literal_dotted_key_is_not_the_mapped_path():
    from connectors.mongo_field_paths import path_view

    assert path_view({"address.city": "Paris"}, ["address.city"]) == {}
    assert path_view({"address": {"city": "Paris"}}, ["address.city"]) == {"address.city": "Paris"}


@live
def test_gate8_fails_on_literal_dotted_keys(mongo_db):
    from services.reconciliation import canonical_checksum_from_iter
    from services.reconciliation_readback import verify_mongodb_collection

    mongo, db = mongo_db
    cols = ["id", "address.city"]
    expected = canonical_checksum_from_iter(
        iter([{"id": 1, "address.city": "Paris"}]), cols, dest_db_type="mongodb")
    mongo[db]["nested"].insert_one({"id": 1, "address": {"city": "Paris"}})
    mongo[db]["flat"].insert_one({"id": 1, "address.city": "Paris"})  # the old fast-path shape
    read = lambda c: verify_mongodb_collection(  # noqa: E731
        connection_string=MONGO_URI, database=db, table_name=c, target_columns=cols)
    assert read("nested") == (1, expected)
    assert read("flat")[1] != expected
