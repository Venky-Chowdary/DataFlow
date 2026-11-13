"""Document Modeler PR1: a flat mapping into MongoDB places values by field path.

Defect: the MongoDB writer named every target with SQL identifier rules, so a
mapping into an existing nested schema (``city -> address.city``) wrote a new
top-level field literally named ``address_city`` beside the application's
``address`` sub-document, and ``$x`` was renamed ``_x`` instead of refused.

Contract now (``services.document_paths`` rules):

* insert and overwrite nest dotted targets into sub-documents:
  ``{"address": {"city": ..., "zip": ...}}``;
* upsert keeps dotted paths in ``$set`` (``{"$set": {"address.city": ...}}``),
  so only the mapped nested field changes and sibling fields an application
  added inside the same sub-document (``address.geo``) survive - the G15
  contract, inside sub-documents; ``_id`` rules from PR 416 are unchanged;
* an invalid path is refused before anything is written, never renamed.

Live tests use a throwaway ``pr1_`` database that is dropped afterwards.
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo
        from pymongo.errors import PyMongoError
    except (OSError, ImportError):
        return False
    try:
        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except PyMongoError:
        return False
    return True


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

HEADERS = ["id", "name", "city", "zip"]
TYPES = {"id": "INTEGER", "name": "VARCHAR", "city": "VARCHAR", "zip": "VARCHAR"}
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "name", "target": "name", "confidence": 1.0},
    {"source": "city", "target": "address.city", "confidence": 1.0},
    {"source": "zip", "target": "address.zip", "confidence": 1.0},
]


# -- unit: target naming -----------------------------------------------------


def test_resolve_target_columns_keeps_dotted_paths_with_mongo_identifier():
    from connectors.destination_naming import writer_identifier
    from connectors.writer_common import resolve_target_columns

    cols, _ = resolve_target_columns(
        MAPPINGS + [{"source": "x", "target": "@ts"}],
        TYPES,
        preserve_case=True,
        identifier=writer_identifier("mongodb"),
    )
    assert cols == ["id", "name", "address.city", "address.zip", "@ts"]


def test_resolve_target_columns_default_is_unchanged_for_sql():
    from connectors.writer_common import resolve_target_columns

    cols, _ = resolve_target_columns(MAPPINGS, TYPES, preserve_case=True)
    assert cols == ["id", "name", "address_city", "address_zip"]


def test_mongo_upsert_update_keeps_dotted_paths_in_set():
    from connectors.mongodb_writer import _upsert_update

    doc = {"id": 1, "address.city": "Paris", "address.zip": "75001"}
    assert _upsert_update(doc, ["id"]) == {"$set": doc}


def test_mongo_insert_document_nests_dotted_paths():
    from connectors.mongo_field_paths import insert_document

    flat = {"_id": 7, "id": 7, "address.city": "Paris", "address.zip": "75001", "name": "A"}
    assert insert_document(flat) == {
        "_id": 7, "id": 7, "address": {"city": "Paris", "zip": "75001"}, "name": "A",
    }
    plain = {"id": 1, "name": "x"}
    assert insert_document(plain) == plain


def test_path_view_reads_nested_values_by_path():
    from connectors.mongo_field_paths import path_view

    doc = {"id": 1, "address": {"city": "Paris", "geo": {"lat": 1}}, "meta": {"tags": {"a": 1}}}
    view = path_view(doc, ["id", "address.city", "address.geo.lat", "meta.tags", "missing.x"])
    assert view == {"id": 1, "address.city": "Paris", "address.geo.lat": 1, "meta.tags": {"a": 1}}


# -- live: the writer --------------------------------------------------------


@pytest.fixture
def mongo_db():
    import pymongo

    name = f"pr1_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


def _write(db, coll, rows, *, mode="insert", create=True, mappings=None, conflict=None):
    from connectors.mongodb_writer import write_mapped_rows

    return write_mapped_rows(
        host="localhost", port=27017, database=db, username="", password="", schema="",
        connection_string=MONGO_URI, ssl=False, table_name=coll, headers=list(HEADERS),
        data_rows=[list(r) for r in rows], mappings=[dict(m) for m in (mappings or MAPPINGS)],
        column_types=dict(TYPES), create_table=create, write_mode=mode,
        conflict_columns=conflict,
    )


@live
def test_insert_nests_dotted_targets(mongo_db):
    client, db = mongo_db
    res = _write(db, "customers", [["1", "Ada", "Paris", "75001"], ["2", "Bob", "Oslo", "0150"]])
    assert res.ok, res.error
    docs = {d["id"]: d for d in client[db]["customers"].find({}, {"_id": 0})}
    assert docs[1] == {"id": 1, "name": "Ada", "address": {"city": "Paris", "zip": "75001"}}
    assert docs[2]["address"] == {"city": "Oslo", "zip": "0150"}
    assert not any("address_city" in d or "address.city" in d for d in docs.values())


@live
def test_upsert_sets_nested_paths_and_keeps_app_fields(mongo_db):
    client, db = mongo_db
    coll = client[db]["customers"]
    coll.insert_many([
        {"id": 1, "name": "old", "address": {"city": "Lyon", "zip": "69001",
                                             "geo": {"lat": 45.76, "lng": 4.83}},
         "loyalty": "gold"},
        {"id": 2, "name": "Bob", "address": {"city": "Oslo", "zip": "0150"}},
    ])
    res = _write(
        db, "customers",
        [["1", "Ada", "Paris", "75001"], ["3", "Cy", "Rome", "00100"]],
        mode="upsert", create=False, conflict=["id"],
    )
    assert res.ok, res.error
    docs = {d["id"]: d for d in coll.find({}, {"_id": 0})}
    # Mapped nested fields changed; the app's address.geo and loyalty survive.
    assert docs[1] == {
        "id": 1, "name": "Ada", "loyalty": "gold",
        "address": {"city": "Paris", "zip": "75001", "geo": {"lat": 45.76, "lng": 4.83}},
    }
    assert docs[2] == {"id": 2, "name": "Bob", "address": {"city": "Oslo", "zip": "0150"}}
    # A new key is inserted nested.
    assert docs[3] == {"id": 3, "name": "Cy", "address": {"city": "Rome", "zip": "00100"}}


@live
def test_overwrite_route_nests_dotted_targets(mongo_db):
    """The engine's overwrite drops/recreates the collection, then inserts."""
    client, db = mongo_db
    client[db]["customers"].insert_one({"id": 9, "address_city": "stale"})
    client[db]["customers"].drop()
    res = _write(db, "customers", [["1", "Ada", "Paris", "75001"]], create=True)
    assert res.ok, res.error
    assert list(client[db]["customers"].find({}, {"_id": 0})) == [
        {"id": 1, "name": "Ada", "address": {"city": "Paris", "zip": "75001"}}
    ]


@live
@pytest.mark.parametrize("bad", ["address..city", "$city", "address.$city", "address._id"])
def test_invalid_path_refused_before_write(mongo_db, bad):
    client, db = mongo_db
    mappings = [dict(m) for m in MAPPINGS]
    mappings[2] = {"source": "city", "target": bad, "confidence": 1.0}
    res = _write(db, "customers", [["1", "Ada", "Paris", "75001"]], mappings=mappings)
    assert res.ok is False
    assert bad in (res.error or ""), res.error
    assert client[db]["customers"].count_documents({}) == 0


@live
def test_scalar_and_subdocument_collision_refused_before_write(mongo_db):
    client, db = mongo_db
    mappings = [dict(m) for m in MAPPINGS]
    mappings[1] = {"source": "name", "target": "address", "confidence": 1.0}
    res = _write(db, "customers", [["1", "Ada", "Paris", "75001"]], mappings=mappings)
    assert res.ok is False
    assert "address.city" in (res.error or "") and "address" in (res.error or "")
    assert client[db]["customers"].count_documents({}) == 0
