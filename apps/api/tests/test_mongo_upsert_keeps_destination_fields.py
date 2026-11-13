"""MongoDB upsert must not delete fields the mapping does not write.

Defect (data loss): a dense upsert batch wrote ``ReplaceOne(filter, doc,
upsert=True)``. Replacing the whole document deleted every field the mapping
does not write - fields the client's own application added to the documents
(``loyalty_tier``, an ``app_meta`` sub-document) or fields an earlier mapping
wrote. Only partial CDC images (``sparse``) used ``$set``. For SQL
destinations the product promise (G15, ``services/shape_contract.py``) is that
destination-only columns stay off SET on insert/upsert; MongoDB broke it.

Semantics now, the same as a SQL ``INSERT ... ON CONFLICT DO UPDATE SET
<mapped columns>``: every upsert (dense batch or CDC full image) is
``UpdateOne(filter, {"$set": <mapped fields>}, upsert=True)``.

* mapped fields are updated; a mapped field that is NULL in the source becomes
  ``null`` (present, not dropped and not left unchanged) - SQL sets the column
  to NULL;
* unmapped destination fields are untouched; an unmapped source column is not
  written;
* a new key is inserted with the mapped fields only (plus a server ``_id``);
* ``_id`` handling is unchanged: an existing document keeps its ``_id``, and a
  mapped 24-hex ``_id`` key still binds as an ObjectId;
* the ``_df_lsn`` stale-image guard still skips an older image.

The live tests use a throwaway ``c8_`` database that is dropped afterwards.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.mongodb_writer import write_mapped_rows  # noqa: E402
from connectors.writer_common import DF_LSN_COL  # noqa: E402

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

APP_META = {"source": "crm", "tags": ["vip", "beta"], "seen": 3}


@pytest.fixture
def mongo_db():
    import pymongo

    name = f"c8_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


def _source(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT, "
            "score INTEGER, internal_note TEXT)"
        )
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?)",
            [
                (1, "Ada", "ada@new.example", 10, "secret-1"),
                (2, "Grace", None, 20, "secret-2"),  # mapped field NULL in source
                (3, "Linus", "linus@new.example", 30, "secret-3"),
                (4, "Margaret", "margaret@new.example", 40, "secret-4"),  # new key
            ],
        )
    conn.close()


#: Writes id/name/email/score; ``internal_note`` is deliberately unmapped.
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "name", "target": "name", "confidence": 1.0},
    {"source": "email", "target": "email", "confidence": 1.0},
    {"source": "score", "target": "score", "confidence": 1.0},
]


def _request(src: Path, db: str, coll: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[dict(m) for m in MAPPINGS],
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=EndpointConfig(
            kind="database",
            format="mongodb",
            connection_string=MONGO_URI,
            database=db,
            collection=coll,
        ),
        sync_mode="upsert",
        stream_contracts=[{
            "name": "customers",
            "sync_mode": "upsert",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
        validation_mode="balanced",
    )


@live
def test_sqlite_to_mongo_upsert_keeps_destination_only_fields(tmp_path: Path, mongo_db):
    import src.transfer.engine as engine_mod
    from bson.objectid import ObjectId

    client, db = mongo_db
    coll_name = "customers"
    coll = client[db][coll_name]
    ids = {k: ObjectId() for k in (1, 2, 3, 99)}
    coll.insert_many([
        {"_id": ids[k], "id": k, "name": f"old-{k}", "email": f"old{k}@example.com",
         "score": 0, "loyalty_tier": tier, "app_meta": dict(APP_META)}
        for k, tier in ((1, "gold"), (2, "silver"), (3, None), (99, "bronze"))
    ])
    src = tmp_path / "src.sqlite"
    _source(src)

    for run in (1, 2):  # run 2 re-delivers the same rows: idempotent
        result = engine_mod.UniversalTransferEngine().execute_tracked(
            _request(src, db, coll_name), f"c8r{run}" + uuid.uuid4().hex[:12]
        )
        assert result.success, result.error
        assert (result.reconciliation or {}).get("passed") is True, result.reconciliation

        docs = {d["id"]: d for d in coll.find({})}
        assert sorted(docs) == [1, 2, 3, 4, 99], (run, docs)

        # Mapped fields updated; destination-only fields survive.
        for key, tier in ((1, "gold"), (2, "silver"), (3, None)):
            doc = docs[key]
            assert doc["_id"] == ids[key], (run, doc)  # identity unchanged
            assert doc["loyalty_tier"] == tier, (run, doc)
            assert doc["app_meta"] == APP_META, (run, doc)
            assert "internal_note" not in doc, (run, doc)
        assert (docs[1]["name"], docs[1]["email"], docs[1]["score"]) == (
            "Ada", "ada@new.example", 10), docs[1]
        assert (docs[3]["name"], docs[3]["score"]) == ("Linus", 30), docs[3]
        # A mapped field NULL in the source is written as null (SQL: SET email = NULL).
        assert "email" in docs[2] and docs[2]["email"] is None, docs[2]
        assert (docs[2]["name"], docs[2]["score"]) == ("Grace", 20), docs[2]

        # New key inserted with the mapped fields only and a server _id.
        new = docs[4]
        assert isinstance(new["_id"], ObjectId), new
        assert set(new) == {"_id", "id", "name", "email", "score"}, new
        assert (new["name"], new["email"], new["score"]) == (
            "Margaret", "margaret@new.example", 40), new

        # A destination document the source does not hold is untouched.
        assert docs[99]["_id"] == ids[99]
        assert docs[99]["name"] == "old-99" and docs[99]["loyalty_tier"] == "bronze"
        assert docs[99]["app_meta"] == APP_META


def _write(db: str, coll: str, headers, rows, types, conflict, create=False):
    return write_mapped_rows(
        host="localhost", port=27017, database=db, username="", password="", schema="",
        connection_string=MONGO_URI, ssl=False, table_name=coll, headers=list(headers),
        data_rows=rows,
        mappings=[{"source": h, "target": h, "confidence": 1.0} for h in headers],
        column_types=types, create_table=create, write_mode="upsert",
        conflict_columns=conflict,
    )


@live
def test_mongo_upsert_on_id_key_keeps_fields_and_objectid_identity(mongo_db):
    from bson.objectid import ObjectId

    client, db = mongo_db
    coll = client[db]["by_oid"]
    existing, fresh = ObjectId(), ObjectId()
    coll.insert_one({"_id": existing, "name": "old", "loyalty_tier": "gold",
                     "app_meta": dict(APP_META)})

    result = _write(db, "by_oid", ["_id", "name"],
                    [[str(existing), "new"], [str(fresh), "fresh"]],
                    {"_id": "string", "name": "string"}, ["_id"])
    assert result.ok, result.error
    assert result.rows_written == 2

    kept = coll.find_one({"_id": existing})
    assert kept == {"_id": existing, "name": "new", "loyalty_tier": "gold",
                    "app_meta": APP_META}, kept
    # The 24-hex key still binds as an ObjectId on insert, as before.
    assert coll.find_one({"_id": fresh}) == {"_id": fresh, "name": "fresh"}
    assert coll.count_documents({}) == 2


@live
def test_mongo_cdc_full_image_sets_mapped_fields_and_keeps_lsn_guard(mongo_db):
    client, db = mongo_db
    coll = client[db]["cdc_full"]
    coll.insert_one({"id": 1, "amount": "old", DF_LSN_COL: "0/16B3700",
                     "loyalty_tier": "gold", "app_meta": dict(APP_META)})
    headers = ["id", "amount", DF_LSN_COL]
    types = {"id": "INTEGER", "amount": "TEXT", DF_LSN_COL: "TEXT"}

    # A full-row CDC image (no missing fields) with a newer LSN.
    newer = _write(db, "cdc_full", headers, [["1", "new", "0/16B3748"]], types, ["id"])
    assert newer.ok, newer.error
    assert (newer.rows_written, newer.rows_skipped) == (1, 0)
    doc = coll.find_one({"id": 1}, {"_id": 0})
    assert doc == {"id": 1, "amount": "new", DF_LSN_COL: "0/16B3748",
                   "loyalty_tier": "gold", "app_meta": APP_META}, doc

    # A stale redelivery is still skipped by the LSN guard.
    stale = _write(db, "cdc_full", headers, [["1", "stale", "0/16B3000"]], types, ["id"])
    assert stale.ok, stale.error
    assert (stale.rows_written, stale.rows_skipped) == (0, 1)
    assert coll.find_one({"id": 1}, {"_id": 0})["amount"] == "new"


def test_dense_upsert_emits_set_update_not_replace():
    """Driver-free guard: a dense upsert row is ``UpdateOne`` with ``$set``."""
    captured: list = []

    class _Coll:
        def find(self, *a, **k):
            return []

        def create_index(self, *a, **k):
            return None

        def bulk_write(self, ops, ordered=False):
            captured.extend(ops)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

        def list_collection_names(self, filter=None):  # noqa: A002
            return []

    class _Client:
        def __getitem__(self, name):
            return _Db()

        def close(self):
            return None

    with patch("connectors.mongodb_common._mongo_client", return_value=_Client()):
        result = _write("testdb", "orders", ["id", "note"], [["1", "n1"], ["2", None]],
                        {"id": "string", "note": "string"}, ["id"], create=True)
    assert result.ok, result.error
    assert [type(op).__name__ for op in captured] == ["UpdateOne", "UpdateOne"]
    assert captured[0]._filter == {"id": "1"}
    assert captured[0]._doc == {"$set": {"id": "1", "note": "n1"}}
    assert captured[1]._doc == {"$set": {"id": "2", "note": None}}
    assert all(op._upsert for op in captured)
