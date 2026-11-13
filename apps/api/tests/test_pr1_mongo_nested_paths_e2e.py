"""Document Modeler PR1, live: SQL rows into an existing nested MongoDB schema.

The user's need: migrate SQL rows into a MongoDB collection whose documents
already have nested objects. A flat mapping ``city -> address.city`` must land
as ``{"address": {"city": ...}}``; a ``nest_json`` transform output must land
as a real sub-document, not a JSON string; a second (upsert) run must change
only the mapped nested fields and keep what the application added; and the
Gate-8 read-back must compare the mapped nested values, so a wrong nested
value fails the run while a change to an unmapped sibling does not.

Before PR1:

* targets became top-level ``address_city`` / ``meta_tags`` fields;
* ``nest_json`` output was stored as a JSON string;
* ``verify_mongodb_collection`` fingerprinted top-level keys only, so a
  dotted target read back as missing on every document and a tampered nested
  value was invisible to Gate-8.

Source is SQLite (no server needed); the destination is a throwaway ``pr1e_``
database dropped afterwards.
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


pytestmark = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

RECIPE = {
    "version": 1,
    "steps": [
        {"op": "nest_json",
         "options": {"to": "tags", "fields": ["tier", "segment"], "drop": True}},
    ],
}
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 1.0},
    {"source": "name", "target": "name", "confidence": 1.0},
    {"source": "city", "target": "address.city", "confidence": 1.0},
    {"source": "zip", "target": "address.zip", "confidence": 1.0},
    {"source": "tags", "target": "meta.tags", "confidence": 1.0},
]
ROWS = [
    (1, "Ada", "Paris", "75001", "gold", "smb"),
    (2, "Grace", "Oslo", "0150", "silver", "ent"),
    (3, "Linus", "Rome", "00100", "bronze", "smb"),
]


@pytest.fixture
def mongo_db():
    import pymongo

    name = f"pr1e_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


def _source(path: Path, rows=ROWS) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT, "
            "zip TEXT, tier TEXT, segment TEXT)"
        )
        conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.close()


def _request(src: Path, db: str, sync_mode: str, *, recipe: bool = True):
    from services.shape_models import ShapeRecipe
    from src.transfer.models import EndpointConfig, TransferRequest

    shaping = (
        {"shape_recipe": RECIPE,
         "approved_shape_recipe_hash": ShapeRecipe.parse(RECIPE).recipe_hash}
        if recipe else {}
    )
    return TransferRequest(
        mappings=[dict(m) for m in (MAPPINGS if recipe else MAPPINGS[:4])],
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="customers"),
        destination=EndpointConfig(
            kind="database", format="mongodb", connection_string=MONGO_URI,
            database=db, collection="customers",
        ),
        sync_mode=sync_mode,
        stream_contracts=[{
            "name": "customers", "sync_mode": sync_mode, "primary_key": "id", "selected": True,
        }],
        **shaping,
        skip_preflight=True,
        validation_mode="balanced",
    )


def _run(src: Path, db: str, sync_mode: str, *, recipe: bool = True):
    import src.transfer.engine as engine_mod

    return engine_mod.UniversalTransferEngine().execute_tracked(
        _request(src, db, sync_mode, recipe=recipe), "pr1" + uuid.uuid4().hex[:16]
    )


def _passed(result) -> bool:
    return (result.reconciliation or {}).get("passed") is True


def test_sqlite_to_mongo_nested_targets_run1_overwrite_run2_upsert(tmp_path: Path, mongo_db):
    client, db = mongo_db
    coll = client[db]["customers"]
    src = tmp_path / "src.sqlite"
    _source(src)

    first = _run(src, db, "full_refresh_overwrite")
    assert first.success, first.error
    assert _passed(first), first.reconciliation
    docs = {d["id"]: d for d in coll.find({}, {"_id": 0})}
    assert docs[1] == {
        "id": 1, "name": "Ada",
        "address": {"city": "Paris", "zip": "75001"},
        # nest_json output is a real sub-document, not a JSON string.
        "meta": {"tags": {"tier": "gold", "segment": "smb"}},
    }, docs[1]
    assert not any(k in d for d in docs.values() for k in ("address_city", "meta_tags", "tags"))

    # The application enriches the documents between runs.
    coll.update_many({}, {"$set": {"address.geo": {"lat": 1.5, "lng": 2.5}, "app_score": 7}})

    _source(src, [
        (1, "Ada", "Lyon", "75001", "gold", "smb"),  # city changed
        *ROWS[1:],
        (4, "Margaret", "Bern", "3000", "gold", "ent"),  # new key
    ])
    second = _run(src, db, "upsert")
    assert second.success, second.error
    assert _passed(second), second.reconciliation
    docs = {d["id"]: d for d in coll.find({}, {"_id": 0})}
    assert docs[1] == {
        "id": 1, "name": "Ada", "app_score": 7,
        "address": {"city": "Lyon", "zip": "75001", "geo": {"lat": 1.5, "lng": 2.5}},
        "meta": {"tags": {"tier": "gold", "segment": "smb"}},
    }, docs[1]
    assert docs[2]["address"]["geo"] == {"lat": 1.5, "lng": 2.5} and docs[2]["app_score"] == 7
    assert docs[4] == {
        "id": 4, "name": "Margaret",
        "address": {"city": "Bern", "zip": "3000"},
        "meta": {"tags": {"tier": "gold", "segment": "ent"}},
    }, docs[4]


def _tampering_readback(update: dict):
    """Gate-8's read-back, run after ``update`` changed the written collection."""
    import services.reconciliation as recon

    original = recon.verify_mongodb_collection

    def tampered(**kwargs):
        import pymongo

        c = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        try:
            c[kwargs["database"]][kwargs["table_name"]].update_one({"id": 2}, update)
        finally:
            c.close()
        return original(**kwargs)

    return patch.object(recon, "verify_mongodb_collection", side_effect=tampered)


def _failed_on(result, column: str) -> None:
    recon = result.reconciliation or {}
    assert not _passed(result), recon
    # The read-back digest itself differs (not only a diagnostic).
    assert recon.get("source_checksum") != recon.get("target_checksum"), recon
    # ... and only the tampered mapped path is named as drifting.
    message = str(recon.get("message") or "") + str(result.error or "")
    assert column in message, message
    assert "address.zip" not in message, message


@pytest.mark.parametrize("recipe", [True, False], ids=["with_nest_json", "plain_mapping"])
def test_gate8_fails_on_a_tampered_nested_value(tmp_path: Path, mongo_db, recipe: bool):
    _client, db = mongo_db
    src = tmp_path / "src.sqlite"
    _source(src)
    clean = _run(src, db, "full_refresh_overwrite", recipe=recipe)
    assert clean.success and _passed(clean), clean.reconciliation
    with _tampering_readback({"$set": {"address.city": "Tampered"}}):
        result = _run(src, db, "full_refresh_overwrite", recipe=recipe)
    _failed_on(result, "address.city")


def test_gate8_fails_on_a_tampered_nest_json_value(tmp_path: Path, mongo_db):
    _client, db = mongo_db
    src = tmp_path / "src.sqlite"
    _source(src)
    with _tampering_readback({"$set": {"meta.tags.tier": "platinum"}}):
        result = _run(src, db, "full_refresh_overwrite")
    _failed_on(result, "meta.tags")


def test_gate8_ignores_an_unmapped_sibling_inside_the_subdocument(tmp_path: Path, mongo_db):
    _client, db = mongo_db
    src = tmp_path / "src.sqlite"
    _source(src)
    with _tampering_readback({"$set": {"address.geo": {"lat": 9}, "meta.note": "app"}}):
        result = _run(src, db, "full_refresh_overwrite")
    assert result.success, result.error
    assert _passed(result), result.reconciliation


# -- the read-back itself ---------------------------------------------------


def _expected(rows: list[dict], columns: list[str]) -> str:
    from services.reconciliation import canonical_checksum_from_iter

    return canonical_checksum_from_iter(iter(rows), columns, dest_db_type="mongodb")


def test_verify_mongodb_collection_reads_dotted_paths(mongo_db):
    from services.reconciliation_readback import verify_mongodb_collection

    client, db = mongo_db
    coll = client[db]["c"]
    coll.insert_many([
        {"id": 1, "address": {"city": "Paris", "zip": "75001", "geo": {"lat": 1}}},
        {"id": 2, "address": {"city": "Oslo", "zip": "0150"}},
    ])
    cols = ["id", "address.city", "address.zip"]
    expected = _expected(
        [{"id": 1, "address.city": "Paris", "address.zip": "75001"},
         {"id": 2, "address.city": "Oslo", "address.zip": "0150"}],
        cols,
    )

    def read() -> tuple[int, str]:
        return verify_mongodb_collection(
            connection_string=MONGO_URI, database=db, table_name="c", target_columns=cols,
        )

    count, clean = read()
    assert (count, clean) == (2, expected)
    # An unmapped sibling inside the same sub-document is not compared.
    coll.update_one({"id": 1}, {"$set": {"address.geo.lat": 99, "address.note": "x"}})
    assert read() == (2, expected)
    # A mapped nested value is.
    coll.update_one({"id": 2}, {"$set": {"address.city": "Bergen"}})
    assert read()[1] != expected


def test_verify_mongodb_collection_infers_flattened_columns(mongo_db):
    from services.reconciliation_readback import verify_mongodb_collection

    client, db = mongo_db
    client[db]["c"].insert_one({"_id": 1, "address": {"city": "Paris"}})
    count, chk = verify_mongodb_collection(
        connection_string=MONGO_URI, database=db, table_name="c",
    )
    assert count == 1
    assert chk == _expected([{"_id": 1, "address.city": "Paris"}], ["_id", "address.city"])

