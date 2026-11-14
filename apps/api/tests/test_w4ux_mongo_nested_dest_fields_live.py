"""Nested MongoDB fields in the Map destination picker (W4).

``mapping_dest_catalog`` / ``endpoint_intelligence`` described an existing
MongoDB destination by its top-level fields only (``address`` as one JSON
value), so a user could not pick or drop onto ``address.city``. The catalog
now lists every nested path with its stored BSON type (``$type``), placed
after its parent, and a mapping onto such a path writes inside the
sub-document (``connectors.mongo_field_paths``) and passes Gate-8.
Live against local MongoDB.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or "mongodb://localhost:27017"


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

SEED = [
    {
        "_id": 90000 + i,
        "name": f"Seed {i}",
        "address": {"city": "Leeds", "zip": "LS1", "geo": {"lat": 53.8, "lng": -1.5}},
        "tags": ["a"],
        "score": 1.5,
        "visits": 3,
    }
    for i in range(1, 4)
]


@pytest.fixture
def seeded():
    import pymongo

    client = pymongo.MongoClient(MONGO_URI)
    db_name = f"w4ux_nested_{uuid.uuid4().hex[:8]}"
    coll = client[db_name]["customers"]
    coll.insert_many([dict(d) for d in SEED])
    # One document stores address.city as a number: the picker must say "mixed".
    coll.insert_one({"_id": 90009, "name": "Odd", "address": {"city": 7}})
    try:
        yield client, db_name
    finally:
        client.drop_database(db_name)


def _endpoint(db_name: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="mongodb", connection_string=MONGO_URI, database=db_name, collection="customers")


@live
def test_catalog_lists_nested_paths_with_bson_types(seeded):
    from services.mapping_dest_catalog import read_destination_column_catalog

    _client, db_name = seeded
    catalog = read_destination_column_catalog(_endpoint(db_name))
    cols = {c["name"]: c for c in catalog["columns"]}
    names = [c["name"] for c in catalog["columns"]]
    # Nested paths are offered, each right after its parent.
    assert names.index("address") < names.index("address.city") < names.index("address.geo") < names.index("address.geo.lat")
    assert names.index("address.geo.lng") < names.index("tags")
    assert cols["address.zip"]["bson_type"] == "string"
    assert cols["address.geo.lat"]["bson_type"] == "double"
    assert cols["address.geo"]["bson_type"] == "object"
    # Two documents disagree on address.city: the picker says so.
    assert cols["address.city"]["bson_type"] == "mixed: int | string"
    # Top-level fields carry their stored BSON type too.
    assert cols["visits"]["bson_type"] == "int" and cols["score"]["bson_type"] == "double"
    assert cols["address.city"]["nested"] is True and cols["address.city"]["parent"] == "address"
    # A sample-derived path's nullability is unmeasured, never "optional".
    assert cols["address.city"]["nullable"] is None and cols["address.city"]["required"] is None
    assert catalog["can_add_columns"] is True


def test_sampling_skips_paths_the_path_rules_refuse():
    from services.mongo_nested_fields import sample_field_paths

    paths = sample_field_paths([{"_id": 1, "a": {"b": 1, "_id": 2, "$x": 3}, "c.d": 4, "e": {}}])
    assert paths == ["_id", "a", "a.b", "e"]


@live
def test_a_mapping_onto_a_nested_path_writes_inside_the_subdocument(seeded, tmp_path: Path, monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return self.update_job_status(job_id, (self.get_job(job_id) or {}).get("status", "running"), **fields)

    fake = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    client, db_name = seeded
    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, city TEXT)")
        conn.executemany("INSERT INTO people VALUES (?, ?, ?)", [(i, f"Person {i}", f"City {i}") for i in range(1, 21)])
    conn.close()
    request = TransferRequest(
        mappings=[
            {"source": "id", "target": "_id", "confidence": 1.0},
            {"source": "name", "target": "name", "confidence": 1.0},
            {"source": "city", "target": "address.city", "confidence": 1.0},
        ],
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=_endpoint(db_name),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "w4uxnest" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    coll = client[db_name]["customers"]
    assert coll.count_documents({}) == 20
    doc = coll.find_one({"_id": 7})
    assert doc["address"] == {"city": "City 7"}, doc
    assert "address.city" not in doc, "no literal dotted top-level key"
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
