"""D147 — MongoDB source introspection must describe a schemaless collection.

* A field a document omits is NULL for that row. Introspection reported any
  field present in the first sampled document as ``nullable=False``, so a
  sparse field (or one an empty sub-document flattens to) read as required.
  Only ``_id`` is.
* A nested BSON date (``customer.since``) is a BSON date like a top-level one.
  The endpoint sample read the carriers before expanding nested documents, so
  the flattened column was typed from its stringified, zoneless text
  (TIMESTAMP beside the top-level TIMESTAMPTZ).
"""

from __future__ import annotations

import datetime as dt
import socket
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")

T0 = dt.datetime(2024, 1, 2, 3, 4, 5, 123000)


@pytest.fixture()
def collection():
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError:
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    name = "d147_" + uuid.uuid4().hex[:8]
    coll = client["dataflow_test"][name]
    coll.insert_many(
        [
            {"_id": 1, "created_at": T0, "customer": {"tier": "x", "since": T0}, "payload_k": "a"},
            {"_id": 2, "created_at": T0, "customer": {}},
            {"_id": 3, "created_at": T0, "customer": {"tier": "z", "since": T0}, "payload_k": "c"},
        ]
    )
    try:
        yield name
    finally:
        coll.drop()
        client.close()


def test_fields_a_document_omits_are_nullable_and_only_id_is_required(collection):
    from services.schema_introspect import _introspect_mongodb

    result = _introspect_mongodb(
        host="127.0.0.1", port=27017, database="dataflow_test", table=collection, reader_cfg={}
    )
    nullable = {c["name"]: c["nullable"] for c in result["columns"]}
    assert nullable["_id"] is False
    for sparse in ("payload_k", "customer_tier", "customer_since", "created_at", "customer"):
        assert nullable[sparse] is True, sparse


def test_nested_bson_date_is_typed_like_a_top_level_one(collection):
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database", format="mongodb", host="127.0.0.1", port=27017,
        database="dataflow_test", collection=collection, table=collection,
    )
    endpoint.extra = {"introspect_purpose": "source", "introspect_strict": True}
    schema = introspect_endpoint(endpoint)["schema"]
    assert schema["created_at"] == "TIMESTAMPTZ"
    assert schema["customer_since"] == schema["created_at"]
