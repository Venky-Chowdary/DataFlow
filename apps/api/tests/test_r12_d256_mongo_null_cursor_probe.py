"""D256: the NULL-cursor probe counts missing/NULL cursors on a MongoDB source.

It called a client factory MongoDBService never had, so every Mongo plan
showed "'MongoDBService' object has no attribute 'client_from_config'"
instead of the count of documents an incremental bound skips.
"""
from __future__ import annotations

import os
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")

_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo():
    client = pymongo.MongoClient(_URI, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - environment, not product
        pytest.skip(f"MongoDB unavailable: {type(exc).__name__}")
    return client


def test_probe_counts_missing_and_null_cursor_documents():
    from services.null_cursor_probe import probe_null_cursor_count

    client = _mongo()
    db = "d256_" + uuid.uuid4().hex[:6]
    coll = client[db]["docs"]
    coll.insert_many(
        [
            {"_id": 1, "updated_at": "2026-11-02T10:00:00Z"},
            {"_id": 2, "updated_at": None},
            {"_id": 3},
            {"_id": 4, "updated_at": "2026-11-02T11:00:00Z"},
        ]
    )
    try:
        result = probe_null_cursor_count(
            source_config={
                "type": "mongodb",
                "connection_string": _URI,
                "database": db,
            },
            source_collection="docs",
            cursor_column="updated_at",
        )
    finally:
        client.drop_database(db)
    assert result.ran, result.message
    assert result.null_count == 2
    assert "attribute" not in (result.message or "")
