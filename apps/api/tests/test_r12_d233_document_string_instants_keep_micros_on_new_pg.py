"""D233: ISO-string timestamps in a document source are not BSON millisecond dates.

QA on prod: Mongo / Elasticsearch fields holding ISO strings with six
fractional digits planned a new PostgreSQL table as TIMESTAMPTZ(3), so every
row quarantined ("fractional seconds exceed destination TIMESTAMPTZ(3)").

Live: MongoDB and PostgreSQL (PGPASSWORD) on 127.0.0.1; skipped otherwise.
"""
from __future__ import annotations

import datetime as dt
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")

from tests.test_d147_d150_mongo_mcp_plan_route import _destination, _up, store  # noqa: E402,F401


@pytest.fixture
def collection():
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    coll = client["dataflow_test"]["d233_" + uuid.uuid4().hex[:8]]
    try:
        yield coll
    finally:
        coll.drop()
        client.close()


def _plan(store, coll):  # noqa: F811
    from src.ai.copilot import transfer_tools

    src = store.create_connector({
        "name": "d233-mongo-" + uuid.uuid4().hex[:6], "type": "mongodb", "role": "source",
        "host": "127.0.0.1", "port": 27017, "database": "dataflow_test", "ssl": False,
        "workspace_id": ""})
    dst = _destination(store, "postgresql")
    result = transfer_tools.plan_transfer(
        source_connector_id=str(src.id), source_table=coll.name,
        dest_connector_id=str(dst.id), dest_table=coll.name + "_out",
        sync_mode="full_refresh_overwrite", validation_mode="strict")
    assert result.success, result.error
    return {m["source"]: m for m in result.output["engine_mappings"]}


def test_iso_string_microseconds_plan_a_six_digit_column(store, collection):  # noqa: F811
    collection.insert_many(
        [{"_id": i, "ts": f"2024-03-10T02:30:00.01234{i}"} for i in range(1, 6)]
    )
    ts = _plan(store, collection)["ts"]
    assert ts["target_type"].upper() == "TIMESTAMPTZ(6)", ts


def test_bson_dates_still_plan_the_millisecond_column(store, collection):  # noqa: F811
    t0 = dt.datetime(2024, 3, 10, 2, 30, 0, 12000, tzinfo=dt.timezone.utc)
    collection.insert_many([{"_id": i, "ts": t0} for i in range(1, 6)])
    ts = _plan(store, collection)["ts"]
    assert ts["target_type"].upper() == "TIMESTAMPTZ(3)", ts
