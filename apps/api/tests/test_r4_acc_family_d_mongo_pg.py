"""QA round 4, Family D acceptance on real engines (D197 / D194 / D190).

QA's acceptance line, verbatim: "a collection with ObjectId _id plus a string
field holding 't','f','true','x' loads with a cell-by-cell match" (Mongo -> new
PostgreSQL table, full_refresh_overwrite, strict), including under a signed
CAST_AND_CONTINUE contract. BOOLEAN only if every value is a real BSON boolean.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, live
MongoDB on 127.0.0.1:27017 and live PostgreSQL (``PG_*`` env, default
127.0.0.1:5432 dataflow/dataflow). Skipped when either is unreachable.
"""

from __future__ import annotations

import uuid

import pytest

pymongo = pytest.importorskip("pymongo")
from bson import ObjectId  # noqa: E402

from test_d190_d194_d197_declared_string_types_e2e import (  # noqa: E402,F401
    _drop,
    _mongo_source,
    _pg,
    _pg_column_types,
    _pg_connector,
    _pg_rows,
    _run,
    _up,
    transfer_client,
)

_OIDS = sorted(ObjectId() for _ in range(7))
# ``flag`` is QA's field: strings 't','f','true','x' (plus NULL / '' / missing).
# ``active`` is every-value-a-real-BSON-boolean (NULL allowed): BOOLEAN.
# ``mixed`` holds a real boolean and the string 'true': not BOOLEAN.
_DOCS = [
    {"_id": _OIDS[0], "flag": "t", "active": True, "mixed": "true", "code": "00123", "n": 7},
    {"_id": _OIDS[1], "flag": "f", "active": False, "mixed": "x", "code": "20240101", "n": 2**40},
    {"_id": _OIDS[2], "flag": "true", "active": True, "mixed": "f"},
    {"_id": _OIDS[3], "flag": "x", "active": None, "mixed": "t"},
    {"_id": _OIDS[4], "flag": None, "active": False, "mixed": None},
    {"_id": _OIDS[5], "flag": "", "active": True, "mixed": " "},
    {"_id": _OIDS[6], "active": False, "mixed": "Zoë"},  # flag missing
]
_MIXED_DOCS = [{"_id": ObjectId(), "mixed": True}]


@pytest.fixture
def collection():
    if not _up("127.0.0.1", 27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    name = "r4d_" + uuid.uuid4().hex[:8]
    coll = client["dataflow_test"][name]
    coll.insert_many([dict(d) for d in _DOCS])
    try:
        yield coll
    finally:
        coll.drop()
        client.close()


def _expected(cols):
    return [tuple(str(d["_id"]) if c == "_id" else d.get(c) for c in cols) for d in _DOCS]


_SIGNED = {
    "approved_by": "qa@example.com",
    "reason": "D197/D194 acceptance under a signed contract",
    "execution_policy": "CAST_AND_CONTINUE",
}


@pytest.mark.parametrize(
    "signed,tz",
    [(False, ""), (True, ""), (False, "UTC")],
    ids=["strict", "signed_cast_and_continue", "strict_source_timezone_utc"],
)
def test_objectid_and_tf_true_x_strings_cell_by_cell(transfer_client, collection, signed, tz):
    _pg().close()
    table = collection.name + ("_s" if signed else "_u") + ("_tz" if tz else "")
    _drop(table)
    args = dict(
        source_connector_id=_mongo_source(collection),
        source_table=collection.name,
        dest_connector_id=_pg_connector("r4d"),
        dest_table=table,
        sync_mode="full_refresh_overwrite",
    )
    if signed:
        args["risk_acceptance"] = dict(_SIGNED)
    if tz:
        # D190: the timezone option touches only real temporal columns.
        args["source_timezone"] = tz
    try:
        plan, _job = _run(transfer_client, **args)
        by_source = {m["source"]: m for m in plan["engine_mappings"]}
        assert by_source["_id"]["source_type"] == "OBJECTID", by_source["_id"]
        types = _pg_column_types(table)
        assert types["_id"] in {"character varying", "character", "text"}, types
        assert types["flag"] in {"character varying", "text"}, types
        assert types["active"] == "boolean", types
        assert types["mixed"] in {"character varying", "text"}, types
        assert types["code"] in {"character varying", "text"}, types
        assert types["n"] in {"integer", "bigint"}, types
        cols = ["_id", "flag", "active", "mixed", "code", "n"]
        assert _pg_rows(table, cols, "_id") == _expected(cols)

        # Run 2 into the table run 1 created: same types, same cells.
        _run(transfer_client, **args)
        assert _pg_column_types(table) == types
        assert _pg_rows(table, cols, "_id") == _expected(cols)
    finally:
        _drop(table)


def test_one_real_boolean_among_strings_is_not_boolean(transfer_client, collection):
    """A field with a real BSON true and strings 'true'/'x' must stay text."""
    _pg().close()
    collection.insert_many([dict(d) for d in _MIXED_DOCS])
    table = collection.name + "_m"
    _drop(table)
    try:
        _run(
            transfer_client,
            source_connector_id=_mongo_source(collection),
            source_table=collection.name,
            dest_connector_id=_pg_connector("r4dm"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
        )
        types = _pg_column_types(table)
        assert types["mixed"] in {"character varying", "text"}, types
        got = dict(_pg_rows(table, ["_id", "mixed"], "_id"))
        for d in _DOCS:
            assert got[str(d["_id"])] == d.get("mixed")
        # The single BSON boolean lands as PostgreSQL's boolean text, distinct
        # from the string 't' another row holds (it used to land as 't').
        assert got[str(_MIXED_DOCS[0]["_id"])] == "true"
    finally:
        _drop(table)
