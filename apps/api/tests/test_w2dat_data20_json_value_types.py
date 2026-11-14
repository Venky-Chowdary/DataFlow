"""DATA-20: a JSON file's values keep their JSON types.

Before the fix inference read every cell as text: JSON -> MongoDB stored the
string "1" as the number 1 and every float as Decimal128, and JSON -> PG
turned the string "true" into a boolean. Now a JSON string column stays
text, BOOLEAN needs real JSON booleans, a JSON float column is DOUBLE, and
only an explicit map gives Decimal128.
"""

from __future__ import annotations

import json

import pytest

from tests import w2dat_live as live

DOCS = [
    {"id": 1, "s": "1", "f": "true", "x": 1.5, "b": True},
    {"id": 2, "s": "30", "f": "false", "x": 0.1, "b": False},
    {"id": 3, "s": "2", "f": "true", "x": 3.0, "b": True},
]


def test_inference_keeps_json_types():
    from services.file_parser import FileParser

    schema = FileParser.infer_schema(DOCS, json_kinds=True)
    assert schema["s"] == "TEXT" and schema["f"] == "TEXT"
    assert schema["x"] == "DOUBLE" and schema["b"] == "BOOLEAN" and schema["id"] == "INTEGER"
    # Text records (CSV) still infer from their text.
    csv_like = [{"id": "1", "s": "1"}, {"id": "2", "s": "30"}]
    assert FileParser.infer_schema(csv_like, json_kinds=True)["s"] == "INTEGER"
    # Other typed records (an uploaded SQLite table) keep the old inference.
    assert FileParser.infer_schema(DOCS)["x"] != "DOUBLE"


def _json_run(dest, **extra):
    from src.transfer.models import EndpointConfig

    return live.run(
        EndpointConfig(kind="file", format="json"), dest, key="id",
        extra_req={"source_content": json.dumps(DOCS).encode(), "source_filename": "d.json", **extra},
    )


def _mongo():
    if not live.reachable("127.0.0.1", 27017):
        pytest.skip("MongoDB is not reachable")
    import pymongo

    return pymongo.MongoClient("127.0.0.1", 27017, serverSelectionTimeoutMS=3000)


def test_json_to_mongo_keeps_strings_and_doubles():
    client = _mongo()
    coll = f"w2dat_d20m_{live.uid()}"
    try:
        res = _json_run(live.ep_mongo(coll))
        assert res.success, res.error
        docs = sorted(client["dataflow"][coll].find({}, {"_id": 0}), key=lambda d: d["id"])
        assert [(d["s"], d["f"], d["x"], d["b"]) for d in docs] == [
            ("1", "true", 1.5, True), ("30", "false", 0.1, False), ("2", "true", 3.0, True)]
        assert all(type(d["x"]) is float and type(d["s"]) is str for d in docs)
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()


def test_an_explicit_decimal_map_gives_decimal128():
    from bson.decimal128 import Decimal128

    client = _mongo()
    coll = f"w2dat_d20d_{live.uid()}"
    maps = [{"source": c, "target": c, "confidence": 1.0, "approved": True, "user_override": True}
            for c in ("id", "s", "f", "b")]
    maps.append({"source": "x", "target": "x", "target_type": "DECIMAL(10,2)", "transform": "decimal",
                 "confidence": 1.0, "approved": True, "user_override": True})
    try:
        res = _json_run(live.ep_mongo(coll), mappings=maps)
        assert res.success, res.error
        docs = list(client["dataflow"][coll].find({}, {"_id": 0}))
        assert docs and all(isinstance(d["x"], Decimal128) for d in docs)
    finally:
        client["dataflow"].drop_collection(coll)
        client.close()


def test_json_to_postgres_keeps_the_string_true():
    live.need_pg()
    table = f"w2dat_d20p_{live.uid()}"
    try:
        res = _json_run(live.ep_pg(table))
        assert res.success, res.error
        types = dict(live.pg(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table,)))
        assert types["f"] == "text" and types["s"] == "text"
        assert types["b"] == "boolean" and types["x"] == "double precision"
        assert live.pg(f'SELECT f, s FROM "{table}" ORDER BY id') == [("true", "1"), ("false", "30"), ("true", "2")]
    finally:
        live.drop_pg(table)
