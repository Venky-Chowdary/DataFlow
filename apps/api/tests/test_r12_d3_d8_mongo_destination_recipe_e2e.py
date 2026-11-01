"""QA r12 D3 and D8 - a transform recipe on a MongoDB destination.

D3 (production 8f7d246d): PG -> MongoDB with a recipe that casts ``id`` to text
stored ``id`` as an int in every document.

Root cause: ``cast_column`` declares a column's logical type, but
``services/shape_apply.recipe_output_schema`` re-inferred the carrier of every
column a step writes from the shaped values. ``1`` cast to text is the string
"1", which infers INTEGER, so the writer was handed BIGINT and the Mongo writer
re-typed the value to an int. The SQL writer had the same defect (PG -> PG
created ``id BIGINT``); it is one schema rule for every destination, fixed once.

D8: the transform proof re-read every destination through a SQLAlchemy engine,
and MongoDB has none: "SQLAlchemy dialect/driver for 'mongodb' is not
available" (or "SQL identifier is empty" when the collection was named by
``dest_collection``). The proof now re-reads a Mongo destination through the
Mongo reader's projection.

Real engines: PostgreSQL :5432 and MongoDB :27017, through POST /transfer/run.
Values cover NULL, '', whitespace and unicode, plus run 2 into the collection
(or table) run 1 created, and a tampered write the proof must catch.
"""

from __future__ import annotations

import json
import socket
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_DB = "dataflow_test"
_RECIPE = {
    "version": 1,
    "steps": [
        {"op": "cast_column", "column": "id", "options": {"to_type": "text"}},
        {"op": "cast_column", "column": "qty", "options": {"to_type": "integer"}},
        {"op": "case", "column": "name", "options": {"mode": "upper"}},
    ],
}
_NAMES = [None, "", "   ", "Łukasz 🎉 山田", "ñandú", "plain"]


def _mongo():
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError:
        pytest.skip("MongoDB not reachable on :27017")
    pymongo = pytest.importorskip("pymongo")
    return pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c


def _source_rows(n: int, offset: int = 0) -> list[tuple]:
    return [
        (i, _NAMES[i % len(_NAMES)], str(i * 3 + offset), Decimal(f"{i}.{i % 100:02d}"))
        for i in range(1, n + 1)
    ]


def _make_source(table: str, rows: list[tuple]) -> None:
    h.drop_table("postgresql", table)
    h.execute("postgresql", f'CREATE TABLE "{table}" (id int PRIMARY KEY, name text, '
                            "qty text, amount numeric(10,2))")
    _load(table, rows)


def _load(table: str, rows: list[tuple]) -> None:
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'DELETE FROM "{table}"')  # nosec B608
        cur.executemany(f'INSERT INTO "{table}" VALUES (%s, %s, %s, %s)', rows)  # nosec B608
    finally:
        conn.close()


def _run(client, table: str, dest: str, target: str, name_field: str = "dest_collection") -> dict:
    cfg = h.dest_config("postgresql")
    data = dict(
        source_kind="database", source_format="postgresql", source_host=cfg["host"],
        source_port=str(cfg["port"]), source_database=cfg["database"],
        source_username=cfg["username"], source_password=cfg["password"],
        source_schema="public", source_table=table, async_mode="false",
        sync_mode="full_refresh_overwrite", shape_recipe_json=json.dumps(_RECIPE),
    )
    if dest == "mongodb":
        data.update(dest_kind="database", dest_format="mongodb",
                    dest_connection_string="mongodb://127.0.0.1:27017",
                    dest_database=_DB, **{name_field: target})
    else:
        data.update(dest_kind="database", dest_format="postgresql", dest_host=cfg["host"],
                    dest_port=str(cfg["port"]), dest_database=cfg["database"],
                    dest_username=cfg["username"], dest_password=cfg["password"],
                    dest_schema="public", dest_table=target)
    resp = client.post("/api/v1/transfer/run", data=data)
    assert resp.status_code == 200, resp.text[:2000]
    body = resp.json()
    assert body.get("success") is True, str(body)[:2000]
    return body


def _expected(rows: list[tuple]) -> dict[str, dict[str, Any]]:
    return {
        str(i): {"name": None if name is None else name.upper(), "qty": int(qty),
                 "amount": amount}
        for i, name, qty, amount in rows
    }


def _assert_mongo(target: str, rows: list[tuple]) -> None:
    docs = list(_mongo()[_DB][target].find({}, {"_id": 0}))
    want = _expected(rows)
    assert len(docs) == len(want)
    for doc in docs:
        # D3: the id the recipe cast to text is a BSON string, never an int.
        assert isinstance(doc["id"], str), doc
        exp = want[doc["id"]]
        assert doc.get("name") == exp["name"], doc
        assert isinstance(doc["qty"], int) and doc["qty"] == exp["qty"], doc
        assert Decimal(str(doc["amount"].to_decimal() if hasattr(doc["amount"], "to_decimal")
                           else doc["amount"])) == exp["amount"], doc
    assert sorted(d["id"] for d in docs) == sorted(want)


def _assert_proof_verified(body: dict) -> None:
    proof = (body.get("reconciliation") or {}).get("transform_applied") or {}
    # D8: the proof re-read the Mongo destination; no SQL dialect involved.
    assert proof.get("status") == "verified", proof
    assert int(proof.get("rows") or 0) > 0, proof
    assert "dialect/driver" not in json.dumps(proof)


@pytest.mark.parametrize("name_field", ["dest_table", "dest_collection"])
def test_pg_to_mongo_cast_column_stores_text_and_the_proof_reads_mongo(client, name_field):
    tag = uuid.uuid4().hex[:6]
    table = target = f"r12d3_{tag}"
    rows = _source_rows(60)
    _make_source(table, rows)
    try:
        body = _run(client, table, "mongodb", target, name_field)
        _assert_mongo(target, rows)
        _assert_proof_verified(body)
        # Run 2 into the collection run 1 created, after source edits.
        rows2 = _source_rows(65, offset=1)
        _load(table, rows2)
        body = _run(client, table, "mongodb", target, name_field)
        _assert_mongo(target, rows2)
        _assert_proof_verified(body)
    finally:
        h.drop_table("postgresql", table)
        _mongo()[_DB][target].drop()


def test_pg_to_pg_cast_column_creates_text_carrier(client):
    tag = uuid.uuid4().hex[:6]
    table, target = f"r12d3s_{tag}", f"r12d3s_{tag}_out"
    rows = _source_rows(30)
    _make_source(table, rows)
    try:
        for _ in range(2):  # run 2 into the table run 1 created
            _run(client, table, "postgresql", target)
            types = dict(h.query("postgresql", (
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = '{target}'")))
            assert types["id"] == "text", types
            assert types["qty"] in {"bigint", "integer"}, types
            got = {r[0]: r[1:] for r in h.query(
                "postgresql", f'SELECT id, name, qty, amount FROM "{target}"')}  # nosec B608
            want = _expected(rows)
            assert sorted(got) == sorted(want)
            for key, (name, qty, amount) in got.items():
                assert (name, int(qty), Decimal(amount)) == (
                    want[key]["name"], want[key]["qty"], want[key]["amount"]), key
    finally:
        h.drop_table("postgresql", table)
        h.drop_table("postgresql", target)


def test_mongo_destination_proof_catches_a_tampered_write(client, monkeypatch):
    """The Mongo read-back is a real proof: a value changed on the way in fails it."""
    from pymongo.collection import Collection

    tag = uuid.uuid4().hex[:6]
    table = target = f"r12d8_{tag}"
    rows = _source_rows(20)
    _make_source(table, rows)
    real_insert_many = Collection.insert_many
    real_bulk_write = Collection.bulk_write

    def _tamper(doc):
        if isinstance(doc, dict) and doc.get("id") == "5":
            doc["name"] = "TAMPERED"
        return doc

    def insert_many(self, documents, *a, **k):
        if self.name == target:
            documents = [_tamper(dict(d)) for d in documents]
        return real_insert_many(self, documents, *a, **k)

    def bulk_write(self, requests, *a, **k):
        if self.name == target:
            for req in requests:
                for attr in ("_doc", "_replacement"):
                    if isinstance(getattr(req, attr, None), dict):
                        _tamper(getattr(req, attr))
        return real_bulk_write(self, requests, *a, **k)

    monkeypatch.setattr(Collection, "insert_many", insert_many)
    monkeypatch.setattr(Collection, "bulk_write", bulk_write)
    try:
        cfg = h.dest_config("postgresql")
        resp = client.post("/api/v1/transfer/run", data=dict(
            source_kind="database", source_format="postgresql", source_host=cfg["host"],
            source_port=str(cfg["port"]), source_database=cfg["database"],
            source_username=cfg["username"], source_password=cfg["password"],
            source_schema="public", source_table=table, async_mode="false",
            sync_mode="full_refresh_overwrite", shape_recipe_json=json.dumps(_RECIPE),
            dest_kind="database", dest_format="mongodb",
            dest_connection_string="mongodb://127.0.0.1:27017",
            dest_database=_DB, dest_collection=target))
        body = resp.json()
        error = str((body.get("detail") or {}).get("error") or "")
        # The proof re-read the Mongo collection, found key 5 off the recipe,
        # failed the run and dropped the collection it created.
        assert resp.status_code == 422, json.dumps(body)[:1500]
        assert "Transform-applied proof failed" in error, error
        assert "changed: 5" in error, error
        assert "dialect/driver" not in error
    finally:
        h.drop_table("postgresql", table)
        _mongo()[_DB][target].drop()


def test_recipe_output_schema_honours_every_cast_type():
    from services.shape_apply import recipe_output_schema
    from services.shape_models import ShapeRecipe

    cols = ["a", "b", "c", "d", "e", "f", "g"]
    recipe = ShapeRecipe.parse({"steps": [
        {"op": "cast_column", "column": "a", "options": {"to_type": "text"}},
        {"op": "cast_column", "column": "b", "options": {"to_type": "integer"}},
        {"op": "cast_column", "column": "c", "options": {"to_type": "decimal",
                                                         "precision": 12, "scale": 3}},
        {"op": "cast_column", "column": "d", "options": {"to_type": "boolean"}},
        {"op": "cast_column", "column": "e", "options": {"to_type": "date"}},
        {"op": "cast_column", "column": "f", "options": {"to_type": "timestamp"}},
        # a later step on the column re-opens inference
        {"op": "cast_column", "column": "g", "options": {"to_type": "text"}},
        {"op": "trim", "column": "g"},
    ]}, source_columns=cols)
    rows = [{"a": "1", "b": "7", "c": "1.5", "d": "true", "e": "2026-01-02",
             "f": "2026-01-02T03:04:05", "g": "2"}]
    prior = {"a": "BIGINT", "b": "VARCHAR", "c": "VARCHAR", "d": "VARCHAR",
             "e": "VARCHAR", "f": "VARCHAR", "g": "VARCHAR"}
    out = recipe_output_schema(recipe, cols, rows, prior)
    assert out["a"] == "VARCHAR"
    assert out["b"].upper() in {"BIGINT", "INTEGER"}
    assert out["c"] == "DECIMAL(12,3)"
    assert out["d"].upper() == "BOOLEAN"
    assert out["e"].upper() == "DATE"
    assert out["f"].upper().startswith("TIMESTAMP")
    assert out["g"].upper() in {"BIGINT", "INTEGER"}, out["g"]


@pytest.mark.parametrize("dest", ["postgresql", "mongodb"])
def test_mongo_source_recipe_proof_reads_the_named_collection(client, dest):
    """D8 from the source side: the proof re-reads the Mongo source by its
    ``source_collection`` and run database (it read ``table`` only and failed
    "collection names cannot be empty", then built a SQL ref for Mongo)."""
    tag = uuid.uuid4().hex[:6]
    coll, target = f"r12d8s_{tag}", f"r12d8s_{tag}_out"
    docs = []
    for i in range(1, 41):
        doc: dict[str, Any] = {"id": i, "name": _NAMES[i % len(_NAMES)], "n": i}
        if i % 7 == 0:
            del doc["name"]  # missing, not NULL
        docs.append(doc)
    db = _mongo()[_DB]
    db[coll].drop()
    db[coll].insert_many([dict(d) for d in docs])
    recipe = {"version": 1, "steps": [
        {"op": "case", "column": "name", "options": {"mode": "upper"}},
        {"op": "cast_column", "column": "id", "options": {"to_type": "text"}},
    ]}
    data = dict(source_kind="database", source_format="mongodb",
                source_connection_string="mongodb://127.0.0.1:27017",
                source_database=_DB, source_collection=coll, async_mode="false",
                sync_mode="full_refresh_overwrite", shape_recipe_json=json.dumps(recipe))
    if dest == "postgresql":
        cfg = h.dest_config("postgresql")
        data.update(dest_kind="database", dest_format="postgresql", dest_host=cfg["host"],
                    dest_port=str(cfg["port"]), dest_database=cfg["database"],
                    dest_username=cfg["username"], dest_password=cfg["password"],
                    dest_schema="public", dest_table=target)
    else:
        data.update(dest_kind="database", dest_format="mongodb",
                    dest_connection_string="mongodb://127.0.0.1:27017",
                    dest_database=_DB, dest_table=target)
    try:
        for _ in range(2):  # run 2 into what run 1 created
            resp = client.post("/api/v1/transfer/run", data=data)
            assert resp.status_code == 200, resp.text[:1500]
            body = resp.json()
            _assert_proof_verified(body)
            if dest == "postgresql":
                got = {r[0]: r[1] for r in h.query(
                    "postgresql", f'SELECT id, name FROM "{target}"')}  # nosec B608
            else:
                got = {d["id"]: d.get("name") for d in db[target].find({}, {"_id": 0})}
            want = {str(d["id"]): (d.get("name").upper() if d.get("name") is not None else None)
                    for d in docs}
            assert got == want
    finally:
        db[coll].drop()
        db[target].drop()
        h.drop_table("postgresql", target)
