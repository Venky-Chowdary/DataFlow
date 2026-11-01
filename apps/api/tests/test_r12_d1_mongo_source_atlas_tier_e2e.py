"""QA r12 D1 - MongoDB as a SOURCE failed for every database destination.

QA (production 8f7d246d, MongoDB Atlas M0 + Neon PostgreSQL): POST
/transfer/run with source_format=mongodb. Read and preflight counted every
document, then the write phase failed with "Source table `X` has no columns or
is empty". File export from the same collection worked.

Root cause (not the writer's column list - the brief's hypothesis did not hold):

1. The streaming engine snapshots a Mongo collection on one held
   ``find({}, no_cursor_timeout=True).sort(_id)`` cursor
   (``connectors/mongodb_reader.read_collection_scan_batch``). Atlas free (M0)
   and Flex tiers refuse noTimeout cursors: ``AtlasError`` 8000 "noTimeout
   cursors are disallowed in this atlas tier". File export and preflight use
   other reads, so they saw every row.
2. ``classify_error`` matched "timeout" inside "noTimeout" and called it
   transient, so ``stream._read_batch`` retried the read.
3. The retry re-entered the same held scan state, already marked started, and
   paged the dead cursor. pymongo reports a cursor whose find failed as
   exhausted, so the first page came back with no documents and no headers:
   "has no columns or is empty". (Mid-scan, the same re-entry is a silent
   truncation.)

Atlas cannot be dialled from this workstation, so the tier is reproduced on the
live local MongoDB: every ``find`` that asks for ``no_cursor_timeout=True`` is
sent to the real server in a form it rejects, and the real pymongo cursor's
failure is re-raised with Atlas's exact error (code 8000, ``AtlasError``). The
dead-cursor behaviour after the failure is the real driver's.

Every value is compared on real engines (Mongo :27017 -> PostgreSQL :5432 and
Mongo -> Mongo): NULL, a missing field, '', whitespace, unicode, nested
documents, arrays, Decimal128, ObjectId and dates, through /transfer/run with a
saved connector and through MCP ``start_transfer``, plus run 2 into the table
or collection run 1 created.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

_DB = "dataflow_test"
_T0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
_NOTES = ["", "   ", "Łukasz 🎉 山田", None, "plain", "  pad  "]


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _mongo():
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    pymongo = pytest.importorskip("pymongo")
    return pymongo.MongoClient(
        "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000, tz_aware=True
    )


@pytest.fixture
def atlas_tier(monkeypatch):
    """MongoDB answering like an Atlas M0 / Flex cluster to noTimeout cursors."""
    from pymongo.collection import Collection
    from pymongo.errors import OperationFailure

    real_find = Collection.find
    refused: list[str] = []
    atlas_msg = "noTimeout cursors are disallowed in this atlas tier"

    class _AtlasCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def sort(self, *a, **k):
            self._cursor = self._cursor.sort(*a, **k)
            return self

        def batch_size(self, *a, **k):
            self._cursor = self._cursor.batch_size(*a, **k)
            return self

        def __iter__(self):
            return self

        def __next__(self):
            try:
                return next(self._cursor)
            except OperationFailure as exc:
                raise OperationFailure(
                    atlas_msg,
                    code=8000,
                    details={"ok": 0, "errmsg": atlas_msg, "code": 8000, "codeName": "AtlasError"},
                ) from exc

        def close(self):
            self._cursor.close()

        def __getattr__(self, name):
            return getattr(self._cursor, name)

    def find(self, *args, **kwargs):
        if kwargs.get("no_cursor_timeout"):
            refused.append(self.name)
            # The real server rejects this find; the real cursor then dies.
            return _AtlasCursor(real_find(self, {"$dfAtlasTierProbe": 1}, **kwargs))
        return real_find(self, *args, **kwargs)

    monkeypatch.setattr(Collection, "find", find)
    return refused


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Source collections
# --------------------------------------------------------------------------- #


def _flat_doc(i: int) -> dict[str, Any]:
    from bson import Decimal128

    doc: dict[str, Any] = {
        "id": i,
        "name": f"n{i}",
        "note": _NOTES[i % len(_NOTES)],
        "amount": Decimal128(f"{i}.{i % 100:02d}"),
        "ref": f"{i:024x}",
        "at": _T0 + timedelta(minutes=i),
    }
    if i % 7 == 0:
        del doc["note"]  # a missing field, not a NULL
    return doc


def _nested_doc(i: int) -> dict[str, Any]:
    from bson import Decimal128, ObjectId

    return {
        "id": i,
        "oid": ObjectId(f"{i:024x}"),
        "note": _NOTES[i % len(_NOTES)],
        "price": Decimal128("12345.6789") if i % 2 else Decimal128("-0.0001"),
        "addr": {"city": ["Zürich", "", "   ", "東京"][i % 4], "zip": {"code": i}},
        "tags": [f"t{i}", "", "é"] if i % 3 else [],
        "seen": _T0 + timedelta(seconds=i),
    }


def _seed(coll: str, docs: list[dict[str, Any]]) -> None:
    db = _mongo()[_DB]
    db[coll].drop()
    db[coll].insert_many([dict(d) for d in docs])


def _drop(coll: str, table: str | None = None) -> None:
    _mongo()[_DB][coll].drop()
    if table:
        h.drop_table("postgresql", table)


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #


def _connector(client, name: str, kind: str) -> str:
    if kind == "mongodb":
        body = {"name": name, "type": "mongodb", "host": "127.0.0.1", "port": 27017,
                "connection_string": "mongodb://127.0.0.1:27017", "database": _DB}
    else:
        cfg = h.dest_config("postgresql")
        body = {"name": name, "type": "postgresql", "host": cfg["host"], "port": cfg["port"],
                "database": cfg["database"], "username": cfg["username"],
                "password": cfg["password"], "schema": "public"}
    resp = client.post("/api/v1/connectors", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


def _transfer_run(client, src_id: str, coll: str, dest: str, target: str,
                  dst_id: str | None = None, wait_s: float = 300) -> dict:
    """QA's route: POST /transfer/run, source_format=mongodb, saved connector."""
    data = {
        "source_kind": "database", "source_format": "mongodb",
        "source_connector_id": src_id, "source_database": _DB, "source_collection": coll,
        "dest_kind": "database", "dest_format": dest,
        "sync_mode": "full_refresh_overwrite", "async_mode": "true",
    }
    if dest == "postgresql":
        data.update(dest_connector_id=dst_id, dest_schema="public", dest_table=target)
    else:
        data.update(dest_connection_string="mongodb://127.0.0.1:27017",
                    dest_database=_DB, dest_collection=target)
    resp = client.post("/api/v1/transfer/run", data=data)
    assert resp.status_code == 200, resp.text
    job = h.wait_job(resp.json()["job_id"], wait_s)
    assert job.get("status") == "completed", {
        "status": job.get("status"), "error": job.get("error") or job.get("message"),
    }
    return job


def _mcp_run(client, src_id: str, dst_id: str, coll: str, target: str) -> dict:
    """MCP start_transfer -> /api/v1/copilot/confirm."""
    ok, staged = h.mcp_call(client, "start_transfer", {
        "source_connector_id": src_id, "source_table": coll,
        "dest_connector_id": dst_id, "dest_table": target,
        "sync_mode": "full_refresh_overwrite",
    })
    assert ok, staged
    assert not h.blockers_of(staged), h.blockers_of(staged)
    return h.confirm(client, staged["ack_id"], reason="r12-d1")


# --------------------------------------------------------------------------- #
# Read-back and comparison
# --------------------------------------------------------------------------- #


def _pg_rows(table: str) -> dict[int, dict[str, Any]]:
    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM public."{table}"')  # nosec B608
        names = [d[0] for d in cur.description]
        return {int(r[names.index("id")]): dict(zip(names, r)) for r in cur.fetchall()}
    finally:
        conn.close()


def _as_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _assert_flat_pg(rows: dict[int, dict[str, Any]], docs: list[dict[str, Any]]) -> None:
    assert sorted(rows) == sorted(d["id"] for d in docs)
    for doc in docs:
        row = rows[doc["id"]]
        assert row["name"] == doc["name"]
        assert row["note"] == doc.get("note"), (doc["id"], row["note"], doc.get("note"))
        assert Decimal(str(row["amount"])) == doc["amount"].to_decimal(), doc["id"]
        assert row["ref"] == doc["ref"]
        assert row["at"] == doc["at"], (doc["id"], row["at"])


def _assert_nested_pg(rows: dict[int, dict[str, Any]], docs: list[dict[str, Any]]) -> None:
    assert sorted(rows) == sorted(d["id"] for d in docs)
    for doc in docs:
        row = rows[doc["id"]]
        assert row["oid"] == str(doc["oid"]), doc["id"]
        assert row["note"] == doc["note"], (doc["id"], row["note"])
        assert Decimal(str(row["price"])) == doc["price"].to_decimal(), doc["id"]
        assert row["addr_city"] == doc["addr"]["city"], (doc["id"], row["addr_city"])
        assert int(row["addr_zip_code"]) == doc["addr"]["zip"]["code"]
        assert _as_json(row["tags"]) == doc["tags"], (doc["id"], row["tags"])
        assert row["seen"] == doc["seen"], doc["id"]


def _assert_mongo_copy(target: str, docs: list[dict[str, Any]]) -> None:
    out = {d["id"]: d for d in _mongo()[_DB][target].find({}, {"_id": 0})}
    assert sorted(out) == sorted(d["id"] for d in docs)
    for doc in docs:
        got = out[doc["id"]]
        for key, want in doc.items():
            if key == "addr":
                have = got.get("addr")
                if have is None:  # flattened carriers
                    have = {"city": got.get("addr_city"),
                            "zip": {"code": got.get("addr_zip_code")}}
                assert have == want, (doc["id"], key, have, want)
                continue
            assert key in got, (doc["id"], key, sorted(got))
            if key == "oid":
                # The row-stream Mongo->Mongo path carries a non-_id ObjectId
                # as its 24-hex text (unchanged by this fix; reported).
                assert str(got[key]) == str(want), (doc["id"], got[key])
                continue
            assert got[key] == want, (doc["id"], key, got[key], want)
        if "note" not in doc:
            assert got.get("note") is None, (doc["id"], got.get("note"))


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "n",
    # QA's 100k collection is opt-in: it takes minutes on the local engines.
    [100] + ([100_000] if os.environ.get("DATAFLOW_R12_D1_SCALE") else []),
)
def test_mongo_source_to_postgres_on_atlas_tier_transfer_run(client, atlas_tier, n):
    tag = uuid.uuid4().hex[:6]
    coll, table = f"r12d1_flat_{tag}", f"r12d1_flat_{tag}"
    docs = [_flat_doc(i) for i in range(1, n + 1)]
    _seed(coll, docs)
    try:
        src = _connector(client, f"m{tag}", "mongodb")
        dst = _connector(client, f"p{tag}", "postgresql")
        wait_s = max(300.0, n / 50)
        _transfer_run(client, src, coll, "postgresql", table, dst, wait_s=wait_s)
        assert atlas_tier, "the held Mongo scan never asked for a noTimeout cursor"
        _assert_flat_pg(_pg_rows(table), docs)

        # Run 2 into the table run 1 created, after source edits.
        db = _mongo()[_DB]
        db[coll].update_one({"id": 3}, {"$set": {"note": "edited ✓", "name": ""}})
        db[coll].update_one({"id": 4}, {"$set": {"note": None}})
        db[coll].insert_one(_flat_doc(n + 1))
        docs = [{k: v for k, v in d.items() if k != "_id"} for d in db[coll].find()]
        _transfer_run(client, src, coll, "postgresql", table, dst, wait_s=wait_s)
        _assert_flat_pg(_pg_rows(table), docs)
    finally:
        _drop(coll, table)


def test_mongo_nested_5000_to_postgres_on_atlas_tier_mcp_start_transfer(client, atlas_tier):
    tag = uuid.uuid4().hex[:6]
    coll, table = f"r12d1_nest_{tag}", f"r12d1_nest_{tag}"
    docs = [_nested_doc(i) for i in range(1, 5001)]
    _seed(coll, docs)
    try:
        src = _connector(client, f"m{tag}", "mongodb")
        dst = _connector(client, f"p{tag}", "postgresql")
        _mcp_run(client, src, dst, coll, table)
        assert atlas_tier
        _assert_nested_pg(_pg_rows(table), docs)
        # Run 2 into the created table.
        _mcp_run(client, src, dst, coll, table)
        _assert_nested_pg(_pg_rows(table), docs)
    finally:
        _drop(coll, table)


def test_mongo_nested_to_mongo_on_atlas_tier_transfer_run(client, atlas_tier):
    tag = uuid.uuid4().hex[:6]
    coll, target = f"r12d1_mm_{tag}", f"r12d1_mm_{tag}_out"
    docs = [_nested_doc(i) for i in range(1, 5001)]
    _seed(coll, docs)
    try:
        src = _connector(client, f"m{tag}", "mongodb")
        _transfer_run(client, src, coll, "mongodb", target)
        _assert_mongo_copy(target, docs)
        _transfer_run(client, src, coll, "mongodb", target)
        _assert_mongo_copy(target, docs)
    finally:
        _drop(coll)
        _drop(target)


def test_atlas_notimeout_refusal_is_not_a_transient_error():
    from pymongo.errors import OperationFailure

    from services.error_handling import classify_error

    msg = "noTimeout cursors are disallowed in this atlas tier"
    exc = OperationFailure(msg, code=8000, details={"errmsg": msg, "code": 8000,
                                                     "codeName": "AtlasError"})
    assert classify_error(exc)["retriable"] is False


def test_held_scan_retry_never_reads_a_dead_cursor_as_the_end(monkeypatch):
    """A failed held-scan page is reopened (nothing served) or refused (pages served)."""
    import src.transfer.stream as stream
    from connectors.base import ReadBatch
    from services.error_handling import HeldScanInterrupted

    calls = {"n": 0}

    def flaky(src_type, cfg, table, columns, offset, limit, **kw):
        state = kw["scan_state"]
        calls["n"] += 1
        if not state.get("started"):
            state.update(started=True, pos=0)
        if calls["n"] in (1, 4):
            raise ConnectionError("connection reset by peer")
        state["pos"] += 1
        return ReadBatch(headers=["id"], rows=[[str(state["pos"])]], offset=offset)

    monkeypatch.setattr(stream, "_read_batch_impl", flaky)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    state: dict[str, Any] = {}
    first = stream._read_batch("mongodb", {}, "t", None, 0, 10, scan_state=state)
    assert first.rows == [["1"]]  # reopened from the top, not an empty "end"
    second = stream._read_batch("mongodb", {}, "t", None, 1, 10, scan_state=state)
    assert second.rows == [["2"]]
    with pytest.raises(HeldScanInterrupted):
        stream._read_batch("mongodb", {}, "t", None, 2, 10, scan_state=state)
