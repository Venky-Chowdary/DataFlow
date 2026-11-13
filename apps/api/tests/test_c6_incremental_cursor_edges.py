"""C6 — incremental cursor edge cases are handled or refused with a clear message.

Real engine path (``execute_tracked``, two incremental runs per case):

* **Leading-zero text keys.** A string cursor is ordered as text by its source.
  One width of zero-padded digits keeps numeric order and is handled; mixed
  widths (``'9'``/``'10'``, ``'007'``/``'0010'``) are refused by name. SQL
  sources already did this; a MongoDB string field did not, and silently
  skipped ``'10'`` behind the watermark ``'9'``.
* **MongoDB ObjectId cursors.** Ordered by MongoDB by their 12 bytes, which the
  24-hex text spells in the same order. Handled, including an ObjectId whose hex
  reads like a number, and never refused by the string-field rule above.
* **PostgreSQL replica sources.** A hot standby's snapshot horizon follows WAL
  replay; the run cannot show its stored horizon and its reads share a replay
  point, so the route takes the re-read window and says why instead of claiming
  the snapshot-horizon guarantee. (No standby runs here: the live leg answers
  ``pg_is_in_recovery()`` as a standby does; the plan is unit-proven too.)
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_commit_safe_review_round2 import _connect, isolated  # noqa: E402,F401

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _sql(statement: str, fetch: bool = False):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _pg_endpoint(schema: str, table: str) -> EndpointConfig:
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], schema=schema, table=table,
        username=c["username"], password=c["password"],
    )


def _run(store, source: EndpointConfig, dest: EndpointConfig, cursor: str, pk: str, **contract):
    request = TransferRequest(
        source=source, destination=dest, sync_mode="incremental_append",
        stream_contracts=[{
            "name": source.table or source.collection, "cursor_field": cursor,
            "sync_mode": "incremental_append", "selected": True, "primary_key": pk, **contract,
        }],
        skip_preflight=True, validation_mode="strict",
    )
    job_id = "c6" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.fixture()
def pg_schema():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    schema = "c6_" + uuid.uuid4().hex[:8]
    _sql(f'CREATE SCHEMA "{schema}"')
    try:
        yield schema
    finally:
        _sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture()
def mongo_db(pg_schema):
    pymongo = pytest.importorskip("pymongo")
    from tests.helpers.live_env import mongo_up

    if not mongo_up(MONGO_URI):
        pytest.skip("live MongoDB not reachable")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    name = pg_schema  # unique per test, and dropped below
    try:
        yield client[name]["src"], name, pg_schema
    finally:
        client.drop_database(name)
        client.close()


def _mongo_source(db: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mongodb", connection_string=MONGO_URI, database=db, collection="src",
    )


def _dest_count(schema: str) -> int:
    return int(_sql(f'SELECT count(*) FROM "{schema}".dst', fetch=True)[0][0])


# --------------------------------------------------------------------------
# Leading-zero text keys
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "first,later,high,low",
    [(["1", "9"], ["10"], "10", "9"), (["007", "009"], ["0010"], "0010", "009")],
)
def test_mongo_string_cursor_of_mixed_widths_is_refused_by_name(
    isolated, mongo_db, first, later, high, low  # noqa: F811
):
    coll, db, schema = mongo_db
    coll.insert_many([{"code": v} for v in first])
    run1 = _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "code", "_id")
    assert run1.success, run1.error
    coll.insert_many([{"code": v} for v in later])
    run2 = _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "code", "_id")
    assert not run2.success
    error = str(run2.error)
    assert "Cursor 'code' is a string field" in error, error
    assert f"as text '{high}' sorts before '{low}'" in error, error
    assert _dest_count(schema) == len(first)  # nothing was read past the refusal


def test_mongo_zero_padded_string_cursor_of_one_width_is_handled(isolated, mongo_db):  # noqa: F811
    coll, db, schema = mongo_db
    coll.insert_many([{"code": v} for v in ("001", "002", "009")])
    assert _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "code", "_id").success
    coll.insert_many([{"code": v} for v in ("010", "011")])
    run2 = _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "code", "_id")
    assert run2.success, run2.error
    assert _dest_count(schema) == 5


def test_pg_text_cursor_leading_zero_keys_handled_or_refused(isolated, pg_schema):  # noqa: F811
    schema = pg_schema
    _sql(f'CREATE TABLE "{schema}".src (code TEXT PRIMARY KEY, v INT)')
    _sql(f"INSERT INTO \"{schema}\".src VALUES ('001', 1), ('009', 2)")
    src, dst = _pg_endpoint(schema, "src"), _pg_endpoint(schema, "dst")
    assert _run(isolated, src, dst, "code", "code").success
    _sql(f"INSERT INTO \"{schema}\".src VALUES ('010', 3)")
    assert _run(isolated, src, dst, "code", "code").success
    assert _dest_count(schema) == 3
    _sql(f"INSERT INTO \"{schema}\".src VALUES ('9999', 4)")  # a second width
    refused = _run(isolated, src, dst, "code", "code")
    assert not refused.success and "Zero-padded values of one width" in str(refused.error)


# --------------------------------------------------------------------------
# MongoDB ObjectId cursors
# --------------------------------------------------------------------------


def test_mongo_objectid_cursor_is_handled_even_when_its_hex_reads_as_a_number(
    isolated, mongo_db  # noqa: F811
):
    from bson import ObjectId

    coll, db, schema = mongo_db
    # '6554e1240…' and '6554e1250…' parse as decimal numbers with an exponent;
    # '6554e124a…' does not. MongoDB orders all three by their bytes.
    coll.insert_many([
        {"_id": ObjectId("6554e124a000000000000001"), "v": 1},
        {"_id": ObjectId("6554e1250000000000000002"), "v": 2},
    ])
    assert _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "_id", "_id").success
    coll.insert_many([
        {"_id": ObjectId("6554e1260000000000000003"), "v": 3},
        {"_id": ObjectId("6554e126b000000000000004"), "v": 4},
    ])
    run2 = _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "_id", "_id")
    assert run2.success, run2.error
    run3 = _run(isolated, _mongo_source(db), _pg_endpoint(schema, "dst"), "_id", "_id")
    assert run3.success, run3.error
    assert _dest_count(schema) == 4
    ids = sorted(r[0] for r in _sql(f'SELECT "_id" FROM "{schema}".dst', fetch=True))
    assert len(set(ids)) == 4, ids  # no document read twice, none skipped


def test_objectid_field_is_not_judged_by_the_string_rule(mongo_db):
    from bson import ObjectId

    from services.mongo_cursor_order import probe_mongo_text_cursor

    coll, db, _schema = mongo_db
    coll.insert_many([
        {"_id": ObjectId("6554e1250000000000000002"), "code": "9"},
        {"_id": ObjectId("6554e124a000000000000001"), "code": "10"},
    ])
    cfg = {"connection_string": MONGO_URI, "database": db}
    assert probe_mongo_text_cursor(cfg, "src", "_id") == ""
    assert "string field" in probe_mongo_text_cursor(cfg, "src", "code")


# --------------------------------------------------------------------------
# PostgreSQL replica (hot standby) sources
# --------------------------------------------------------------------------


def test_hot_standby_plan_takes_the_re_read_window_and_says_why():
    from services import commit_safe_window as csw

    plan = csw.plan_commit_safe_window(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        cursor_type="bigint", stored_watermark="100", can_replay=True,
        contract={"cursor_overlap_values": 50}, metadata={},
        capture_bound=lambda: {"in_recovery": True}, probe_late=None, relation_kind="r",
    )
    assert plan.strategy == csw.STRATEGY_OVERLAP, plan
    assert "hot standby (read replica)" in plan.reason, plan.reason
    unguarded = csw.plan_commit_safe_window(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        cursor_type="bigint", stored_watermark="100", can_replay=False, contract={},
        metadata={}, capture_bound=lambda: {"in_recovery": True}, probe_late=None,
        relation_kind="r",
    )
    assert unguarded.strategy == csw.STRATEGY_NONE and "hot standby" in unguarded.reason


def test_live_pg_replica_source_is_not_claimed_commit_safe(isolated, pg_schema, monkeypatch):  # noqa: F811
    from services import commit_safe_window as csw

    schema = pg_schema
    _sql(f'CREATE TABLE "{schema}".src (id BIGINT PRIMARY KEY, v TEXT)')
    _sql(f"INSERT INTO \"{schema}\".src SELECT g, 'x' FROM generate_series(1, 5) g")
    src, dst = _pg_endpoint(schema, "src"), _pg_endpoint(schema, "dst")
    # The server answers as a hot standby does; everything else is the live run.
    monkeypatch.setattr(csw, "_pg_in_recovery", lambda _cur: True)
    first = _run(isolated, src, dst, "id", "id", cursor_overlap_values=10)
    assert first.success, first.error
    _sql(f"INSERT INTO \"{schema}\".src SELECT g, 'y' FROM generate_series(6, 8) g")
    second = _run(isolated, src, dst, "id", "id", cursor_overlap_values=10)
    assert second.success, second.error
    window = (second.destination_summary or {}).get("commit_safe_window") or {}
    assert window.get("strategy") == csw.STRATEGY_OVERLAP, window
    assert "hot standby (read replica)" in str(window.get("reason")), window
    assert _dest_count(schema) == 8
    assert json.dumps(window)  # evidence is serializable for the job record
