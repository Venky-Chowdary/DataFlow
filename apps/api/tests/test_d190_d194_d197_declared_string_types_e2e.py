"""D190 / D194 / D197 on the route QA drives: ``start_transfer`` -> confirm -> job.

One family: a value's *declared* type was re-typed from what its sampled text
looked like.

* D197: a plain MongoDB collection could not load into PostgreSQL. ``_id`` is
  an ObjectId, mapped to ``VARCHAR(24)``. The name-triggered ``trim_id``
  transform stayed on it (ObjectId and VARCHAR are different logical families),
  which graded it ``mutate`` at 0.70 confidence and held G4. A signed
  continue-policy contract was minted for it but G4's confidence check never
  read contracts, so the approval could not clear it either.
* D194: BSON strings ``'t'`` / ``'f'`` were typed BOOLEAN.
* D190: digit strings were typed INTEGER / DATE / TIMESTAMP (on a Kafka topic a
  JSON string ``"20240101"`` read as TIMESTAMP and blocked as lossy).

A BSON string, a JSON string on a Kafka topic and the Kafka message key are
declared strings. They land as text, byte for byte, on run 1 and on run 2 into
the table run 1 created.

Live: MongoDB on 127.0.0.1:27017, Kafka on 127.0.0.1:9092, PostgreSQL from
``PG_HOST`` / ``PG_PORT`` (default 127.0.0.1:5432, user/password/db
``dataflow``). Each is skipped when unreachable.
"""

from __future__ import annotations

import os
import socket
import time
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")
psycopg2 = pytest.importorskip("psycopg2")
from bson import ObjectId  # noqa: E402

PG = {
    "host": os.environ.get("PG_HOST") or "127.0.0.1",
    "port": int(os.environ.get("PG_PORT") or 5432),
    "user": os.environ.get("PG_USER") or "dataflow",
    "password": os.environ.get("PG_PASSWORD") or "dataflow",
    "dbname": os.environ.get("PG_DB") or "dataflow",
}
_KAFKA = {"host": "127.0.0.1", "port": 9092}


def _up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


def _pg():
    if not _up(PG["host"], PG["port"]):
        pytest.skip(f"PostgreSQL not reachable on {PG['host']}:{PG['port']}")
    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:  # environmental: wrong credentials on this host
        pytest.skip(f"PostgreSQL login failed: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def transfer_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.ai.copilot import tools as tools_mod
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    monkeypatch.setattr(tools_mod, "_tools", None)

    with TestClient(app) as client:
        yield client


def _wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        last = job
        if str(job.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return job
        time.sleep(0.25)
    return last


def _pg_connector(tag: str) -> str:
    from services.connector_store import create_connector

    return str(
        create_connector(
            {
                "name": f"{tag}-pg-{uuid.uuid4().hex[:6]}",
                "type": "postgresql",
                "role": "destination",
                "host": PG["host"],
                "port": PG["port"],
                "database": PG["dbname"],
                "username": PG["user"],
                "password": PG["password"],
                "schema": "public",
                "ssl": False,
                "workspace_id": "",
            }
        ).id
    )


def _run(client, **kwargs) -> tuple[dict, dict]:
    """``start_transfer`` -> ``/copilot/confirm`` -> finished job."""
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    plan = (staged.output or {}).get("plan") or {}
    assert staged.success, {
        "error": staged.error,
        "blockers": ((staged.output or {}).get("preflight") or {}).get("blockers"),
        "mappings": [
            {k: m.get(k) for k in ("source", "source_type", "target_type", "fidelity")}
            for m in (staged.output or {}).get("engine_mappings") or []
        ],
    }
    preflight = plan.get("preflight") or {}
    assert preflight.get("passed") is True, preflight.get("blockers")
    from src.ai.copilot.ack_ledger import get_ack_ledger

    # The mappings the confirmed job runs (start_transfer keeps them in the ledger).
    payload, _why = get_ack_ledger().get_pending_payload(staged.output["ack_id"])
    plan = {**plan, "engine_mappings": list((payload or {}).get("mappings") or [])}
    confirmed = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "d197"},
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return plan, job


def _pg_rows(table: str, columns: list[str], order_by: str) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cols = ", ".join(f'"{c}"' for c in columns)
            cur.execute(f'SELECT {cols} FROM public."{table}" ORDER BY "{order_by}"')
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _pg_column_types(table: str) -> dict[str, str]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            return {str(r[0]): str(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()


def _drop(table: str) -> None:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


_TEXT_PG = {"text", "character varying", "character"}

# ---------------------------------------------------------------- MongoDB → PG

# Every string field is a BSON string. Names are chosen to trip the old name
# heuristics: ``order_date`` (YYYYMMDD → DATE), ``created_at`` (epoch digits →
# TIMESTAMP), ``is_active`` (t/f → BOOLEAN), ``zip`` (leading zeros).
_OIDS = [ObjectId() for _ in range(6)]
_MONGO_DOCS = [
    {"_id": _OIDS[0], "order_date": "20240101", "created_at": "1700000000",
     "code": "0012", "is_active": "t", "note": "", "label": "Zoë ✓"},
    {"_id": _OIDS[1], "order_date": "20231231", "created_at": "1700000000000",
     "code": "0007", "is_active": "f", "note": None, "label": "  padded  "},
    {"_id": _OIDS[2], "order_date": "19991231", "created_at": "1700000001",
     "code": "0100", "is_active": "t", "label": "x"},  # note missing
    {"_id": _OIDS[3], "order_date": None, "created_at": "1700000002",
     "code": "", "is_active": "f", "note": "t"},  # label missing
    {"_id": _OIDS[4], "order_date": "20240229", "created_at": None,
     "code": "0000", "is_active": "t", "note": "20240101", "label": ""},
    {"_id": _OIDS[5], "code": "42", "is_active": "f", "note": " ", "label": "f"},
]
_MONGO_COLS = ["_id", "order_date", "created_at", "code", "is_active", "note", "label"]


def _expected_mongo_rows() -> list[tuple]:
    rows = []
    for doc in _MONGO_DOCS:
        rows.append(tuple(
            str(doc["_id"]) if c == "_id" else doc.get(c) for c in _MONGO_COLS
        ))
    return sorted(rows, key=lambda r: r[0])


@pytest.fixture
def mongo_collection():
    if not _up("127.0.0.1", 27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    name = "d197_" + uuid.uuid4().hex[:8]
    coll = client["dataflow_test"][name]
    coll.insert_many([dict(d) for d in _MONGO_DOCS])
    try:
        yield coll
    finally:
        coll.drop()
        client.close()


def _mongo_source(coll) -> str:
    from services.connector_store import create_connector

    return str(
        create_connector(
            {
                "name": "d197-mongo-" + uuid.uuid4().hex[:6], "type": "mongodb",
                "role": "source", "host": "127.0.0.1", "port": 27017,
                "database": "dataflow_test", "ssl": False, "workspace_id": "",
            }
        ).id
    )


def test_mongo_objectid_and_declared_strings_land_as_text_on_two_runs(
    transfer_client, mongo_collection
):
    _pg().close()
    table = mongo_collection.name + "_out"
    _drop(table)
    source_id = _mongo_source(mongo_collection)
    dest_id = _pg_connector("d197")
    try:
        args = dict(
            source_connector_id=source_id,
            source_table=mongo_collection.name,
            dest_connector_id=dest_id,
            dest_table=table,
            sync_mode="full_refresh_overwrite",
        )
        plan, job = _run(transfer_client, **args)
        by_source = {m["source"]: m for m in plan["engine_mappings"]}
        oid = by_source["_id"]
        # ObjectId has a defined carrier: its 24 hex characters as text. No
        # approval is needed and no value rewrite is planned on it.
        assert oid["source_type"] == "OBJECTID", oid
        assert oid["target_type"] == "VARCHAR(24)", oid
        assert oid["fidelity"] == "preserve", oid
        assert str(oid.get("transform") or "none") == "none", oid
        assert not oid.get("requires_risk_contract"), oid
        for col in ("order_date", "created_at", "code", "is_active", "note", "label"):
            m = by_source[col]
            assert m["source_type"].split("(")[0].upper() in {"TEXT", "VARCHAR"}, m

        types = _pg_column_types(table)
        for col in _MONGO_COLS:
            assert types[col] in _TEXT_PG, types
        assert _pg_rows(table, _MONGO_COLS, "_id") == _expected_mongo_rows()
        assert int(job.get("records_transferred") or job.get("rows_written") or 0) in {0, 6}

        # Run 2 lands into the table run 1 created.
        plan2, _job2 = _run(transfer_client, **args)
        assert (plan2.get("preflight") or {}).get("passed") is True
        assert _pg_column_types(table) == types
        assert _pg_rows(table, _MONGO_COLS, "_id") == _expected_mongo_rows()
    finally:
        _drop(table)


def test_mongo_plan_with_signed_acceptance_is_not_held_on_objectid(
    transfer_client, mongo_collection
):
    """The operator's signed acceptance must never leave a mapping held.

    It used to: the plan signed a contract on ``_id`` (fidelity ``mutate``)
    while G4's confidence check ignored contracts, so the approval did nothing.
    """
    from src.ai.copilot import transfer_tools

    _pg().close()
    source_id = _mongo_source(mongo_collection)
    dest_id = _pg_connector("d197s")
    result = transfer_tools.plan_transfer(
        source_connector_id=source_id,
        source_table=mongo_collection.name,
        dest_connector_id=dest_id,
        dest_table=mongo_collection.name + "_signed",
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        risk_acceptance={
            "approved_by": "qa@example.com",
            "reason": "ObjectId lands as hex text",
            "execution_policy": "QUARANTINE_ROW",
        },
    )
    assert result.success, result.error
    blockers = (result.output.get("preflight") or {}).get("blockers") or []
    assert result.output["safe_to_start"] is True, blockers


# ----------------------------------------------------------------- Kafka → PG

_KAFKA_RUN1 = [
    (b"20240101", {"order_id": "20240101", "ts_s": "1700000000", "code": "0012",
                   "flag": "t", "note": ""}),
    (b"1700000000", {"order_id": "20231231", "ts_s": "1700000000000", "code": "0007",
                     "flag": "f", "note": None}),
    (b"0012", {"order_id": "19991231", "ts_s": "1700000001", "code": "",
               "flag": "t", "note": "Zoë ✓"}),
]
_KAFKA_RUN2 = [
    (b"20240102", {"order_id": "20240102", "ts_s": "1700000003", "code": "0100",
                   "flag": "f", "note": " "}),
]
_KAFKA_COLS = ["_kafka_key", "order_id", "ts_s", "code", "flag", "note"]


@pytest.fixture
def kafka_topic():
    if not _up(_KAFKA["host"], _KAFKA["port"]):
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    name = f"d190_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_KAFKA)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _produce(topic: str, messages) -> None:
    from connectors.kafka_writer import _producer

    producer = _producer(_KAFKA)
    try:
        for key, value in messages:
            producer.send(topic, key=key, value=value)
        producer.flush(10)
    finally:
        producer.close()


def _kafka_source(topic: str) -> str:
    from services.connector_store import create_connector

    return str(
        create_connector(
            {
                "name": "d190-kafka-" + uuid.uuid4().hex[:6], "type": "kafka",
                "role": "source", "host": _KAFKA["host"], "port": _KAFKA["port"],
                "database": "", "ssl": False, "workspace_id": "",
                "extra": {"group_id": f"g_{topic}"},
            }
        ).id
    )


def _expected_kafka(messages) -> list[tuple]:
    rows = []
    for key, value in messages:
        rows.append((key.decode(), *[value.get(c) for c in _KAFKA_COLS[1:]]))
    return sorted(rows, key=lambda r: r[0])


def test_kafka_digit_string_key_and_fields_land_as_text_on_two_runs(
    transfer_client, kafka_topic
):
    _pg().close()
    table = kafka_topic + "_out"
    _drop(table)
    _produce(kafka_topic, _KAFKA_RUN1)
    source_id = _kafka_source(kafka_topic)
    dest_id = _pg_connector("d190")
    try:
        args = dict(
            source_connector_id=source_id,
            source_table=kafka_topic,
            dest_connector_id=dest_id,
            dest_table=table,
            sync_mode="incremental_append",
        )
        plan, _job = _run(transfer_client, **args)
        by_source = {m["source"]: m for m in plan["engine_mappings"]}
        for col in _KAFKA_COLS:
            m = by_source[col]
            assert m["source_type"].split("(")[0].upper() in {"TEXT", "VARCHAR"}, m
            assert m["fidelity"] == "preserve", m
        types = _pg_column_types(table)
        for col in _KAFKA_COLS:
            assert types[col] in _TEXT_PG, types
        got = sorted(_pg_rows(table, _KAFKA_COLS, "_kafka_key"), key=lambda r: r[0])
        assert got == _expected_kafka(_KAFKA_RUN1)

        _produce(kafka_topic, _KAFKA_RUN2)
        _run(transfer_client, **args)
        assert _pg_column_types(table) == types
        got = sorted(_pg_rows(table, _KAFKA_COLS, "_kafka_key"), key=lambda r: r[0])
        assert got == _expected_kafka(_KAFKA_RUN1 + _KAFKA_RUN2)
    finally:
        _drop(table)
