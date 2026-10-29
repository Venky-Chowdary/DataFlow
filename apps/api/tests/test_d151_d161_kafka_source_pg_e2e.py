"""D151 / D161 (live broker -> live PostgreSQL): a Kafka-source route keeps loading.

QA on ac612203: "Kafka as a SOURCE is still broken". The earlier fixes were
proved on integer-only, unkeyed JSON into SQLite / SQL Server. On the route QA
runs (Kafka -> PostgreSQL, real values) two things still blocked the load:

* D161 - a JSON number with a full IEEE fraction (0.30000000000000004) is a
  typed double on the wire, but the create-new risk stamp only treated
  relational engines and MongoDB as declaring their types, so every Kafka
  DOUBLE got the "IEEE/Excel float artifact" chip: confidence 0.70, G4 blocked.
* D151 - an incremental run maps on the messages it reads. A field that is
  NULL in every one of them (``"note": null``) was "VARCHAR, no samples" and
  was capped under the Map floor, so the incremental run after a full refresh
  loaded 0 rows. The D151 fix had exempted only the connector's own
  ``_kafka_key`` from that cap.

Every test here goes through ``UniversalTransferEngine().execute_tracked`` (or
the MCP ``start_transfer`` -> confirm route) against the broker on
127.0.0.1:9092 and PostgreSQL from ``PG_HOST``/``PG_PORT``/``PG_USER``/
``PG_PASSWORD``/``PG_DB`` (defaults: the project container on 5432).
"""
from __future__ import annotations

import json
import os
import socket
import time
import uuid
from unittest.mock import patch

import pytest

from tests.test_property2_golden_path_never_blocked import _FakeMongo

_BROKER = {"host": "127.0.0.1", "port": 9092}
PG = {
    "host": os.environ.get("PG_HOST", "127.0.0.1"),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "user": os.environ.get("PG_USER", "dataflow"),
    "password": os.environ.get("PG_PASSWORD", "dataflow"),
    "dbname": os.environ.get("PG_DB", "dataflow"),
}

# Values QA's topics carry: IEEE doubles with a full fraction, NULL fields,
# empty / whitespace / unicode text, digit-string keys (leading zeros), an
# unkeyed message and a tombstone for an earlier key.
_MESSAGES = [
    (b"001", {"id": 1, "price": 0.30000000000000004, "name": "a", "note": None}),
    (b"42", {"id": 2, "price": 1.23456789012345, "name": "", "note": "  "}),
    (b"7", {"id": 3, "price": 12.5, "name": "é中\U0001f600", "note": "x"}),
    (b"42", None),  # tombstone: no row
    (None, {"id": 4, "price": None, "name": None, "note": None}),
    (b"0099", {"id": 5, "price": 1e-07, "name": "z", "note": "y"}),
    # A JSON number written without a fraction: still the DOUBLE column.
    (b"8", {"id": 6, "price": 3, "name": " lead", "note": "\u00a0"}),
]
_EXPECTED = {
    1: (0.30000000000000004, "a", None, "001"),
    2: (1.23456789012345, "", "  ", "42"),
    3: (12.5, "é中\U0001f600", "x", "7"),
    4: (None, None, None, None),
    5: (1e-07, "z", "y", "0099"),
    6: (3.0, " lead", "\u00a0", "8"),
}


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
    except Exception as exc:
        pytest.skip(f"PostgreSQL {PG['host']}:{PG['port']} not usable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def topic():
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    name = f"d151pg_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


@pytest.fixture
def table():
    name = f"d151pg_{uuid.uuid4().hex[:10]}"
    conn = _pg()
    yield name
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{name}"')
    finally:
        conn.close()


def _produce(topic: str, messages) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}")
    try:
        for key, value in messages:
            producer.send(
                topic, key=key, value=None if value is None else json.dumps(value).encode()
            )
        producer.flush(10)
    finally:
        producer.close()


def _run(topic: str, table: str, mode: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        destination=EndpointConfig(
            kind="database", format="postgresql", host=PG["host"], port=PG["port"],
            database=PG["dbname"], schema="public", table=table,
            username=PG["user"], password=PG["password"],
        ),
        sync_mode=mode,
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _rows(table: str) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, price, name, note, _kafka_key FROM public."{table}" ORDER BY id, _kafka_offset'
            )
            return cur.fetchall()
    finally:
        conn.close()


def _column_types(table: str) -> dict[str, str]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            return dict(cur.fetchall())
    finally:
        conn.close()


def _assert_values(rows: list[tuple]) -> None:
    for row in rows:
        ident = int(row[0])
        assert tuple(row[1:]) == _EXPECTED[ident], row


def test_full_refresh_overwrite_twice_lands_every_value(topic, table):
    _produce(topic, _MESSAGES)
    for n in (1, 2):
        result = _run(topic, table, "full_refresh_overwrite")
        assert result.success, (n, result.error)
        assert result.records_transferred == 6, n
        rows = _rows(table)
        assert [int(r[0]) for r in rows] == [1, 2, 3, 4, 5, 6], n
        _assert_values(rows)
    types = _column_types(table)
    assert types["price"] == "double precision"
    # A key is text even when it is digits: leading zeros survive.
    assert types["_kafka_key"] == "text"


def test_full_refresh_append_twice_rereads_the_topic(topic, table):
    _produce(topic, _MESSAGES)
    for n in (1, 2):
        result = _run(topic, table, "full_refresh_append")
        assert result.success, (n, result.error)
        assert result.records_transferred == 6, n
        assert result.destination_summary["kafka_offsets"]["start_position"] == "earliest"
    rows = _rows(table)
    assert len(rows) == 12
    _assert_values(rows)


def test_incremental_after_full_refresh_reads_only_new_messages(topic, table):
    _produce(topic, _MESSAGES)
    first = _run(topic, table, "full_refresh_overwrite")
    assert first.success, first.error

    # The new messages carry a NULL in every field the incremental read could
    # type ``note`` from, and a digit-only key.
    _produce(topic, [
        (b"12", {"id": 7, "price": 2.5, "name": "n", "note": None}),
        (b"13", None),  # tombstone
    ])
    second = _run(topic, table, "incremental_append")
    assert second.success, second.error
    assert second.records_transferred == 1
    assert second.destination_summary["kafka_offsets"]["start_position"] == "committed"
    rows = _rows(table)
    assert [int(r[0]) for r in rows] == [1, 2, 3, 4, 5, 6, 7]
    assert tuple(rows[-1][1:]) == (2.5, "n", None, "12")
    _assert_values(rows[:-1])


def test_incremental_with_no_new_messages_is_not_a_failure(topic, table):
    _produce(topic, _MESSAGES)
    first = _run(topic, table, "incremental_append")
    assert first.success, first.error
    assert first.records_transferred == 6
    again = _run(topic, table, "incremental_append")
    assert again.success, again.error
    assert again.records_transferred == 0
    assert len(_rows(table)) == 6


# --- MCP route: start_transfer -> /copilot/confirm -> job ---------------------


@pytest.fixture
def transfer_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.connector_store as connector_store
    import services.mongodb_service as mongo_mod
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(ack_mod, "_ledger", None)

    with TestClient(app) as client:
        yield client


def _wait_job(job_id: str, timeout_s: float = 180.0) -> dict:
    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        last = get_mongodb_service().get_job(job_id) or {}
        if str(last.get("status") or "") in {"completed", "failed", "partial", "cancelled"}:
            return last
        time.sleep(0.25)
    return last


def test_mcp_start_transfer_full_refresh_twice(topic, table, transfer_client):
    _pg().close()
    from services.connector_store import create_connector
    from src.ai.copilot.transfer_tools import start_transfer

    token = uuid.uuid4().hex[:8]
    source = create_connector({
        "name": f"d151-kafka-{token}", "type": "kafka", "role": "source",
        "host": _BROKER["host"], "port": _BROKER["port"],
    })
    dest = create_connector({
        "name": f"d151-pg-{token}", "type": "postgresql", "role": "destination",
        "host": PG["host"], "port": PG["port"], "database": PG["dbname"],
        "username": PG["user"], "password": PG["password"], "schema": "public", "ssl": False,
    })
    _produce(topic, _MESSAGES)
    for n in (1, 2):
        staged = start_transfer(
            source_connector_id=source.id,
            source_table=topic,
            dest_connector_id=dest.id,
            dest_table=table,
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
        )
        assert staged.success, (n, staged.error)
        confirmed = transfer_client.post(
            "/api/v1/copilot/confirm",
            json={"ack_id": staged.output["ack_id"], "reason": f"d151-run-{n}"},
        )
        assert confirmed.status_code == 200, confirmed.text
        body = confirmed.json()
        assert body.get("ok") is True, body
        job = _wait_job(str(body.get("job_id") or ""))
        assert str(job.get("status") or "") == "completed", (
            n, job.get("status"), job.get("error") or job.get("message"),
        )
        rows = _rows(table)
        assert [int(r[0]) for r in rows] == [1, 2, 3, 4, 5, 6], n
        _assert_values(rows)
        assert _column_types(table)["price"] == "double precision"


# --- the same rules without a broker ------------------------------------------


def test_kafka_double_is_a_typed_double_at_the_risk_stamp():
    from services.create_new_risk_stamp import apply_create_new_risk_stamps

    row = {
        "source": "price", "target": "price", "source_type": "DOUBLE",
        "target_type": "DOUBLE PRECISION", "assignment_strategy": "identity_passthrough",
        "create_new": True, "confidence": 0.95,
    }
    residue = {"price": ["0.30000000000000004", "1.23456789012345", None, ""]}
    for engine in ("kafka", "mongodb", "postgresql"):
        out = apply_create_new_risk_stamps(
            [dict(row)], "postgresql", source_samples=residue,
            dest_table_exists=False, source_db_type=engine,
        )[0]
        assert "ieee_float_artifact" not in [r.get("kind") for r in out.get("create_new_risks") or []]
        assert float(out["confidence"]) >= 0.85, engine
    csv = apply_create_new_risk_stamps(
        [dict(row)], "postgresql", source_samples=residue,
        dest_table_exists=False, source_db_type="csv",
    )[0]
    assert "ieee_float_artifact" in [r.get("kind") for r in csv.get("create_new_risks") or []]


@pytest.mark.parametrize(
    "null_rate, samples, target, capped",
    [
        (1.0, [], "TEXT", False),      # observed, every value NULL: lossless into text
        (1.0, [], "BIGINT", True),     # NULL says nothing about an integer target
        (None, [], "TEXT", True),      # never observed: still the placeholder cap
        (0.0, [], "TEXT", True),       # a stamped 0 with no samples is not observed NULL
        (0.5, ["", " "], "TEXT", False),  # empty / whitespace text is evidence
    ],
)
def test_all_null_observed_column_is_not_weak_evidence(null_rate, samples, target, capped):
    from services.mapping_pipeline import _demote_untyped_varchar_confidence

    out = _demote_untyped_varchar_confidence(
        [{"source": "note", "target": "note", "source_type": "VARCHAR",
          "target_type": target, "confidence": 0.97}],
        source_schemas=[{"name": "note", "inferred_type": "VARCHAR",
                         "samples": samples, "null_rate": null_rate}],
    )[0]
    assert (out["confidence"] < 0.85) is capped


def test_kafka_numbers_widen_within_numbers():
    from connectors.kafka_reader import _records_to_batch, merge_kafka_logicals

    batch = _records_to_batch(
        [{"p": 3, "q": 3, "s": "x"}, {"p": 2.5, "q": 3_000_000_000, "s": 1}, {"p": None, "q": None}],
        None, None,
    )
    native = batch.meta["native_types"]
    assert native["p"] == "DOUBLE"
    assert native["q"] == "BIGINT"
    assert native["s"] == "TEXT"
    assert merge_kafka_logicals("INTEGER", "FLOAT") == "FLOAT"
    assert merge_kafka_logicals("DOUBLE", "DECIMAL") == "DECIMAL"
    assert merge_kafka_logicals(None, "VARCHAR") == "VARCHAR"
