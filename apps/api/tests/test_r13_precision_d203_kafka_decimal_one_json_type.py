"""D203 - a DECIMAL column sent to Kafka has one JSON type for the whole topic.

QA (prod ac612203, still on 3e258e90f): MySQL decimal -> Kafka topic; values
binary64 holds exactly went out as JSON numbers (``1.5``, ``-1e-06``) while
the rest went out as strings (``"99999999999.999999"``), so one field mixed
two JSON types and typed consumers (Avro/JSON-Schema, Spark, ksqlDB) broke.

Root cause: ``to_json_value`` demoted a DECIMAL cell to a float whenever
binary64 could hold it (C26/D88) and left the others as exact text. The
Registry JSON Schema already declared DECIMAL as ``string``.

Fix: a declared DECIMAL/NUMERIC field on a JSON wire is always its exact text;
JSON *files* (``exact_decimal``) keep exact JSON numbers (D144); IEEE
float/double columns and integers keep their JSON types; CSV keeps the
source spelling.

Live broker 127.0.0.1:9092, live PostgreSQL source; the topic is consumed
back and every message is checked.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.kafka_writer import kafka_json_payload  # noqa: E402
from connectors.writer_common import to_delimited_value, to_json_value  # noqa: E402
from services.value_serializer import json_default  # noqa: E402

BIG = "99999999999.999999"
LONG = "12345678901234567890.0123456789"
_BROKER = {"host": "127.0.0.1", "port": 9092}


def test_decimal_field_is_exact_text_for_every_value():
    types = {"id": "integer", "amount": "DECIMAL(18,6)", "big": "DECIMAL(30,10)", "ratio": "DOUBLE", "n": "TEXT"}
    rows = [
        ("1", "1.500000", LONG, "0.5", "a"),
        ("2", BIG, "-0.000001", "1e-06", ""),
        ("3", Decimal("7"), Decimal("1E+2"), 2.5, "Łukasz 山田"),
        ("4", "__DF_SQL_NULL__", None, None, "  "),
    ]
    wire = [json.loads(json.dumps(kafka_json_payload(r, list(types), types), default=json_default)) for r in rows]
    assert [m["amount"] for m in wire] == ["1.500000", BIG, "7", None]
    assert [m["big"] for m in wire] == [LONG, "-0.000001", "100", None]
    assert all(isinstance(m["amount"], str) for m in wire[:3])  # one JSON type: string
    assert [m["ratio"] for m in wire] == [0.5, 1e-06, 2.5, None]  # IEEE stays a JSON number
    assert [m["id"] for m in wire] == [1, 2, 3, 4]
    assert [m["n"] for m in wire] == ["a", "", "Łukasz 山田", "  "]


def test_files_and_csv_keep_their_own_decimal_contracts():
    types = {"amount": "DECIMAL(18,6)"}
    assert to_json_value("10.50", "amount", types, exact_decimal=True) == Decimal("10.50")
    assert to_json_value(Decimal("10.50"), "amount", types) == "10.50"
    assert to_delimited_value("10.50", "amount", types) == "10.50"
    assert to_delimited_value("1E+2", "amount", types) == "1E+2"
    assert to_delimited_value(Decimal("10.50"), "amount", types) == Decimal("10.50")


# ------------------------------------------------------------------ live e2e
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402


def _broker_up() -> bool:
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture()
def live():
    if not _broker_up():
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    if not pg_up():
        pytest.skip("PostgreSQL 127.0.0.1:5432 not reachable")
    pytest.importorskip("kafka")
    psycopg2 = pytest.importorskip("psycopg2")
    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                            user=c["username"], password=c["password"])
    conn.autocommit = True
    table = "r13_d203_src_" + uuid.uuid4().hex[:8]
    topic = "r13_d203_" + uuid.uuid4().hex[:10]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, amount NUMERIC(18,6), big NUMERIC(30,10), label TEXT)")
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s,%s,%s,%s)",
            [(1, "1.500000", LONG, "a"), (2, BIG, "-0.0000000001", ""),
             (3, "-0.000001", "7", "Łukasz 山田 🎉"), (4, None, None, "  ")],
        )
    try:
        yield c, table, topic
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
        from services.copy_kafka_common import kafka_admin

        admin = kafka_admin(_BROKER)
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        finally:
            admin.close()


def _consume(topic: str, expected: int) -> list[dict]:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic, bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}",
        auto_offset_reset="earliest", enable_auto_commit=False,
        group_id=f"r13-d203-{uuid.uuid4().hex[:8]}", consumer_timeout_ms=15000,
    )
    out = []
    try:
        for msg in consumer:
            out.append(json.loads(msg.value.decode("utf-8")))
            if len(out) >= expected:
                break
    finally:
        consumer.close()
    return out


@pytest.mark.timeout(180)
def test_pg_numeric_to_kafka_topic_has_one_json_type_per_field(live, monkeypatch):
    pg, table, topic = live
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", host=pg["host"], port=pg["port"],
                              database=pg["database"], username=pg["username"], password=pg["password"],
                              schema="public", table=table),
        destination=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        sync_mode="full_refresh_append", skip_preflight=True, validation_mode="strict",
    )
    for run in (1, 2):  # the second run appends into the topic the first one created
        res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        assert res.success, (run, getattr(res, "error", None))
    messages = _consume(topic, 8)
    assert len(messages) == 8, messages
    by_id = {}
    for m in messages:
        by_id.setdefault(m["id"], []).append(m)
    assert set(by_id) == {1, 2, 3, 4} and all(len(v) == 2 for v in by_id.values())
    for m in messages:
        for field in ("amount", "big"):
            assert m[field] is None or isinstance(m[field], str), (field, m)
    assert by_id[1][0]["amount"] == "1.500000" and by_id[1][0]["big"] == LONG
    assert by_id[2][0]["amount"] == BIG and by_id[2][0]["big"] == "-0.0000000001"
    assert by_id[3][0]["amount"] == "-0.000001" and by_id[3][0]["big"] == "7.0000000000"
    assert by_id[4][0]["amount"] is None and by_id[4][0]["big"] is None
    assert by_id[2][0]["label"] == "" and by_id[3][0]["label"] == "Łukasz 山田 🎉" and by_id[4][0]["label"] == "  "
