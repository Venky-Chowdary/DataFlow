"""D151: a Kafka-source plan shows the broker envelope columns the run emits.

The transfer reader adds ``_kafka_topic`` / ``_kafka_partition`` /
``_kafka_offset`` / ``_kafka_key`` to every row, but the schema sample that
builds the plan decoded messages without their coordinates. A plan mapped from
that schema was refused at run time: "4 source column(s) are neither mapped
nor declared omitted". The envelope is now a fixed, typed part of the topic
schema, so a plan maps or omits it like any other column.
"""
from __future__ import annotations

import socket
import sqlite3
import uuid
from unittest.mock import patch

import pytest

from connectors.kafka_debezium_bridge import KAFKA_ENVELOPE_TYPES
from tests.test_property2_golden_path_never_blocked import _FakeMongo

_BROKER = {"host": "127.0.0.1", "port": 9092}


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


def test_envelope_types_are_fixed_whatever_the_sampled_values():
    from connectors.kafka_reader import _records_to_batch

    batch = _records_to_batch(
        [{"id": 1, "_kafka_topic": "t", "_kafka_partition": 0, "_kafka_offset": 5,
          "_kafka_key": "7"}],
        None,
        None,
    )
    native = batch.meta["native_types"]
    assert {c: native[c] for c in KAFKA_ENVELOPE_TYPES} == KAFKA_ENVELOPE_TYPES


@pytest.fixture
def topic():
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    name = f"d151_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
        admin.delete_groups([f"g_{name}"])
    except Exception:
        pass
    finally:
        admin.close()


def _produce_unkeyed(topic: str, n: int) -> None:
    from connectors.kafka_writer import _producer

    producer = _producer(_BROKER)
    try:
        for i in range(1, n + 1):
            producer.send(topic, value={"id": i, "amount": i * 10})
        producer.flush(10)
    finally:
        producer.close()


def _plan_columns(topic: str) -> list[str]:
    from services.schema_introspect import introspect_schema

    info = introspect_schema("kafka", table=topic, **_BROKER)
    assert info["ok"], info
    return [c["name"] for c in info["columns"]]


def _run(topic: str, db: str, mappings: list[dict]):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="kafka", table=topic,
            extra={"group_id": f"g_{topic}"}, **_BROKER,
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        mappings=mappings,
        sync_mode="incremental_append",
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _dest_columns(db: str) -> list[str]:
    with sqlite3.connect(db) as conn:
        return [r[1] for r in conn.execute("pragma table_info(t)")]


def test_plan_shows_the_envelope_and_a_fully_mapped_plan_runs(topic, tmp_path):
    _produce_unkeyed(topic, 3)
    columns = _plan_columns(topic)
    assert columns == ["id", "amount", *KAFKA_ENVELOPE_TYPES], columns

    db = str(tmp_path / "dest.db")
    result = _run(topic, db, [{"source": c, "target": c, "confidence": 1.0} for c in columns])
    assert result.success, result.error
    assert result.records_transferred == 3
    assert _dest_columns(db) == columns


def test_a_plan_that_omits_the_envelope_runs(topic, tmp_path):
    _produce_unkeyed(topic, 3)
    columns = _plan_columns(topic)
    mappings = [
        {"source": c, "target": "", "intentional_omit": True}
        if c in KAFKA_ENVELOPE_TYPES
        else {"source": c, "target": c, "confidence": 1.0}
        for c in columns
    ]
    db = str(tmp_path / "dest.db")
    result = _run(topic, db, mappings)
    assert result.success, result.error
    assert result.records_transferred == 3
    assert _dest_columns(db) == ["id", "amount"]
