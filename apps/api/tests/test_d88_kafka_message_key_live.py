"""D88 (live broker): a Kafka destination keys messages by the source's key.

A declared identity (stream contract primary_key / upsert_key) already
became the produce key, but a plain append from a table with a primary key
went out key=None: no per-key partitioning, nothing for log compaction to
keep. The source catalog's primary key, when mapped, is now the key; a
source without one still produces unkeyed messages.
"""
from __future__ import annotations

import socket
import sqlite3
import uuid
from unittest.mock import patch

import pytest

from tests.test_property2_golden_path_never_blocked import _FakeMongo

_BROKER = {"host": "127.0.0.1", "port": 9092}


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


@pytest.fixture
def topic():
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    name = f"d88_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _load(tmp_path, topic: str, ddl: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    db = str(tmp_path / "src.db")
    with sqlite3.connect(db) as conn:
        conn.execute(ddl)
        conn.executemany("insert into t values (?,?)", [(i, f"v{i}") for i in (7, 8, 9)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        destination=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")],
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success, result.error
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers="127.0.0.1:9092", enable_auto_commit=False)
    try:
        tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or []]
        consumer.assign(tps)
        consumer.seek_to_beginning()
        messages = [m for batch in consumer.poll(timeout_ms=5000).values() for m in batch]
    finally:
        consumer.close()
    return sorted(m.key for m in messages if m.key is not None), len(messages)


def test_append_keys_messages_by_the_source_primary_key(topic, tmp_path):
    keys, n = _load(tmp_path, topic, "create table t (id integer primary key, v text)")
    assert n == 3
    assert keys == [b"7", b"8", b"9"]


def test_a_source_without_a_key_stays_unkeyed(topic, tmp_path):
    keys, n = _load(tmp_path, topic, "create table t (id integer, v text)")
    assert n == 3
    assert keys == []
