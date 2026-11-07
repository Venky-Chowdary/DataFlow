"""D152 (live broker): a Kafka destination that took every row is not FAILED.

Gate-8 read the topic back with the transfer reader, which stops after one
poll batch (2,500 messages) and digests the broker envelope columns the
source never had. Every produce larger than a batch failed "expected target
N vs target 2500" (and every produce failed the checksum), although all rows
were written: an invitation to a duplicating retry. The read-back now reads
each partition to its end offsets and digests the produced fields; a
read-back that cannot finish is unverified (-1), never a short count.
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
    name = f"d152_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _source(tmp_path, n: int) -> str:
    db = str(tmp_path / "src.db")
    with sqlite3.connect(db) as conn:
        conn.execute("create table t (id integer primary key, amount integer, note text)")
        conn.executemany(
            "insert into t values (?,?,?)",
            [(i, i * 10, None if i % 2 else f"n{i}") for i in range(1, n + 1)],
        )
    return db


def _run(db: str, topic: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        destination=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in ("id", "amount", "note")],
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def test_produce_larger_than_one_poll_batch_verifies(topic, tmp_path):
    db = _source(tmp_path, 3000)
    first = _run(db, topic)
    assert first.success, first.error
    assert first.records_transferred == 3000
    assert first.reconciliation["target_rows"] == 3000

    # Appending into the now non-empty topic still verifies.
    second = _run(db, topic)
    assert second.success, second.error
    assert second.records_transferred == 3000


def test_unfinished_readback_is_unverified_not_short(topic, tmp_path, monkeypatch):
    from connectors import kafka_reader
    from services.reconciliation import verify_kafka_topic

    db = _source(tmp_path, 5)
    assert _run(db, topic).success
    count, _ = verify_kafka_topic(topic=topic, target_columns=["id", "amount", "note"], **_BROKER)
    assert count == 5
    monkeypatch.setattr(kafka_reader, "_READ_TO_END_S", 0.0)
    assert verify_kafka_topic(topic=topic, **_BROKER) == (-1, "")
