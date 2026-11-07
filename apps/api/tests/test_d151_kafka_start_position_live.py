"""D151 (live broker): a full refresh re-reads the topic; only incremental continues.

The route's consumer group is stable (D84) and commits on acceptance (D85), so
every run after the first resumed at the committed offset. A full-refresh
append/overwrite of the same topic then read 0 new messages and failed
("fidelity collapse" on an empty read), which is the "Kafka as a source loads 0
rows" report. Only incremental modes continue from the group; everything else
starts at the log start, and the job summary says which start was used.
Messages produced before the first run are read in full.
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
    name = f"d151s_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _produce(topic: str, ids) -> None:
    from connectors.kafka_writer import _producer

    producer = _producer(_BROKER)
    try:
        for i in ids:
            producer.send(topic, value={"id": i, "amount": i * 10})
        producer.flush(10)
    finally:
        producer.close()


def _run(topic: str, db: str, mode: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        sync_mode=mode,
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _count(db: str) -> int:
    with sqlite3.connect(db) as conn:
        return conn.execute("select count(*) from t").fetchone()[0]


def test_full_refresh_rereads_the_topic_on_every_run(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(topic, range(1, 6))
    for n in (1, 2, 3):
        result = _run(topic, db, "full_refresh_append")
        assert result.success, (n, result.error)
        assert result.records_transferred == 5, n
        offsets = result.destination_summary["kafka_offsets"]
        assert offsets["start_position"] == "earliest"
    assert _count(db) == 15


def test_incremental_continues_from_the_committed_offset(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(topic, range(1, 4))
    first = _run(topic, db, "incremental_append")
    assert first.success, first.error
    assert first.records_transferred == 3
    assert first.destination_summary["kafka_offsets"]["start_position"] == "committed"

    _produce(topic, range(4, 6))
    second = _run(topic, db, "incremental_append")
    assert second.success, second.error
    assert second.records_transferred == 2
    assert _count(db) == 5
