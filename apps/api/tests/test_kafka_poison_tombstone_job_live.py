"""Live broker: a Kafka page with a poison message or a tombstone must not crash the job.

The per-page write function bound a local ``dest_summary`` later in its body,
so the Kafka poison/tombstone block at its top read it before assignment and
every such page failed the whole job ("cannot access local variable
'dest_summary'"). The page summary is now ``chunk_summary``; the Kafka
ledger lands on the run summary. The poison message is a consumed source
record, quarantined with its broker coordinates, so Gate-8 conservation
balances (read = written + rejected).
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
    name = f"poison_{uuid.uuid4().hex[:10]}"
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


def test_poison_message_and_tombstone_do_not_fail_the_job(topic, tmp_path):
    from kafka import KafkaProducer

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")
    try:
        for i in (1, 2):
            producer.send(topic, value=f'{{"id": {i}, "amount": {i * 10}}}'.encode())
        producer.send(topic, value=b"{not json")
        producer.send(topic, value=None, key=b"gone")
        producer.send(topic, value=b'{"id": 3, "amount": 30}')
        producer.flush(10)
    finally:
        producer.close()

    db = str(tmp_path / "dest.db")
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="kafka", table=topic,
            extra={"group_id": f"g_{topic}"}, **_BROKER,
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        sync_mode="incremental_append",
        validation_mode="balanced",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, result.error
    summary = result.destination_summary
    assert summary["rejected_rows"] == 1
    assert summary["kafka_tombstones_seen"] == 1
    assert "kafka poison message" in summary["rejected_details"][0]["reason"]
    with sqlite3.connect(db) as conn:
        assert conn.execute("select count(*) from t").fetchone()[0] == 3
