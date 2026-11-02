"""D89 (live broker): a Kafka changelog upsert keeps the last image per key.

A topic repeating a key is a changelog: the later message is the newer
version of the row. The uniqueness probe treated the repeat as a duplicate
source key and blocked the upsert; once that passed, the run's Gate-8 came up
one row short per collapsed image, and the next run's dry-run reconciliation
blocked on the destination primary key. Now the repeat collapses
last-write-wins, is accounted as skipped, and later runs keep applying.
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
    name = f"d89_{uuid.uuid4().hex[:10]}"
    yield name
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    finally:
        admin.close()


def _produce(topic: str, rows: list[dict]) -> None:
    from connectors.kafka_writer import _producer

    producer = _producer(_BROKER)
    try:
        for row in rows:
            producer.send(topic, value=row, key=str(row["id"]))
        producer.flush(10)
    finally:
        producer.close()


def _run(topic: str, db: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    mode = "incremental_upsert"
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="kafka", table=topic, **_BROKER),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        sync_mode=mode,
        stream_contracts=[{
            "stream": topic,
            "sync_mode": mode,
            "primary_key": ["id"],
            "cursor_field": "_kafka_offset",
            "cursor_semantics": "monotonic_sequence",
        }],
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _rows(db: str) -> list[tuple]:
    with sqlite3.connect(db) as conn:
        return [tuple(r) for r in conn.execute("select id, amount from t order by id")]


def test_repeated_key_is_last_write_wins_across_runs(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(topic, [{"id": 1, "amount": 10}, {"id": 2, "amount": 20},
                     {"id": 3, "amount": 30}, {"id": 1, "amount": 999}])
    first = _run(topic, db)
    assert first.success, first.error
    assert _rows(db) == [(1, 999), (2, 20), (3, 30)]
    assert int(first.destination_summary.get("collapsed_duplicate_rows") or 0) == 1

    _produce(topic, [{"id": 2, "amount": 222}, {"id": 2, "amount": 2222},
                     {"id": 4, "amount": 40}])
    second = _run(topic, db)
    assert second.success, second.error
    assert _rows(db) == [(1, 999), (2, 2222), (3, 30), (4, 40)]
