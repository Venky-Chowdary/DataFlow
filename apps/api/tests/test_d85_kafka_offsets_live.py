"""D85 (live broker): Kafka group offsets move only when the job is accepted.

A Kafka-source run is streamed. The streaming success path never committed the
group offsets, so every run re-read the whole topic, and the in-run
continuation read started from the group position again. Here, against a real
broker: a run whose Gate-8 fails leaves the committed group offset unchanged
and the rerun reads the same messages; an accepted run commits, and the next
run reads only what arrived since. Skips without a broker on 127.0.0.1:9092.
"""
from __future__ import annotations

import socket
import sqlite3
import uuid
from unittest.mock import patch

import pytest

from tests.test_mx1_03_kafka_client_sasl_matrix import _produce
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
    name = f"d85_{uuid.uuid4().hex[:10]}"
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


def _committed(topic: str) -> int:
    from services.copy_kafka_common import kafka_admin

    group = f"g_{topic}"
    admin = kafka_admin(_BROKER)
    try:
        offsets = admin.list_group_offsets({group: None}).get(group) or {}
    finally:
        admin.close()
    return sum(om.offset for tp, om in offsets.items() if tp.topic == topic)


def _run(topic: str, db: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="kafka", table=topic,
            extra={"group_id": f"g_{topic}"}, **_BROKER,
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        sync_mode="incremental_append",
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _ids(db: str) -> list[int]:
    with sqlite3.connect(db) as conn:
        return sorted(int(r[0]) for r in conn.execute("select id from t"))


def test_gate8_failure_keeps_group_offset_and_rerun_rereads(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(_BROKER, topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])

    forced = {"passed": False, "message": "forced Gate-8 failure"}
    with patch("src.transfer.engine.run_reconciliation", return_value=forced):
        failed = _run(topic, db)
    assert not failed.success
    assert _committed(topic) == 0, "a failed run must not move the group offset"

    rerun = _run(topic, db)
    assert rerun.success, rerun.error
    assert rerun.records_transferred == 3, "the rerun must re-read every message"
    assert _committed(topic) == 3


def test_accepted_run_commits_and_next_run_reads_only_new(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(_BROKER, topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    first = _run(topic, db)
    assert first.success, first.error
    assert first.records_transferred == 3
    assert _committed(topic) == 3

    _produce(_BROKER, topic, [{"id": i, "amount": i * 10} for i in range(4, 6)])
    second = _run(topic, db)
    assert second.success, second.error
    assert second.records_transferred == 2
    assert _committed(topic) == 5
    assert _ids(db) == [1, 2, 3, 4, 5]


def test_continuation_batch_resumes_after_offsets_this_run_read(topic):
    from connectors.kafka_reader import read_topic_batch

    _produce(_BROKER, topic, [{"id": i} for i in range(1, 7)])
    cfg = {**_BROKER, "group_id": f"g_{topic}"}
    first, cursor = read_topic_batch(cfg=cfg, topic=topic, limit=3)
    second, _ = read_topic_batch(cfg=cfg, topic=topic, limit=3, kafka_cursor=cursor)
    id_at = first.headers.index("id")
    assert [r[id_at] for r in first.rows] == ["1", "2", "3"]
    assert [r[second.headers.index("id")] for r in second.rows] == ["4", "5", "6"]
