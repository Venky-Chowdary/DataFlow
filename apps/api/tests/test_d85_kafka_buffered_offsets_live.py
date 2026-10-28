"""D85 (live broker): the buffered (non-streaming) path commits Kafka offsets too.

A priority column routes a run through the buffered read. That read dropped
the reader's offset cursor, so no run ever committed and every rerun re-read
and re-delivered the same messages. The cursor now rides the read stamp to the
accept step: an accepted run commits, a failed one leaves the group alone.
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
    name = f"d85b_{uuid.uuid4().hex[:10]}"
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
        priority_column="id",
        validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def test_buffered_run_commits_on_accept_and_rerun_reads_only_new(topic, tmp_path):
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
    with sqlite3.connect(db) as conn:
        assert conn.execute("select count(*) from t").fetchone()[0] == 5


def test_buffered_failed_run_leaves_the_group_offset(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(_BROKER, topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    with patch(
        "src.transfer.engine.run_reconciliation",
        return_value={"passed": False, "message": "forced Gate-8 failure"},
    ):
        failed = _run(topic, db)
    assert not failed.success
    assert _committed(topic) == 0
