"""D84 / D87: a Kafka source reads under its own consumer group.

D84: every Kafka connector without a username shared the consumer group
"dataflow-kafka-source" (with one, the SASL username was the group), and
there was no group_id connector field. D87: two jobs on one connector at the
same time therefore joined one group; the rebalance left one job's first read
empty and Validate reported a false "fidelity collapse".

Now an explicit ``group_id`` (connector field or endpoint extra) wins; a
transfer run otherwise derives a group per route; a read outside a run (a
preview / Validate sample) uses a group of its own that never commits.
"""
from __future__ import annotations

import socket
import sqlite3
import threading
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from connectors.kafka_reader import (
    KAFKA_ROUTE_GROUP_KEY,
    configured_kafka_group_id,
    kafka_route_group_id,
)
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_mx1_03_kafka_client_sasl_matrix import _produce
from tests.test_property2_golden_path_never_blocked import _FakeMongo

_BROKER = {"host": "127.0.0.1", "port": 9092}


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


def _kafka(topic: str, **extra) -> EndpointConfig:
    return EndpointConfig(kind="database", format="kafka", table=topic, extra=extra, **_BROKER)


def _sqlite(db: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=db, table="t")


def test_route_group_is_per_route_and_stable():
    a = kafka_route_group_id(_kafka("orders"), _sqlite("/x/a.db"))
    assert a == kafka_route_group_id(_kafka("orders"), _sqlite("/x/a.db"))
    assert a != kafka_route_group_id(_kafka("orders"), _sqlite("/x/b.db"))
    assert a != kafka_route_group_id(_kafka("payments"), _sqlite("/x/a.db"))
    assert a.startswith("dataflow-kafka-")


def test_explicit_group_wins_over_the_route_group():
    from src.transfer.engine_kafka import stamp_kafka_source

    explicit = TransferRequest(source=_kafka("orders", group_id="team-g"), destination=_sqlite("a"))
    stamp_kafka_source(explicit)
    assert KAFKA_ROUTE_GROUP_KEY not in explicit.source.extra
    assert configured_kafka_group_id(explicit.source.extra) == "team-g"

    derived = TransferRequest(source=_kafka("orders"), destination=_sqlite("a"))
    stamp_kafka_source(derived)
    assert configured_kafka_group_id(derived.source.extra) == kafka_route_group_id(
        derived.source, derived.destination
    )


def test_unconfigured_read_never_joins_a_shared_or_username_group(monkeypatch):
    from connectors import kafka_reader

    seen: list[str] = []

    class _Bridge:
        def __init__(self, config):
            seen.append(config["group_id"])

        def poll_rows(self, **_):
            return iter(())

        def has_assignment(self):
            return True

        def pending_offsets(self):
            return []

        def exhausted(self):
            return False

        def end_offsets_snapshot(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(kafka_reader, "KafkaDebeziumConsumer", _Bridge)
    cfg = {"host": "b", "username": "svc", "password": "pw"}
    kafka_reader.read_topic_batch(cfg=cfg, topic="t")
    kafka_reader.read_topic_batch(cfg=cfg, topic="t")
    assert seen[0] != seen[1], "a preview read must not share a group"
    assert not {"dataflow-kafka-source", "svc"} & set(seen)


def test_saved_connector_group_id_reaches_the_endpoint(monkeypatch, tmp_path: Path):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")
    from src.routers.saved_connectors_router import ConnectorSaveDTO
    from src.transfer.adapters import resolve_endpoint

    body = ConnectorSaveDTO(name="k", type="kafka", role="source", group_id="orders-loader", **_BROKER)
    conn = cs.create_connector(body.model_dump(by_alias=True))
    assert cs.get_connector(conn.id).extra.get("group_id") == "orders-loader"

    endpoint = resolve_endpoint(
        EndpointConfig(kind="database", format="kafka", connector_id=conn.id, table="orders")
    )
    assert configured_kafka_group_id(endpoint.extra) == "orders-loader"


@pytest.fixture
def broker():
    try:
        with socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    topics: list[str] = []
    yield topics
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        admin.delete_topics(topics)
    except Exception:
        pass
    finally:
        admin.close()


def test_two_pipelines_on_one_connector_run_concurrently(broker, tmp_path):
    base = f"d87_{uuid.uuid4().hex[:8]}"
    topics = [f"{base}_a", f"{base}_b"]
    broker.extend(topics)
    for topic in topics:
        _produce(_BROKER, topic, [{"id": i, "amount": i * 10} for i in range(1, 6)])

    results: dict[int, object] = {}
    dbs = [str(tmp_path / f"dest_{i}.db") for i in range(2)]

    def run(i: int) -> None:
        request = TransferRequest(
            source=_kafka(topics[i]),
            destination=_sqlite(dbs[i]),
            sync_mode="incremental_append",
            validation_mode="strict",
        )
        from src.transfer.engine import UniversalTransferEngine

        with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
            results[i] = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])

    threads = [threading.Thread(target=run, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i in range(2):
        assert results[i].success, results[i].error
        assert results[i].records_transferred == 5
        with sqlite3.connect(dbs[i]) as conn:
            assert conn.execute("select count(*) from t").fetchone()[0] == 5

    groups = [results[i].destination_summary["kafka_offsets"]["group_id"] for i in range(2)]
    assert groups[0] != groups[1]
    assert all(g.startswith("dataflow-kafka-") for g in groups)
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_BROKER)
    try:
        for group in groups:
            offsets = admin.list_group_offsets({group: None}).get(group) or {}
            assert sum(om.offset for om in offsets.values()) == 5
        admin.delete_groups(groups)
    finally:
        admin.close()
