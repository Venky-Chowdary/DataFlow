"""D83 (live SASL_PLAINTEXT broker): the SASL config reaches every data path.

Test passed on a SASL_PLAINTEXT connector, but introspection rebuilt the
Kafka cfg without the security protocol / mechanism, so SASL_SSL was assumed
and the TLS handshake failed; the producer and the Gate-8 read-back dropped
the same fields. A connector keeps them either in the form slots (protocol
in ``schema``, mechanism in ``database``) or under ``extra``; both shapes
must introspect, list topics, read, write and read back.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from unittest.mock import patch

import pytest

from tests.test_mx1_03_kafka_client_sasl_matrix import _live_cfg, _produce, live
from tests.test_property2_golden_path_never_blocked import _FakeMongo

SHAPES = {
    "form": {"schema": "SASL_PLAINTEXT", "database": "PLAIN"},
    "extra": {"extra": {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN"}},
}


class _Mongo(_FakeMongo):
    def update_job_fields(self, *a, **k):
        return None


def _credentials() -> dict:
    cfg = _live_cfg()
    return {k: cfg[k] for k in ("host", "port", "username", "password")}


def _run(source, destination):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    request = TransferRequest(
        source=source, destination=destination,
        sync_mode="full_refresh_append", validation_mode="strict",
    )
    with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
        return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@live
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_sasl_plaintext_connector_introspects_and_lists(shape):
    from services.schema_introspect import introspect_schema

    topic = f"d83_{shape}_{uuid.uuid4().hex[:8]}"
    _produce(_live_cfg(), topic, [{"id": 1, "v": "a"}])
    info = introspect_schema("kafka", table=topic, **_credentials(), **SHAPES[shape])
    assert info["ok"], info.get("error")
    assert [c["name"] for c in info["columns"]][:2] == ["id", "v"]
    listing = introspect_schema("kafka", table="", **_credentials(), **SHAPES[shape])
    assert listing["ok"], listing.get("error")
    assert topic in listing["tables"]


@live
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_sasl_plaintext_connector_reads_writes_and_reads_back(shape, tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    from src.transfer.models import EndpointConfig

    creds = _credentials()
    topic = f"d83_{shape}_{uuid.uuid4().hex[:8]}"
    kafka_fields = dict(SHAPES[shape])
    extra = kafka_fields.pop("extra", {})

    _produce(_live_cfg(), topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    db = str(tmp_path / "dest.db")
    read = _run(
        EndpointConfig(kind="database", format="kafka", table=topic, extra=extra,
                       **creds, **kafka_fields),
        EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
    )
    assert read.success, read.error
    with sqlite3.connect(db) as conn:
        assert conn.execute("select count(*) from t").fetchone()[0] == 3

    out_topic = f"{topic}_out"
    wrote = _run(
        EndpointConfig(kind="database", format="sqlite", database=db, table="t"),
        EndpointConfig(kind="database", format="kafka", table=out_topic, extra=extra,
                       **creds, **kafka_fields),
    )
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers="127.0.0.1:9092")
    try:
        parts = consumer.partitions_for_topic(out_topic) or set()
        end = consumer.end_offsets([TopicPartition(out_topic, p) for p in parts])
    finally:
        consumer.close()
    assert sum(end.values()) == 3, wrote.error
    # The Gate-8 read-back dialed the SASL listener and saw every message.
    assert wrote.reconciliation.get("target_rows") == 3, wrote.reconciliation
    assert os.environ.get("DATAFLOW_LIVE_KAFKA_SASL_PASSWORD")
