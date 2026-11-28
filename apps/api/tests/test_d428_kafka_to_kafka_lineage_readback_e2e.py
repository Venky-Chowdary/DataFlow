"""D428 — a Kafka → Kafka copy that landed every message must verify green.

QA (r66-r75): topic → topic, full_refresh_append with transforms, every message
exact on an independent consumer, yet "Read-back sample verification failed:
row 30 column _kafka_topic→_kafka_topic: source=r66_kfa dest=r66y_kf_kf".

Root cause: a Kafka source row carries its broker lineage (``_kafka_topic``,
``_kafka_partition``, ``_kafka_offset``, ``_kafka_key``) and the copy writes it
into the destination message body as data. The destination read-back decodes
each message with the same reader, which overwrote those body fields with the
*destination* broker's coordinates — so the value compared was never the value
written. The read-back (Gate-8 digest and keyed sample) now reads the written
image: a body field named like an envelope column keeps its written value.

Runs the real engine (reader, writer, Gate-8) on ``tests.helpers.fake_kafka``
and, when the kafka-live CI job's broker is up, on the real broker.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.config  # noqa: E402,F401 - load Settings before a fixture blanks env
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.fake_kafka import FakeBroker  # noqa: E402
from tests.helpers.live_kafka import LiveBroker, broker_up  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Jobs(_FakeMongo):
    def update_job_fields(self, job_id, fields, **_kw):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


@pytest.fixture
def jobs(monkeypatch):
    store = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "d428-test-key")
    return store


@pytest.fixture
def fake_broker(monkeypatch, jobs):
    return FakeBroker().install(monkeypatch)


@pytest.fixture(params=["fake", "live"])
def broker(request, jobs):
    if request.param == "fake":
        yield request.getfixturevalue("fake_broker")
        return
    if not broker_up():
        pytest.skip("Kafka 127.0.0.1:9092 not reachable (the kafka-live CI job starts it)")
    live = LiveBroker()
    try:
        yield live
    finally:
        live.close()


_TRANSFORMS = {"name": "lower", "note": "trim", "payload": "hash_pii", "long_txt": "omit"}


def _seed(broker, topic: str, *, partitions: int = 1) -> None:
    broker.create_topic(topic, partitions=partitions)
    notes = {2: "", 3: None, 5: "  padded  ", 11: "東京 ß 😀"}
    for i in range(1, 31):
        body = {
            "id": i,
            "name": f"Name {i} MiXeD",
            "note": notes.get(i, f"n{i}"),
            "amount": "99999999999.999999" if i == 9 else f"{i}.50",
            "payload": {} if i == 10 else {"k": i, "nested": {"a": [1, 2]}},
            "long_txt": "x" * 200,
        }
        broker.produce(
            topic, json.dumps(body), key=f"key-{i}",
            partition=(i % partitions) if partitions > 1 else None,
        )


def _request(src: str, dst: str, validation_mode: str) -> TransferRequest:
    from connectors.kafka_reader import infer_topic_schema

    schema, _native, _warning = infer_topic_schema({"host": "127.0.0.1", "port": 9092}, src)
    assert {"_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_key"} <= set(schema)
    mappings = []
    for name in sorted(schema):
        row = {"source": name, "target": name}
        if name in _TRANSFORMS:
            row.update(transform=_TRANSFORMS[name], transform_override=True)
        mappings.append(row)
    endpoint = dict(kind="database", format="kafka", host="127.0.0.1", port=9092)
    return TransferRequest(
        source=EndpointConfig(**endpoint, table=src, database=src),
        destination=EndpointConfig(**endpoint, table=dst, database=dst),
        mappings=mappings,
        sync_mode="full_refresh_append",
        skip_preflight=True,
        validation_mode=validation_mode,
    )


def _run(jobs, request: TransferRequest):
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, (jobs.get_job(job_id) or {}).get("reconciliation") or {}


def _names() -> tuple[str, str]:
    tag = uuid.uuid4().hex[:8]
    return f"d428_src_{tag}", f"d428_dst_{tag}"


@pytest.mark.parametrize("validation_mode", ["strict", "balanced"])
def test_exact_kafka_to_kafka_copy_is_green(broker, jobs, validation_mode):
    src, dst = _names()
    _seed(broker, src)
    result, recon = _run(jobs, _request(src, dst, validation_mode))
    assert result.success, result.error
    assert result.records_transferred == 30
    assert recon.get("passed") is True, recon.get("message")
    # Not vacuous: the keyed sample compared rows (an omit used to exclude all).
    assert int((recon.get("sample_compare") or {}).get("rows_compared") or 0) > 0, recon.get(
        "sample_compare"
    )


def test_keyed_three_partition_source_into_an_occupied_topic(broker, jobs):
    """Partition numbers and offsets differ on the destination; the copy is exact."""
    src, dst = _names()
    _seed(broker, src, partitions=3)
    broker.produce(dst, json.dumps({"id": 900, "note": "already here"}))
    result, recon = _run(jobs, _request(src, dst, "strict"))
    assert result.success, result.error
    assert result.records_transferred == 30
    assert recon.get("passed") is True, recon.get("message")
    # Not vacuous: the keyed sample compared rows (an omit used to exclude all).
    assert int((recon.get("sample_compare") or {}).get("rows_compared") or 0) > 0, recon.get(
        "sample_compare"
    )


def test_a_wrong_data_field_still_fails_and_is_named(fake_broker, jobs, monkeypatch):
    """AC-3: a genuine mismatch in a data field fails, naming that field."""
    src, dst = _names()
    _seed(fake_broker, src)
    real_produce = fake_broker.produce

    def _corrupting(topic, value, key=None, partition=None):
        if topic == dst and value:
            body = json.loads(value)
            if body.get("id") == 7:
                body["note"] = "tampered"
                value = json.dumps(body).encode("utf-8")
        return real_produce(topic, value, key=key, partition=partition)

    monkeypatch.setattr(fake_broker, "produce", _corrupting)
    result, recon = _run(jobs, _request(src, dst, "balanced"))
    assert not result.success
    text = json.dumps(recon, default=str)
    assert "note" in text and "tampered" in text, recon.get("message")
    assert "_kafka_topic" not in str(recon.get("message") or "")


def test_a_lineage_field_the_write_dropped_is_not_filled_in_from_the_broker(
    fake_broker, jobs, monkeypatch
):
    """Single partition: source and destination offsets are both 0..29, so a
    dropped ``_kafka_offset`` filled in from the destination broker would
    match by coincidence. The read-back takes no envelope from the broker
    when the body carries lineage, so the missing field is a mismatch."""
    src, dst = _names()
    _seed(fake_broker, src)
    real_produce = fake_broker.produce

    def _dropping(topic, value, key=None, partition=None):
        if topic == dst and value:
            body = json.loads(value)
            body.pop("_kafka_offset", None)
            value = json.dumps(body).encode("utf-8")
        return real_produce(topic, value, key=key, partition=partition)

    monkeypatch.setattr(fake_broker, "produce", _dropping)
    result, recon = _run(jobs, _request(src, dst, "strict"))
    assert not result.success
    assert recon.get("passed") is not True, recon.get("message")
