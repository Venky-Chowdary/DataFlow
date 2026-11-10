"""D287 - a Kafka-source job froze with no heartbeat after its rows had landed.

Two causes, both on the Kafka page read every phase of a run goes through
(peek, uniqueness probe, sizing probe, batches):

* the run's offset snapshot asked the group for its committed position with
  ``committed(tp)`` and no deadline. kafka-python retries a silent or
  unavailable coordinator forever, so an incremental run that lost its broker
  after the partition assignment never returned;
* while a page read waited on the broker nothing reached the job row: it kept
  its last message ("Writing batch 1/2 (10 rows)...") and no ``heartbeat_at``
  stamp, for minutes, until it was cancelled.
"""
from __future__ import annotations

import json
import select
import socket
import threading
import time
import uuid

import pytest

from tests.test_property2_golden_path_never_blocked import _FakeMongo


class _Store(_FakeMongo):
    def __init__(self) -> None:
        super().__init__()
        self.stamps: list[dict] = []

    def update_job_fields(self, job_id, fields, **kwargs):
        if "heartbeat_at" in (fields or {}):
            self.stamps.append(dict(fields))
        return super().update_job_fields(job_id, fields)


class _OffsetConsumer:
    """Records every offset lookup the snapshot makes."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def assignment(self):
        from kafka import TopicPartition

        return {TopicPartition("t", 0), TopicPartition("t", 1)}

    def beginning_offsets(self, tps, timeout_ms=None):
        self.calls.append(("beginning_offsets", timeout_ms))
        return {tp: 0 for tp in tps}

    def end_offsets(self, tps, timeout_ms=None):
        self.calls.append(("end_offsets", timeout_ms))
        return {tp: 5 for tp in tps}

    def committed(self, tp, metadata=False, timeout_ms=None):
        self.calls.append(("committed", timeout_ms))
        return 2


def test_every_offset_lookup_of_the_snapshot_has_a_deadline():
    pytest.importorskip("kafka")
    from connectors.kafka_debezium_bridge import KafkaDebeziumConsumer

    reader = KafkaDebeziumConsumer({"topic": "t", "start_position": "committed"})
    fake = _OffsetConsumer()
    reader._consumer = fake
    reader._snapshot_end_offsets()

    names = sorted({name for name, _ in fake.calls})
    assert names == ["beginning_offsets", "committed", "end_offsets"]
    for name, timeout_ms in fake.calls:
        assert timeout_ms is not None and 0 < timeout_ms <= 30_000, (name, timeout_ms)
    assert reader.end_offsets_snapshot()[0]["start"] == 2


def test_a_waiting_kafka_page_read_pulses_the_job_heartbeat(monkeypatch):
    import connectors.kafka_reader as kafka_reader
    import services.cancel_check as cc
    from src.transfer.batch_readers import _read_batch_unscoped

    store = _Store()
    store.jobs["j"] = {"status": "running", "message": "Writing batch 1/2 (10 rows)…"}
    monkeypatch.setattr(cc, "BLOCKING_STEP_PULSE_SECONDS", 0.05)

    def _slow_read(**_kwargs):
        time.sleep(0.6)
        return object(), None

    monkeypatch.setattr(kafka_reader, "read_topic_batch", _slow_read)
    cc.set_progress_meter(cc.ProgressMeter(store, "j", interval=0))
    try:
        _read_batch_unscoped("kafka", {"host": "127.0.0.1"}, "orders", None, 0, 500)
    finally:
        cc.set_progress_meter(None)

    assert store.stamps, "the page read waited 0.6 s and never stamped the heartbeat"
    message = store.stamps[-1]["message"]
    assert "Reading Kafka topic 'orders'" in message and "elapsed" in message
    # The pulse leaves the run's phase alone and never touches updated_at.
    assert all("phase" not in s and "updated_at" not in s for s in store.stamps)


def test_no_pulse_for_a_cancelled_job(monkeypatch):
    import connectors.kafka_reader as kafka_reader
    import services.cancel_check as cc
    from src.transfer.batch_readers import _read_batch_unscoped

    store = _Store()
    store.jobs["j"] = {"status": "cancelled", "message": "Cancelled"}
    monkeypatch.setattr(cc, "BLOCKING_STEP_PULSE_SECONDS", 0.05)
    monkeypatch.setattr(
        kafka_reader, "read_topic_batch", lambda **_k: (time.sleep(0.3), None)
    )
    cc.set_progress_meter(cc.ProgressMeter(store, "j", interval=0))
    try:
        _read_batch_unscoped("kafka", {"host": "127.0.0.1"}, "orders", None, 0, 500)
    finally:
        cc.set_progress_meter(None)
    assert store.stamps == []
    assert store.jobs["j"]["message"] == "Cancelled"


# --- live: the broker goes silent right after the partition assignment -------

_BROKER = ("127.0.0.1", 9092)


def _broker_up() -> bool:
    try:
        with socket.create_connection(_BROKER, timeout=1):
            return True
    except OSError:
        return False


class _SilencingRelay:
    """Forwards to the broker until ``silence`` is set, then holds every
    connection open and forwards nothing (a broker that stopped answering)."""

    def __init__(self) -> None:
        self.silence = threading.Event()
        self._closed = threading.Event()
        self._srv = socket.socket()
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(64)
        self._srv.settimeout(0.2)
        self.port = self._srv.getsockname()[1]
        self._socks: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._srv.accept()
            except OSError:
                continue
            self._socks.append(client)
            if self.silence.is_set():
                continue
            broker = socket.create_connection(_BROKER)
            self._socks.append(broker)
            threading.Thread(target=self._pump, args=(client, broker), daemon=True).start()

    def _pump(self, client: socket.socket, broker: socket.socket) -> None:
        while not self._closed.is_set():
            try:
                ready, _, _ = select.select([client, broker], [], [], 0.2)
            except (OSError, ValueError):
                return
            if self.silence.is_set():
                continue
            for sock in ready:
                data = sock.recv(65536)
                if not data:
                    return
                (broker if sock is client else client).sendall(data)

    def close(self) -> None:
        self._closed.set()
        for sock in [self._srv, *self._socks]:
            try:
                sock.close()
            except OSError:
                pass


@pytest.mark.skipif(not _broker_up(), reason="Kafka 127.0.0.1:9092 not reachable")
def test_incremental_page_read_ends_when_the_broker_goes_silent(monkeypatch):
    import kafka
    import kafka.net.manager as manager
    from kafka import KafkaProducer

    from connectors.kafka_reader import read_topic_batch

    topic = f"d287_{uuid.uuid4().hex[:10]}"
    producer = KafkaProducer(bootstrap_servers="%s:%d" % _BROKER)
    try:
        for i in range(5):
            producer.send(topic, json.dumps({"id": i}).encode())
        producer.flush(10)
    finally:
        producer.close()

    relay = _SilencingRelay()
    original = manager.create_connection

    async def _via_relay(net, host, port, *args, **kwargs):
        if int(port) == _BROKER[1]:
            host, port = "127.0.0.1", relay.port
        return await original(net, host, port, *args, **kwargs)

    monkeypatch.setattr(manager, "create_connection", _via_relay)
    real_committed = kafka.KafkaConsumer.committed

    def _silence_then_committed(self, *args, **kwargs):
        relay.silence.set()
        return real_committed(self, *args, **kwargs)

    monkeypatch.setattr(kafka.KafkaConsumer, "committed", _silence_then_committed)

    outcome: dict = {}

    def _read() -> None:
        try:
            outcome["result"] = read_topic_batch(
                cfg={
                    "host": "127.0.0.1",
                    "port": _BROKER[1],
                    "extra": {
                        "group_id": f"d287-{uuid.uuid4().hex[:8]}",
                        "kafka_start_position": "committed",
                    },
                },
                topic=topic,
                limit=500,
            )
        except Exception as exc:  # noqa: BLE001 - the outcome is the assertion
            outcome["error"] = exc

    started = time.monotonic()
    worker = threading.Thread(target=_read, daemon=True)
    worker.start()
    try:
        worker.join(90)
        assert relay.silence.is_set(), "the read never reached the committed lookup"
        assert not worker.is_alive(), (
            "the page read was still blocked 90 s after the broker went silent"
        )
        assert "error" in outcome, outcome
        assert time.monotonic() - started < 90
    finally:
        relay.close()
