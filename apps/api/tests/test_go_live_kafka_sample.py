"""Kafka sample reads records that are already on the topic.

The transfer reader subscribes with the pipeline consumer group. After a
load that group is committed at the end, and the first poll during a
rebalance is empty, so sample_connector_object reported 0 rows.
"""

from __future__ import annotations

import sys
import types

from connectors.base import ReadBatch


class _Partition:
    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _Partition)
            and self.topic == other.topic
            and self.partition == other.partition
        )

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))


class _Message:
    def __init__(self, value: bytes, offset: int) -> None:
        self.value = value
        self.offset = offset
        self.topic = "orders"
        self.partition = 0


class _Consumer:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.seeks: list[tuple[_Partition, int]] = []
        self.assigned = None
        self.closed = False
        self.committed = False
        self._meta_loads = 0

    def partitions_for_topic(self, topic: str):
        self._meta_loads += 1
        if self._meta_loads == 1:
            return None
        assert topic == "orders"
        return {0}

    def topics(self):
        return {"orders"}

    def assign(self, tps) -> None:
        self.assigned = list(tps)

    def beginning_offsets(self, tps):
        return {tp: 0 for tp in tps}

    def end_offsets(self, tps):
        return {tp: 10 for tp in tps}

    def seek(self, tp, offset: int) -> None:
        self.seeks.append((tp, offset))

    def poll(self, timeout_ms: int = 0, max_records: int = 0):
        del timeout_ms, max_records
        if getattr(self, "_polled", False):
            return {}
        self._polled = True
        tp = self.assigned[0]
        return {tp: [_Message(b'{"id": 7, "name": "ada"}', 7)]}

    def commit(self, *_a, **_k) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


def _install_kafka(monkeypatch, consumer: _Consumer) -> None:
    module = types.ModuleType("kafka")
    module.TopicPartition = _Partition

    class _KafkaConsumer:
        DEFAULT_CONFIG: dict = {}

        def __init__(self, **kwargs):
            consumer.kwargs = kwargs
            self._inner = consumer

        def __getattr__(self, name):
            return getattr(consumer, name)

    module.KafkaConsumer = _KafkaConsumer
    monkeypatch.setitem(sys.modules, "kafka", module)


def test_sample_seeks_the_tail_and_does_not_join_the_group(monkeypatch) -> None:
    from connectors.kafka_reader import sample_topic_batch

    consumer = _Consumer()
    _install_kafka(monkeypatch, consumer)
    batch = sample_topic_batch(
        cfg={"host": "127.0.0.1", "port": 9092, "group_id": "dataflow-kafka-source"},
        topic="orders",
        limit=3,
    )
    assert "group_id" not in consumer.kwargs
    assert consumer.kwargs["enable_auto_commit"] is False
    assert consumer.seeks[0][1] == 7
    assert consumer.committed is False
    assert consumer.closed is True
    assert batch.headers == ["id", "name"]
    assert batch.rows == [["7", "ada"]]
    assert batch.meta["native_types"]["id"] == "INTEGER"


def test_an_empty_topic_stays_empty(monkeypatch) -> None:
    from connectors.kafka_reader import sample_topic_batch

    consumer = _Consumer()
    consumer.end_offsets = lambda tps: {tp: 0 for tp in tps}
    consumer.poll = lambda **_k: {}
    _install_kafka(monkeypatch, consumer)
    batch = sample_topic_batch(cfg={"host": "localhost"}, topic="orders", limit=5)
    assert batch.headers == []
    assert batch.rows == []
    assert consumer.seeks[0][1] == 0
    assert consumer.committed is False


def test_pilot_kafka_sample_uses_the_tail_reader(monkeypatch) -> None:
    from src.ai.copilot.query_tools import _sample_batch_source

    seen: dict = {}

    def _sample(**kwargs):
        seen.update(kwargs)
        batch = ReadBatch(headers=["id"], rows=[["1"]], offset=0, total_rows=1)
        batch.meta = {"native_types": {"id": "INTEGER"}}
        return batch

    def _transfer(*_a, **_k):
        raise AssertionError("the transfer group reader must not sample")

    monkeypatch.setattr("connectors.kafka_reader.sample_topic_batch", _sample)
    monkeypatch.setattr("src.transfer.stream._read_batch", _transfer)
    rows, columns, schema = _sample_batch_source(
        {"type": "kafka", "host": "k.internal", "port": 9092, "group_id": "pipeline"},
        "orders",
        10,
    )
    assert columns == ["id"]
    assert rows == [{"id": "1"}]
    assert schema["id"] == "INTEGER"
    assert seen["topic"] == "orders"
    assert seen["limit"] == 10
