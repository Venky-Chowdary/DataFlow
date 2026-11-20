"""An in-memory Kafka broker behind kafka-python's client API, for engine tests.

No broker runs in the API test job. Unit fakes elsewhere stub one reader
function at a time; this one replaces ``kafka.KafkaConsumer`` /
``KafkaProducer`` / ``KafkaAdminClient`` so the product's real reader,
bridge, sampler, type census and offset commit all run unchanged against it.

What it keeps faithful to a broker (Apache Kafka consumer semantics,
https://kafka.apache.org/documentation/#consumerconfigs and the kafka-python
``KafkaConsumer`` reference):

* a partitioned, append-only log per topic; offsets start at 0 and the end
  offset is the next offset to be written;
* consumer groups own committed offsets; ``commit`` writes the *next* offset
  to read; nothing moves a group's position except an explicit commit
  (``enable_auto_commit=False`` is the only mode the product uses);
* a subscribed consumer starts at the group's committed offset, else at
  ``auto_offset_reset``; the rebalance listener runs on assignment, before
  the first fetch, so its ``seek`` wins;
* an assigned consumer (``assign``) joins no group;
* ``committed()`` without a deadline blocks while the group coordinator is
  unavailable (kafka-python retries forever): the fake raises instead of
  hanging the test, so a caller that forgets the deadline fails loudly.
"""

from __future__ import annotations

import threading
from collections import namedtuple
from typing import Any

from kafka import OffsetAndMetadata, TopicPartition  # noqa: F401 - re-exported
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaTimeoutError


class CoordinatorBlockedForever(AssertionError):
    """``committed()`` was called with no deadline while the coordinator was down."""


_Message = namedtuple("_Message", "key value timestamp")


class FakeBroker:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.logs: dict[str, list[list[_Message]]] = {}
        self.groups: dict[str, dict[TopicPartition, int]] = {}
        self.commit_calls: list[tuple[str, dict[TopicPartition, int]]] = []
        self.coordinator_down = False
        #: Every client's constructor configs, to check the listener each path dialled.
        self.client_configs: list[dict[str, Any]] = []
        self._clock = 1_700_000_000_000

    # -- log -------------------------------------------------------------
    def create_topic(self, topic: str, partitions: int = 1) -> None:
        with self.lock:
            self.logs.setdefault(topic, [[] for _ in range(partitions)])

    def produce(self, topic: str, value: Any, key: Any = None, partition: int | None = None) -> int:
        with self.lock:
            self.create_topic(topic)
            parts = self.logs[topic]
            if partition is None:
                partition = (hash(key) % len(parts)) if key is not None else 0
            if isinstance(value, str):
                value = value.encode("utf-8")
            if isinstance(key, str):
                key = key.encode("utf-8")
            self._clock += 1
            parts[partition].append(_Message(key, value, self._clock))
            return len(parts[partition]) - 1

    def end(self, tp: TopicPartition) -> int:
        return len(self.logs.get(tp.topic, [[]] * (tp.partition + 1))[tp.partition])

    def partitions(self, topic: str) -> set[int] | None:
        parts = self.logs.get(topic)
        return set(range(len(parts))) if parts else None

    def committed(self, group: str, tp: TopicPartition) -> int | None:
        return self.groups.get(group, {}).get(tp)

    def group_offsets(self, group: str) -> dict[tuple[str, int], int]:
        return {(tp.topic, tp.partition): off for tp, off in self.groups.get(group, {}).items()}

    def only_group(self, topic: str) -> str:
        names = [g for g, offs in self.groups.items() if any(tp.topic == topic for tp in offs)]
        assert len(names) == 1, f"expected one committed group on {topic}, got {names}"
        return names[0]

    # -- client classes bound to this broker ------------------------------
    def classes(self) -> dict[str, type]:
        broker = self
        from kafka import KafkaConsumer as _Real

        class FakeKafkaConsumer:
            DEFAULT_CONFIG = dict(_Real.DEFAULT_CONFIG)

            def __init__(self, *topics: str, **configs: Any) -> None:
                broker.client_configs.append(dict(configs))
                self.config = configs
                self.group = configs.get("group_id")
                self.reset = str(configs.get("auto_offset_reset") or "latest")
                self._deser = configs.get("value_deserializer")
                self._subscribed: list[str] = []
                self._listener = None
                self._assigned: list[TopicPartition] = []
                self._pos: dict[TopicPartition, int] = {}
                self._joined = False
                self.closed = False
                if topics:
                    self.subscribe(list(topics))

            # group membership
            def subscribe(self, topics=(), pattern=None, listener=None):
                self._subscribed = list(topics)
                self._listener = listener
                self._joined = False
                self._assigned = []

            def _join(self) -> None:
                if self._joined or not self._subscribed:
                    return
                self._joined = True
                tps = []
                for t in self._subscribed:
                    for p in sorted(broker.partitions(t) or ()):
                        tps.append(TopicPartition(t, p))
                self._assigned = tps
                for tp in tps:
                    c = broker.committed(self.group, tp) if self.group else None
                    if c is not None:
                        self._pos[tp] = c
                    else:
                        self._pos[tp] = 0 if self.reset == "earliest" else broker.end(tp)
                if self._listener is not None:
                    self._listener.on_partitions_assigned(set(tps))

            def assign(self, partitions):
                self._subscribed = []
                self._assigned = list(partitions)
                for tp in self._assigned:
                    self._pos.setdefault(tp, 0)

            def assignment(self):
                return set(self._assigned)

            def topics(self):
                return set(broker.logs)

            def partitions_for_topic(self, topic):
                return broker.partitions(topic)

            # positions
            def seek(self, partition, offset):
                self._pos[partition] = int(offset)

            def seek_to_beginning(self, *partitions):
                for tp in partitions or self._assigned:
                    self._pos[tp] = 0

            def position(self, partition, timeout_ms=None):
                return self._pos.get(partition)

            def beginning_offsets(self, partitions, timeout_ms=None):
                return {tp: 0 for tp in partitions}

            def end_offsets(self, partitions, timeout_ms=None):
                with broker.lock:
                    return {tp: broker.end(tp) for tp in partitions}

            def committed(self, partition, metadata=False, timeout_ms=None):
                if broker.coordinator_down:
                    if timeout_ms is None:
                        raise CoordinatorBlockedForever(
                            "committed() without timeout_ms retries an unavailable "
                            "coordinator forever"
                        )
                    raise KafkaTimeoutError("group coordinator not available")
                return broker.committed(self.group, partition)

            def commit(self, offsets=None, timeout_ms=None):
                assert self.group, "commit needs a group_id"
                assert offsets, "the product always commits explicit offsets"
                with broker.lock:
                    written = {tp: int(meta.offset) for tp, meta in offsets.items()}
                    broker.groups.setdefault(self.group, {}).update(written)
                    broker.commit_calls.append((self.group, written))

            def poll(self, timeout_ms=0, max_records=None, update_offsets=True):
                self._join()
                budget = int(max_records or 500)
                out: dict[TopicPartition, list[Any]] = {}
                with broker.lock:
                    for tp in self._assigned:
                        if budget <= 0:
                            break
                        log = broker.logs.get(tp.topic, [])
                        msgs = log[tp.partition] if tp.partition < len(log) else []
                        start = self._pos.get(tp, 0)
                        chunk = msgs[start:start + budget]
                        if not chunk:
                            continue
                        recs = []
                        for i, m in enumerate(chunk):
                            value = self._deser(m.value) if self._deser else m.value
                            recs.append(ConsumerRecord(
                                tp.topic, tp.partition, -1, start + i, m.timestamp, 0,
                                m.key, value, [], None,
                                len(m.key or b""), len(m.value or b""), -1,
                            ))
                        out[tp] = recs
                        budget -= len(recs)
                        if update_offsets:
                            self._pos[tp] = start + len(recs)
                return out

            def close(self, autocommit=True, timeout_ms=None):
                self.closed = True

        class FakeKafkaProducer:
            def __init__(self, **configs: Any) -> None:
                broker.client_configs.append(dict(configs))
                self._key_ser = configs.get("key_serializer")
                self._val_ser = configs.get("value_serializer")

            def send(self, topic, value=None, key=None, partition=None, **_kw):
                if self._val_ser and value is not None:
                    value = self._val_ser(value)
                if self._key_ser and key is not None:
                    key = self._key_ser(key)
                broker.produce(topic, value, key=key, partition=partition)

                class _Future:
                    def get(self, timeout=None):
                        return None

                return _Future()

            def flush(self, timeout=None):
                return None

            def close(self, timeout=None):
                return None

        class FakeKafkaAdminClient:
            def __init__(self, **configs: Any) -> None:
                pass

            def create_topics(self, new_topics, **_kw):
                for nt in new_topics:
                    broker.create_topic(nt.name, int(getattr(nt, "num_partitions", 1) or 1))

            def delete_topics(self, topics, **_kw):
                for t in topics:
                    broker.logs.pop(t, None)

            def close(self):
                return None

        return {
            "KafkaConsumer": FakeKafkaConsumer,
            "KafkaProducer": FakeKafkaProducer,
            "KafkaAdminClient": FakeKafkaAdminClient,
        }

    def install(self, monkeypatch: Any) -> "FakeBroker":
        """Patch kafka-python's client classes; the SASL socket check never runs."""
        import kafka

        import services.copy_kafka_common as kafka_common

        for name, cls in self.classes().items():
            monkeypatch.setattr(kafka, name, cls, raising=False)
        monkeypatch.setattr(kafka_common, "kafka_sasl_check", lambda cfg, timeout=5.0: None)
        return self
