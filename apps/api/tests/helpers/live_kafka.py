"""The real Kafka broker behind the same small API as ``fake_kafka.FakeBroker``.

The Kafka story tests (D86, D286, D287, D404) were written against the
in-memory broker because no broker ran in the API test job. The kafka-live CI
job starts a single-node KRaft broker on 127.0.0.1:9092; this adapter lets the
same test bodies run against it, so each story is retested live without a
second copy of its assertions:

* ``produce`` writes through a real ``KafkaProducer`` (acks=all, flushed), to
  a topic created with one partition so offsets match the fake's;
* ``group_offsets`` / ``only_group`` / ``commit_calls`` read the consumer
  groups' committed offsets from the broker (``KafkaAdminClient``), which is
  the only state a real consumer group keeps. ``commit_calls`` lists one entry
  per group that committed on a topic this test produced to: "nothing was
  committed" is "no group has an offset there".

Nothing in the product is patched: the reader, the offset commit and the
consumer group all talk to the broker.
"""

from __future__ import annotations

import socket
import time
from typing import Any

BOOTSTRAP = ("127.0.0.1", 9092)


def broker_up(host: str = BOOTSTRAP[0], port: int = BOOTSTRAP[1]) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


class LiveBroker:
    """``FakeBroker``'s test-facing API over a real broker."""

    live = True

    def __init__(self, host: str = BOOTSTRAP[0], port: int = BOOTSTRAP[1]) -> None:
        from kafka import KafkaAdminClient, KafkaProducer

        self.bootstrap = f"{host}:{port}"
        self.topics: set[str] = set()
        self.client_configs: list[dict[str, Any]] = []
        self._admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, request_timeout_ms=15000)
        self._producer = KafkaProducer(bootstrap_servers=self.bootstrap, acks="all", linger_ms=0)

    # -- log -------------------------------------------------------------
    def create_topic(self, topic: str, partitions: int = 1) -> None:
        if topic in self.topics:
            return
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        try:
            self._admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            pass
        self.topics.add(topic)
        # Metadata for a new topic reaches the producer asynchronously.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self._producer.partitions_for(topic):
                return
            time.sleep(0.1)
        raise AssertionError(f"topic {topic} has no partitions after 15 s")

    def produce(self, topic: str, value: Any, key: Any = None, partition: int | None = None) -> int:
        self.create_topic(topic)
        if isinstance(value, str):
            value = value.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")
        meta = self._producer.send(topic, value=value, key=key, partition=partition).get(timeout=15)
        self._producer.flush(timeout=15)
        return int(meta.offset)

    # -- consumer groups ---------------------------------------------------
    def _groups(self) -> list[str]:
        # kafka-python 3 (the image's pin) has list_groups -> dicts; 2.x had
        # list_consumer_groups -> (group_id, protocol) tuples.
        lister = getattr(self._admin, "list_groups", None) or self._admin.list_consumer_groups
        groups = []
        for row in lister():
            if isinstance(row, dict):
                groups.append(str(row["group_id"]))
            else:
                groups.append(str(row[0] if isinstance(row, tuple) else row))
        return groups

    def group_offsets(self, group: str) -> dict[tuple[str, int], int]:
        if hasattr(self._admin, "list_group_offsets"):
            offsets = self._admin.list_group_offsets([group]).get(group) or {}
        else:
            offsets = self._admin.list_consumer_group_offsets(group)
        return {
            (tp.topic, tp.partition): int(meta.offset)
            for tp, meta in offsets.items()
            if int(meta.offset) >= 0
        }

    def _committed_on(self, topics: set[str]) -> list[tuple[str, dict[tuple[str, int], int]]]:
        out = []
        for group in self._groups():
            offs = {tp: off for tp, off in self.group_offsets(group).items() if tp[0] in topics}
            if offs:
                out.append((group, offs))
        return out

    def only_group(self, topic: str) -> str:
        names = [g for g, _offs in self._committed_on({topic})]
        assert len(names) == 1, f"expected one committed group on {topic}, got {names}"
        return names[0]

    @property
    def commit_calls(self) -> list[tuple[str, dict[tuple[str, int], int]]]:
        return self._committed_on(set(self.topics))

    def close(self) -> None:
        if self.topics:
            try:
                self._admin.delete_topics(sorted(self.topics))
            except Exception:  # noqa: BLE001 - cleanup only; the broker is per job
                pass
        self._producer.close(timeout=5)
        self._admin.close()
