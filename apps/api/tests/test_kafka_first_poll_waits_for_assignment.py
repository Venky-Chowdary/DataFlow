"""A fresh consumer group has no partitions until the group join finishes.

The broker delays the first join (group.initial.rebalance.delay.ms, 3 s by
default). read_topic_batch used to stop at the first empty 800 ms poll, so a
new group's first batch came back empty even with messages on the topic.
An empty poll only means "caught up" once partitions are assigned.
"""
from __future__ import annotations

import uuid

from tests.test_mx1_03_kafka_client_sasl_matrix import _live_cfg, _produce, live


class _JoiningBridge:
    """Empty, unassigned polls until the join lands, then the rows."""

    instances: list = []

    def __init__(self, config, *, join_after=4, rows=3):
        self.polls = 0
        self.join_after = join_after
        self.rows = rows
        self.closed = False
        _JoiningBridge.instances.append(self)

    def has_assignment(self):
        return self.polls >= self.join_after

    def poll_rows(self, **_):
        self.polls += 1
        if self.polls < self.join_after:
            return iter(())
        if self.polls == self.join_after:
            return iter([{"id": i} for i in range(self.rows)])
        return iter(())

    def pending_offsets(self):
        if self.rows and self.polls >= self.join_after:
            return [{"topic": "t", "partition": 0, "offset": self.rows}]
        return []

    def exhausted(self):
        return False

    def end_offsets_snapshot(self):
        return []

    def close(self):
        self.closed = True


def test_empty_polls_before_assignment_are_not_end_of_topic(monkeypatch):
    from connectors import kafka_reader

    monkeypatch.setattr(kafka_reader, "KafkaDebeziumConsumer", _JoiningBridge)
    batch, cursor = kafka_reader.read_topic_batch(cfg={"host": "b"}, topic="t", limit=50)
    assert len(batch.rows) == 3
    assert cursor and cursor["pending_offsets"]


class _CaughtUpBridge(_JoiningBridge):
    """Knows where the log ends: caught up only once its rows were read."""

    def caught_up(self):
        return self.rows == 0 or self.polls >= self.join_after


def test_assigned_empty_poll_ends_the_batch_once_caught_up(monkeypatch):
    from connectors import kafka_reader

    monkeypatch.setattr(
        kafka_reader, "KafkaDebeziumConsumer", lambda c: _CaughtUpBridge(c, join_after=1, rows=0)
    )
    batch, cursor = kafka_reader.read_topic_batch(cfg={"host": "b"}, topic="t", limit=50)
    assert len(batch.rows) == 0 and cursor is None
    assert _JoiningBridge.instances[-1].polls == 1


class _AssignedBeforeFetchBridge(_CaughtUpBridge):
    """D151: the join lands at the end of a poll; the next polls fetch nothing yet."""

    def has_assignment(self):
        return True


def test_first_empty_poll_after_the_join_is_not_end_of_topic(monkeypatch):
    """The join can land before any start offset is looked up or fetched (D151)."""
    from connectors import kafka_reader

    monkeypatch.setattr(
        kafka_reader, "KafkaDebeziumConsumer",
        lambda c: _AssignedBeforeFetchBridge(c, join_after=3, rows=4),
    )
    batch, cursor = kafka_reader.read_topic_batch(cfg={"host": "b"}, topic="t", limit=50)
    assert len(batch.rows) == 4 and cursor and cursor["pending_offsets"]


def test_assigned_but_nothing_arrives_fails_loudly(monkeypatch):
    """Unread messages that never arrive are a broker problem, not an empty topic."""
    import pytest

    from connectors import kafka_reader

    monkeypatch.setattr(kafka_reader, "_JOIN_WAIT_S", 0.0)
    monkeypatch.setattr(
        kafka_reader, "KafkaDebeziumConsumer",
        lambda c: _AssignedBeforeFetchBridge(c, join_after=10**9, rows=4),
    )
    with pytest.raises(kafka_reader.KafkaFirstFetchTimeout, match="Nothing was written"):
        kafka_reader.read_topic_batch(cfg={"host": "b"}, topic="t", limit=50)


def test_join_wait_is_bounded(monkeypatch):
    from connectors import kafka_reader

    monkeypatch.setattr(kafka_reader, "_JOIN_WAIT_S", 0.0)
    monkeypatch.setattr(
        kafka_reader, "KafkaDebeziumConsumer", lambda c: _JoiningBridge(c, join_after=10**9)
    )
    batch, _ = kafka_reader.read_topic_batch(cfg={"host": "b"}, topic="t", limit=50)
    assert len(batch.rows) == 0 and _JoiningBridge.instances[-1].closed


@live
def test_live_fresh_group_reads_commits_and_resumes():
    from connectors import kafka_reader

    topic = f"mx103_join_{uuid.uuid4().hex[:10]}"
    _produce(_live_cfg(), topic, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    cfg = _live_cfg(group_id=f"g_{topic}")
    batch, cursor = kafka_reader.read_topic_batch(cfg=cfg, topic=topic, limit=50)
    assert len(batch.rows) == 3 and cursor and cursor["pending_offsets"]
    kafka_reader.commit_kafka_offsets(cfg, cursor)
    again, cursor2 = kafka_reader.read_topic_batch(cfg=cfg, topic=topic, limit=50)
    assert len(again.rows) == 0 and cursor2 is None
