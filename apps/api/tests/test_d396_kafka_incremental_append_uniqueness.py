"""D396 (Kafka variant) — an incremental Kafka run's uniqueness is judged on its own messages.

QA r62-r70: a keyed Kafka -> PostgreSQL incremental_append schedule (no column
cursor; the consumer-group offset is the cursor) loaded run 1, then refused
runs 2 and 3 with "Duplicate identity keys: Key id repeats in the source
(5x2, 3x2)". Run 2 reads only the messages past the group's committed offset
(ids 5, 3, 6): no key repeats there.

Root cause: the source uniqueness probe read the topic from the log start under
a throwaway consumer group, so every earlier version of a key counted as a
duplicate of its later version (R3: a check must read the rows the run reads).
It now starts at the route group's committed offsets, read without joining the
group (``kafka_reader.group_committed_offsets``).

The e2e case runs on the in-memory fake broker everywhere and on the real
broker in the kafka-live CI job (``broker`` fixture of the P2 Kafka suite).
"""

from __future__ import annotations

import json

import pytest

from tests.test_p2_kafka_source_runs_against_fake_broker import (  # noqa: F401 - fixtures
    _names,
    _request,
    _rows,
    _run,
    _schema,
    broker,
    fake_broker,
    pg,
)


def _probe(topic: str, extra: dict) -> list:
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    result = probe_source_duplicate_keys_result(
        source_config={"type": "kafka", "host": "127.0.0.1", "port": 9092, "table": topic,
                       "database": topic, "extra": extra},
        source_table=topic,
        primary_key="id",
    )
    assert result.status == "ran", result.message
    return [(str(f.get("value")), int(f.get("count") or 0)) for f in result.findings]


def test_incremental_probe_reads_past_the_committed_offset_only(fake_broker):
    from kafka import TopicPartition

    topic = _names("uniq")[0]
    for i in range(1, 6):
        fake_broker.produce(topic, json.dumps({"id": i, "name": f"v1-{i}"}))
    group = "dataflow-kafka-route-under-test"
    fake_broker.groups[group] = {TopicPartition(topic, 0): 5}  # run 1 accepted
    for i, name in ((5, "v2-5"), (3, "v2-3"), (6, "v1-6")):
        fake_broker.produce(topic, json.dumps({"id": i, "name": name}))

    incremental = {"kafka_start_position": "committed", "kafka_route_group_id": group}
    assert _probe(topic, incremental) == []
    # A full refresh reads the whole topic: there the repeats are real.
    full = {"kafka_start_position": "earliest", "kafka_route_group_id": group}
    assert sorted(_probe(topic, full)) == [("3", 2), ("5", 2)]
    # The probe never commits, and never moves the route group.
    assert fake_broker.groups[group] == {TopicPartition(topic, 0): 5}
    # A group with nothing committed yet: the first run reads the whole topic.
    fresh = {"kafka_start_position": "committed", "kafka_route_group_id": "never-committed"}
    assert sorted(_probe(topic, fresh)) == [("3", 2), ("5", 2)]


def test_keyed_incremental_schedule_runs_two_and_three_are_not_refused(broker, pg):
    """The QA schedule shape: key ``id`` (a keyed schedule's duplicate policy is
    upsert), Validate on, strict. Changed keys arrive as new messages."""
    conn, made = pg
    topic, dst = _names("d396")
    made.append(dst)
    for i in range(1, 6):
        broker.produce(topic, json.dumps({"id": i, "name": f"v1-{i}"}))
    mappings = [
        {"source": k, "target": k, "approved": True, "confidence": 0.99}
        for k in sorted(_schema(topic))
    ]

    def _go():
        req = _request(topic, dst, sync_mode="incremental_append", mappings=mappings,
                       validation_mode="strict")
        req.skip_preflight = False
        req.duplicate_policy = "upsert"
        req.source.extra = {**(req.source.extra or {}), "duplicate_policy": "upsert"}
        req.stream_contracts = [{"name": topic, "selected": True,
                                 "sync_mode": "incremental_append", "primary_key": ["id"]}]
        return _run(req)[1]

    first = _go()
    assert first.success, first.error
    assert first.records_transferred == 5
    for i, name in ((5, "v2-5"), (3, "v2-3"), (6, "v1-6")):
        broker.produce(topic, json.dumps({"id": i, "name": name}))
    second = _go()
    assert second.success, second.error
    assert second.records_transferred == 3
    for i, name in ((5, "v3-5"), (3, "v3-3")):
        broker.produce(topic, json.dumps({"id": i, "name": name}))
    third = _go()
    assert third.success, third.error
    assert third.records_transferred == 2

    landed = {(int(k), n) for k, n in _rows(conn, dst)}
    for expected in ((1, "v1-1"), (2, "v1-2"), (3, "v3-3"), (4, "v1-4"), (5, "v3-5"), (6, "v1-6")):
        assert expected in landed, (expected, sorted(landed))
