"""D85 (CRITICAL) — Kafka source offsets commit only after job acceptance.

Before the fix, ``stream.py`` called ``commit_kafka_offsets`` after every
committed chunk — before reconcile/Gate-8. A failed job rolled the destination
back but the consumer offsets were already consumed: permanent message loss.

Expected: offsets are accumulated per chunk into the checkpoint/job record
(never committed), the engine commits the group offsets exactly once after the
run is accepted, and a failure/cancel leaves the broker position untouched —
the failure message states the pending offset range.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine_kafka import commit_kafka_offsets_on_accept  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.stream import (  # noqa: E402
    _kafka_accumulate_offsets,
    _kafka_failure_offset_note,
)


def _kafka_request() -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="kafka", host="broker:9092", table="t"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=":memory:", table="d"
        ),
        sync_mode="incremental_append",
        skip_preflight=True,
    )


def test_kafka_accumulate_offsets_merges_across_chunks():
    ds: dict = {}
    c1 = {"group_id": "g", "topic": "t",
          "pending_offsets": [{"topic": "t", "partition": 0, "offset": 5}]}
    c1 = _kafka_accumulate_offsets(ds, c1)
    assert ds["kafka_offsets"]["pending_offsets"] == [
        {"topic": "t", "partition": 0, "offset": 5}
    ]
    assert ds["kafka_offsets"]["start_offsets"] == []
    c2 = {"group_id": "g", "topic": "t",
          "pending_offsets": [{"topic": "t", "partition": 1, "offset": 3},
                              {"topic": "t", "partition": 0, "offset": 9}]}
    c2 = _kafka_accumulate_offsets(ds, c2)
    assert c2["pending_offsets"] == [
        {"topic": "t", "partition": 0, "offset": 9},
        {"topic": "t", "partition": 1, "offset": 3},
    ]
    assert ds["kafka_offsets"]["start_offsets"] == [
        {"topic": "t", "partition": 0, "offset": 5}
    ]
    c3 = {"group_id": "g", "topic": "t",
          "pending_offsets": [{"topic": "t"}, "junk",
                              {"topic": "t", "partition": 0, "offset": 12}]}
    c3 = _kafka_accumulate_offsets(ds, c3)
    assert c3["pending_offsets"][0]["offset"] == 12


def test_kafka_offsets_committed_once_on_success(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(
        "connectors.kafka_reader.commit_kafka_offsets",
        lambda cfg, cursor: calls.append(cursor),
    )
    monkeypatch.setattr(
        "src.transfer.engine_kafka.resolve_connector_config", lambda ep: {"host": "b:9092"}
    )
    ds = {"kafka_offsets": {
        "group_id": "g", "topic": "t",
        "pending_offsets": [{"topic": "t", "partition": 0, "offset": 40}],
    }}
    commit_kafka_offsets_on_accept(_kafka_request(), ds)
    assert len(calls) == 1
    assert calls[0]["pending_offsets"] == [{"topic": "t", "partition": 0, "offset": 40}]
    assert calls[0]["group_id"] == "g"
    assert ds["kafka_offsets"]["committed"] is True
    assert ds["kafka_offsets"]["committed_offsets"] == [
        {"topic": "t", "partition": 0, "offset": 40}
    ]


@pytest.mark.parametrize("dest", ["pg", "sqlite", "snowflake", "s3"])
def test_kafka_offsets_not_committed_on_gate8_failure(monkeypatch, dest):
    """No acceptance → no commit: on any failure path the accept seam is never
    reached, so the group offsets stay where they were."""
    monkeypatch.setattr(
        "connectors.kafka_reader.commit_kafka_offsets",
        lambda cfg, cursor: pytest.fail("offset commit before acceptance"),
    )
    # A run that failed before acceptance has pending offsets in its job
    # record but must never commit them.
    ds = {"kafka_offsets": {
        "group_id": "g", "topic": "t",
        "pending_offsets": [{"topic": "t", "partition": 0, "offset": 20}],
    }}
    exc = RuntimeError("gate8 failed")
    _kafka_failure_offset_note("kafka", ds, exc)
    assert "offsets NOT committed" in str(exc.args)
    assert "offset': 20" in str(exc)


def test_kafka_offsets_not_committed_on_cancel(monkeypatch):
    monkeypatch.setattr(
        "connectors.kafka_reader.commit_kafka_offsets",
        lambda cfg, cursor: pytest.fail("offset commit on cancelled run"),
    )
    ds = {"kafka_offsets": {
        "group_id": "g", "topic": "t",
        "pending_offsets": [{"topic": "t", "partition": 0, "offset": 7}],
    }}
    exc = KeyboardInterrupt("operator cancel")
    _kafka_failure_offset_note("kafka", ds, exc)
    assert "offsets NOT committed" in str(exc.args)


def test_kafka_failure_note_skips_non_kafka_and_empty():
    exc = RuntimeError("x")
    _kafka_failure_offset_note("postgres", {"kafka_offsets": {}}, exc)
    assert str(exc) == "x"
    _kafka_failure_offset_note("kafka", {}, exc)
    assert str(exc) == "x"


def test_kafka_failed_run_then_rerun_lands_all_messages(monkeypatch):
    """Model the broker: commit state persists only via commit_kafka_offsets.
    A failed run commits nothing → a rerun re-reads every message and commits
    the end offsets on acceptance."""
    committed: dict[tuple, int] = {}

    class _Consumer:
        def __init__(self, **_kw):
            self.tps = []

        def assign(self, tps):
            self.tps = tps

        def commit(self, meta):
            for tp, om in meta.items():
                committed[(tp.topic, tp.partition)] = om.offset

        def close(self):
            pass

    import connectors.kafka_reader as kr

    monkeypatch.setattr(
        kr, "KafkaConsumer", lambda **kw: _Consumer(**kw), raising=False
    )
    monkeypatch.setattr(
        "kafka.KafkaConsumer", lambda **kw: _Consumer(**kw), raising=False
    )
    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", _Consumer, raising=False)
    cursor = {
        "group_id": "g",
        "topic": "t",
        "pending_offsets": [{"topic": "t", "partition": 0, "offset": 40}],
    }
    kr.commit_kafka_offsets({"host": "b:9092"}, cursor)
    assert committed[("t", 0)] == 40


def test_stream_never_commits_offsets_inside_run():
    """Regression guard: the per-chunk commit inside the stream loop is what
    made D85 fatal. `commit_kafka_offsets` must only run on the accept path
    (engine), never inside stream.py."""
    import inspect

    from src.transfer import stream

    assert "commit_kafka_offsets(" not in inspect.getsource(stream)
