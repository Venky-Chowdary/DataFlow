"""QA round 4, Kafka family acceptance on the real broker + real PostgreSQL.

QA's acceptance line: "JSON-valued topic -> new PG, overwrite and incremental
upsert, with types inferred (int, numeric, ts, text) and a cell-by-cell match".
Also QA's notes: D161's float block still reproduced on Kafka -> new PG on a
``ratio`` column even with a signed contract; D151 "every column is read as
VARCHAR, and Execute fails with 0 rows, or 'no columns or is empty'".

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, broker on
127.0.0.1:9092, PostgreSQL from ``PG_*`` (default 127.0.0.1:5432
dataflow/dataflow). Topic shapes: a fresh single-partition topic, a
three-partition topic, and a topic whose connector consumer group already has
offsets committed at the log end.
"""
from __future__ import annotations

import datetime as dt
import json
import socket
import time
import uuid

import pytest

from test_d190_d194_d197_declared_string_types_e2e import (  # noqa: F401
    PG,
    _drop,
    _pg,
    _pg_column_types,
    _pg_connector,
    _wait_job,
    transfer_client,
)

_BROKER = {"host": "127.0.0.1", "port": 9092}
_UTC = dt.timezone.utc

_RUN1 = [
    {"id": 1, "amount": 19.99, "ratio": 0.30000000000000004, "ts": "2024-03-01T10:00:00Z", "name": "a"},
    {"id": 2, "amount": 100, "ratio": 1.23456789012345, "ts": "2024-03-01T10:00:01.5Z", "name": ""},
    {"id": 3, "amount": -0.5, "ratio": 1e-07, "ts": "2024-03-02T00:00:00+02:00", "name": "é中\U0001f600"},
    {"id": 4, "amount": None, "ratio": None, "ts": None, "name": None},
    {"id": 5, "amount": 12345678.25, "ratio": 2.5, "ts": "2023-12-31T23:59:59Z", "name": "  pad  "},
]
# Run 2 of the upsert: id 2 changes, id 6 is new.
_RUN2 = [
    {"id": 2, "amount": 101.5, "ratio": 0.1, "ts": "2024-03-05T08:00:00Z", "name": "b2"},
    {"id": 6, "amount": 7, "ratio": 0.7, "ts": "2024-03-05T09:00:00Z", "name": "new"},
]


def _ts(text):
    if text is None:
        return None
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(_UTC)


def _expected(messages) -> dict[int, tuple]:
    out: dict[int, tuple] = {}
    for m in messages:
        out[m["id"]] = (m["amount"], m["ratio"], _ts(m["ts"]), m["name"])
    return out


def _admin():
    from services.copy_kafka_common import kafka_admin

    return kafka_admin(_BROKER)


@pytest.fixture
def make_topic():
    try:
        socket.create_connection((_BROKER["host"], _BROKER["port"]), timeout=1).close()
    except OSError:
        pytest.skip("Kafka 127.0.0.1:9092 not reachable")
    pytest.importorskip("kafka")
    from kafka.admin import NewTopic

    made: list[str] = []

    def _make(partitions: int = 1) -> str:
        name = f"r4k_{uuid.uuid4().hex[:10]}"
        admin = _admin()
        try:
            admin.create_topics([NewTopic(name, partitions, 1)])
        finally:
            admin.close()
        made.append(name)
        return name

    yield _make
    admin = _admin()
    try:
        admin.delete_topics(made)
    except Exception:
        pass
    finally:
        admin.close()


def _produce(topic: str, messages, partitions: int = 1) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}")
    try:
        for i, value in enumerate(messages):
            producer.send(
                topic,
                key=str(value["id"]).encode(),
                value=json.dumps(value).encode(),
                partition=(value["id"] % partitions) if partitions > 1 else None,
            )
        producer.flush(10)
    finally:
        producer.close()


def _commit_to_end(topic: str, group_id: str) -> None:
    """Give the connector's consumer group committed offsets at the log end."""
    from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=f"{_BROKER['host']}:{_BROKER['port']}",
        group_id=group_id,
        enable_auto_commit=False,
    )
    try:
        parts = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or [])]
        consumer.assign(parts)
        ends = consumer.end_offsets(parts)
        consumer.commit({tp: OffsetAndMetadata(ends[tp], None, -1) for tp in parts})
    finally:
        consumer.close()


def _kafka_connector(group_id: str = "") -> str:
    from services.connector_store import create_connector

    spec = {
        "name": "r4k-" + uuid.uuid4().hex[:6], "type": "kafka", "role": "source",
        "host": _BROKER["host"], "port": _BROKER["port"], "database": "",
        "ssl": False, "workspace_id": "",
    }
    if group_id:
        spec["extra"] = {"group_id": group_id}
    return str(create_connector(spec).id)


def _run(client, **kwargs) -> dict:
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    assert staged.success, {
        "error": staged.error,
        "blockers": ((staged.output or {}).get("preflight") or {}).get("blockers"),
        "mappings": [
            {k: m.get(k) for k in ("source", "source_type", "target_type", "fidelity", "confidence")}
            for m in (staged.output or {}).get("engine_mappings") or []
        ],
    }
    confirmed = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged.output["ack_id"], "reason": "r4k"}
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"), "error": job.get("error") or job.get("message"),
    }
    return job


def _rows(table: str) -> dict[int, tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, amount, ratio, ts, name FROM public."{table}"')
            out = {}
            for r in cur.fetchall():
                amount = None if r[1] is None else float(r[1])
                ts = None if r[3] is None else r[3].astimezone(_UTC)
                assert int(r[0]) not in out, ("duplicate id", r)
                out[int(r[0])] = (amount, r[2], ts, r[4])
            return out
    finally:
        conn.close()


def _assert_types(table: str) -> None:
    types = _pg_column_types(table)
    assert types["id"] in {"integer", "bigint", "smallint"}, types
    assert types["amount"] in {"numeric", "double precision"}, types
    assert types["ratio"] == "double precision", types
    assert types["ts"] == "timestamp with time zone", types
    assert types["name"] == "text" or types["name"] == "character varying", types


_SIGNED = {
    "approved_by": "qa@example.com",
    "reason": "Kafka JSON doubles land as double precision",
    "execution_policy": "CAST_AND_CONTINUE",
}

_SHAPES = [
    pytest.param(1, False, id="fresh_topic"),
    pytest.param(3, False, id="three_partitions"),
    pytest.param(1, True, id="group_committed_at_end"),
    pytest.param(3, True, id="three_partitions_group_committed"),
]


@pytest.mark.parametrize("partitions,committed", _SHAPES)
@pytest.mark.parametrize("signed", [False, True], ids=["strict", "signed"])
def test_json_topic_overwrite_into_new_pg_cell_by_cell(
    transfer_client, make_topic, partitions, committed, signed
):
    _pg().close()
    topic = make_topic(partitions)
    _produce(topic, _RUN1, partitions)
    group = f"r4g_{uuid.uuid4().hex[:8]}" if committed else ""
    if committed:
        _commit_to_end(topic, group)
    table = topic + "_ow"
    _drop(table)
    args = dict(
        source_connector_id=_kafka_connector(group),
        source_table=topic,
        dest_connector_id=_pg_connector("r4k"),
        dest_table=table,
        sync_mode="full_refresh_overwrite",
    )
    if signed:
        args["risk_acceptance"] = dict(_SIGNED)
    try:
        for _n in (1, 2):
            _run(transfer_client, **args)
            _assert_types(table)
            assert _rows(table) == _expected(_RUN1)
    finally:
        _drop(table)


@pytest.mark.parametrize("cursor", ["", "_kafka_offset"], ids=["no_cursor", "offset_cursor"])
@pytest.mark.parametrize("partitions,committed", _SHAPES)
def test_json_topic_incremental_upsert_into_new_pg_cell_by_cell(
    transfer_client, make_topic, partitions, committed, cursor
):
    _pg().close()
    topic = make_topic(partitions)
    _produce(topic, _RUN1, partitions)
    group = f"r4g_{uuid.uuid4().hex[:8]}" if committed else ""
    if committed:
        # Offsets already committed at the end: the first incremental run
        # under this group has nothing new to read. It must not fail.
        _commit_to_end(topic, group)
    table = topic + "_up"
    _drop(table)
    args = dict(
        source_connector_id=_kafka_connector(group),
        source_table=topic,
        dest_connector_id=_pg_connector("r4ku"),
        dest_table=table,
        sync_mode="incremental_upsert",
        upsert_key="id",
    )
    if cursor:
        args["cursor_column"] = cursor
        args["cursor_semantics"] = "monotonic_sequence"
    try:
        if committed:
            _run(transfer_client, **args)
            _produce(topic, _RUN1, partitions)  # re-send run 1 past the commit
        _run(transfer_client, **args)
        _assert_types(table)
        assert _rows(table) == _expected(_RUN1)

        _produce(topic, _RUN2, partitions)
        _run(transfer_client, **args)
        _assert_types(table)
        assert _rows(table) == _expected(_RUN1 + _RUN2)

        # Nothing new: a third run is a clean no-op.
        _run(transfer_client, **args)
        assert _rows(table) == _expected(_RUN1 + _RUN2)
    finally:
        _drop(table)
