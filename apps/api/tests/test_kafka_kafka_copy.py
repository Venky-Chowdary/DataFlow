"""Kafka → Kafka consume-produce of bytes — dest watermark COUNT, never producer ack."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_kafka_common import (  # noqa: E402
    kafka_delete_topic,
    kafka_family_name,
    kafka_topic,
    kafka_type_is_copy_safe,
)
from services.copy_kafka_kafka import (  # noqa: E402
    copy_kafka_to_kafka,
    kafka_kafka_copy_enabled,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _kafka_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9092), timeout=1):
            pass
    except OSError:
        pytest.skip("Kafka 9092 not reachable")


def _kafka_cfg(topic: str) -> dict:
    return {
        "type": "kafka",
        "format": "kafka",
        "host": "127.0.0.1",
        "port": 9092,
        "database": topic,
        "table": topic,
    }


def _admin():
    _kafka_or_skip()
    pytest.importorskip("kafka")
    from services.copy_kafka_common import kafka_admin

    admin = kafka_admin(_kafka_cfg("probe"))
    try:
        admin.list_topics()
    except Exception as exc:
        pytest.skip(f"Kafka unavailable: {exc}")
    return admin


def _dest_count(topic: str) -> int:
    n = destination_row_count(
        "kafka", _kafka_cfg(topic), schema="", table_name=topic
    )
    assert n is not None
    return int(n)


def _seed(topic: str, rows: int) -> None:
    from kafka import KafkaProducer
    from services.copy_kafka_common import kafka_byte_producer, kafka_create_topic

    kafka_delete_topic(_kafka_cfg(topic), topic)
    kafka_create_topic(_kafka_cfg(topic), topic, num_partitions=1)
    producer = kafka_byte_producer(_kafka_cfg(topic))
    try:
        for i in range(1, rows + 1):
            if i == 2:
                value: bytes | None = b""
            else:
                value = json.dumps({"id": i, "label": f"r{i}"}).encode("utf-8")
            producer.send(
                topic,
                key=str(i).encode("utf-8"),
                value=value,
            )
        producer.flush(timeout=30)
    finally:
        producer.close()
    del KafkaProducer


def _drop(topic: str) -> None:
    kafka_delete_topic(_kafka_cfg(topic), topic)


def _pairs():
    return [("id", "id"), ("label", "label")]


def test_kafka_family_and_copy_safe_types():
    assert kafka_family_name("apache_kafka") == "kafka"
    assert kafka_family_name("redpanda") == "kafka"
    assert kafka_family_name("amazon_msk") == "kafka"
    assert kafka_type_is_copy_safe("json") is True
    assert kafka_type_is_copy_safe("bytes") is True
    assert kafka_type_is_copy_safe("keyword") is True
    assert kafka_type_is_copy_safe("avro") is False


def test_kafka_kafka_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_KAFKA_KAFKA_COPY", "0")
    assert kafka_kafka_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_kafka_to_kafka(
            source_cfg=_kafka_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_kafka_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            kafka_ddls=["long"],
            replace_destination=True,
        )


def test_kafka_kafka_same_topic_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    cfg = _kafka_cfg("same_topic")
    with pytest.raises(FastPathUnavailable, match="same topic"):
        copy_kafka_to_kafka(
            source_cfg=cfg,
            source_table="same_topic",
            dest_cfg=cfg,
            dest_table="same_topic",
            pairs=[("id", "id")],
            kafka_ddls=["long"],
            replace_destination=True,
        )


def test_kafka_kafka_public_proxy_declines():
    dest = {
        **_kafka_cfg("b"),
        "host": "",
        "connection_string": "caboose.proxy.rlwy.net:9092",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_kafka_to_kafka(
            source_cfg=_kafka_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            kafka_ddls=["long"],
            replace_destination=True,
        )


def test_kafka_kafka_cross_endpoint_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    dest = {**_kafka_cfg("b"), "port": 9093}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_kafka_to_kafka(
            source_cfg=_kafka_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            kafka_ddls=["long"],
            replace_destination=True,
        )


def test_kafka_kafka_column_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_kafka_to_kafka(
            source_cfg=_kafka_cfg("a"),
            source_table="a",
            dest_cfg=_kafka_cfg("b"),
            dest_table="b",
            pairs=[("id", "user_id")],
            kafka_ddls=["long"],
            replace_destination=True,
        )


def test_kafka_topic_rejects_glob_and_internal_names():
    with pytest.raises(FastPathUnavailable, match="glob"):
        kafka_topic("a,b")
    with pytest.raises(FastPathUnavailable, match="internal"):
        kafka_topic("__consumer_offsets")


def test_live_kafka_kafka_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        result = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("kafka_read") == "byte_records"
        assert result.source_snapshot.get("kafka_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_copy_is_not_json_decode(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    _seed(src, 80)
    _drop(dest)

    def _no_json(*_a, **_k):
        raise AssertionError("Kafka→Kafka COPY must not json.loads")

    def _no_payload(*_a, **_k):
        raise AssertionError("Kafka→Kafka COPY must not kafka_json_payload")

    import connectors.kafka_writer as kafka_writer

    monkeypatch.setattr(json, "loads", _no_json)
    monkeypatch.setattr(kafka_writer, "kafka_json_payload", _no_payload)
    try:
        result = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("kafka_read") == "byte_records"
        assert _dest_count(dest) == 80
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_empty_bytes_and_json_null_preserved():
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        from kafka import KafkaConsumer, TopicPartition
        from services.copy_kafka_common import kafka_byte_producer, kafka_create_topic

        _drop(src)
        _drop(dest)
        kafka_create_topic(_kafka_cfg(src), src, num_partitions=1)
        producer = kafka_byte_producer(_kafka_cfg(src))
        try:
            producer.send(src, key=b"1", value=None)
            producer.send(src, key=b"2", value=b"")
            producer.send(src, key=b"3", value=b"null")
            producer.send(src, key=b"4", value=b'{"id":4,"label":"x"}')
            producer.flush(timeout=30)
        finally:
            producer.close()
        result = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 4
        assert _dest_count(dest) == 4
        consumer = KafkaConsumer(
            bootstrap_servers="127.0.0.1:9092",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=4000,
        )
        try:
            tp = TopicPartition(dest, 0)
            consumer.assign([tp])
            consumer.seek(tp, 0)
            got = {}
            deadline_idle = 0
            while len(got) < 4 and deadline_idle < 20:
                batch = consumer.poll(timeout_ms=500, max_records=20)
                if not batch:
                    deadline_idle += 1
                    continue
                deadline_idle = 0
                for records in batch.values():
                    for rec in records:
                        got[rec.key] = rec.value
            assert got[b"1"] is None
            assert got[b"2"] == b""
            assert got[b"3"] == b"null"
            assert got[b"4"] == b'{"id":4,"label":"x"}'
        finally:
            consumer.close()
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_skip_when_dest_count_matches():
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        first = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_occupied_mismatch_declines():
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        _seed(src, 800)
        _seed(dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Kafka dest"):
            copy_kafka_to_kafka(
                source_cfg=_kafka_cfg(src),
                source_table=src,
                dest_cfg=_kafka_cfg(dest),
                dest_table=dest,
                pairs=_pairs(),
                kafka_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_overwrite_replaces_dest():
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        _seed(src, 800)
        _seed(dest, 1)
        result = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("kafka_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)
        admin.close()


def test_live_kafka_kafka_dest_count_is_not_producer_ack():
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    other = f"dfkoth{tag}"
    try:
        _seed(src, 80)
        _seed(other, 50)
        _drop(dest)
        other_count = _dest_count(other)
        assert other_count == 50
        result = copy_kafka_to_kafka(
            source_cfg=_kafka_cfg(src),
            source_table=src,
            dest_cfg=_kafka_cfg(dest),
            dest_table=dest,
            pairs=_pairs(),
            kafka_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
        assert _dest_count(dest) != other_count + 80
        assert _dest_count(other) == 50
    finally:
        _drop(src)
        _drop(dest)
        _drop(other)
        admin.close()


def test_live_kafka_kafka_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_KAFKA_KAFKA_COPY", raising=False)
    admin = _admin()
    tag = uuid.uuid4().hex[:8]
    src = f"dfksrc{tag}"
    dest = f"dfkdst{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"kafka-kafka-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_kafka_cfg(src), "format": "kafka"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_kafka_cfg(dest), "format": "kafka"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "consume_produce_bytes_kafka_kafka"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Kafka" in line or "consume" in line.lower() for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)
        admin.close()
