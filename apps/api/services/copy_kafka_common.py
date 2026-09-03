"""Shared Kafka identity-COPY helpers.

Dest COUNT is ``destination_row_count`` / log-end minus log-start
watermarks — never producer ack, never poll count, never leftover
compaction MERGE. Same bootstrap+topic declines. Cross-endpoint
consume+produce declines (identity COPY stays on one cluster; MirrorMaker 2
/ Cluster Linking is not this path). Occupancy is counted **before**
delete. Desktop-lab Kafka / Redpanda on :9092 is not a customer-tenant
PRODUCTION_SKU.
"""

from __future__ import annotations

import logging
import re
import ssl
import time
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable

logger = logging.getLogger(__name__)

_KAFKA_FAMILY = frozenset({
    "kafka",
    "apache_kafka",
    "confluent_kafka",
    "confluent",
    "redpanda",
    "msk",
    "amazon_msk",
})

# Mapping stamps are opaque — identity COPY does not decode JSON / Avro.
_KAFKA_COPY_SAFE_TYPES = frozenset({
    "json",
    "string",
    "text",
    "keyword",
    "bytes",
    "binary",
    "long",
    "integer",
    "int",
    "boolean",
    "bool",
    "number",
    "double",
    "float",
})

_TOPIC_OK = re.compile(r"^[a-zA-Z0-9._-]+$")
_TOPIC_MAX = 249


def kafka_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in _KAFKA_FAMILY:
        return "kafka"
    return n


def kafka_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return True
    base = raw.split("<", 1)[0].split("(", 1)[0].strip()
    return base in _KAFKA_COPY_SAFE_TYPES


def kafka_topic(table: str) -> str:
    name = (table or "").strip()
    if not name:
        raise FastPathUnavailable("Kafka topic required")
    if any(ch in name for ch in "*,?[]\\/ "):
        raise FastPathUnavailable("Kafka COPY refuses glob characters in the topic")
    if not _TOPIC_OK.match(name) or len(name) > _TOPIC_MAX:
        raise FastPathUnavailable("Kafka COPY refuses this topic name")
    if name.startswith("__") or name in {".", ".."}:
        raise FastPathUnavailable("Kafka COPY refuses internal topic names")
    return name


def kafka_endpoint_key(cfg: dict[str, Any]) -> str:
    from connectors.kafka_reader import _bootstrap

    raw = str(_bootstrap(cfg) or "").strip().lower()
    raw = raw.replace("localhost", "127.0.0.1")
    return raw or "127.0.0.1:9092"


def kafka_object_id(cfg: dict[str, Any], topic: str) -> tuple[str, str]:
    return (kafka_endpoint_key(cfg), kafka_topic(topic))


def kafka_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def kafka_client_kwargs(cfg: dict[str, Any]) -> dict[str, Any]:
    """Shared kafka-python kwargs. No JSON serializers — identity is bytes."""
    from connectors.kafka_reader import _bootstrap

    bootstrap = _bootstrap(cfg)
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap,
        "request_timeout_ms": 30000,
    }
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    security = str(
        cfg.get("security_protocol") or extra.get("security_protocol") or cfg.get("schema") or ""
    ).upper()
    username = str(cfg.get("username") or extra.get("username") or "")
    password = str(cfg.get("password") or cfg.get("api_key") or extra.get("password") or "")
    mechanism = str(
        extra.get("sasl_mechanism") or cfg.get("sasl_mechanism") or ""
    ).strip()
    if username and password:
        kwargs["security_protocol"] = (
            security if security in {"SASL_SSL", "SASL_PLAINTEXT"} else "SASL_SSL"
        )
        kwargs["sasl_mechanism"] = mechanism or "PLAIN"
        kwargs["sasl_plain_username"] = username
        kwargs["sasl_plain_password"] = password
        if kwargs["security_protocol"] == "SASL_SSL":
            kwargs["ssl_context"] = ssl.create_default_context()
    return kwargs


def kafka_dest_count(cfg: dict[str, Any], topic: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "kafka",
        {**cfg, "type": "kafka", "format": "kafka"},
        schema="",
        table_name=kafka_topic(topic),
    )
    if n is None:
        raise ValueError(f"Kafka dest COUNT unmeasured for {topic}")
    return int(n)


def kafka_admin(cfg: dict[str, Any]):
    from kafka import KafkaAdminClient

    return KafkaAdminClient(**kafka_client_kwargs(cfg))


def kafka_byte_consumer(cfg: dict[str, Any]):
    from kafka import KafkaConsumer

    kwargs = kafka_client_kwargs(cfg)
    kwargs.update(
        {
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "consumer_timeout_ms": 2000,
            "key_deserializer": None,
            "value_deserializer": None,
        }
    )
    return KafkaConsumer(**kwargs)


def kafka_byte_producer(cfg: dict[str, Any]):
    from kafka import KafkaProducer

    kwargs = kafka_client_kwargs(cfg)
    kwargs.update(
        {
            "acks": "all",
            "retries": 5,
            "key_serializer": None,
            "value_serializer": None,
            "linger_ms": 5,
        }
    )
    return KafkaProducer(**kwargs)


def kafka_dest_can_write(dest_cfg: dict[str, Any]) -> None:
    """Dest credentials must reach the cluster before dest mutation.

    Produce / create / delete use dest credentials so dest write ACL is
    the boundary. This probe lists topics — it does not produce a record.
    """
    admin = kafka_admin(dest_cfg)
    try:
        admin.list_topics()
    except Exception as exc:
        raise FastPathUnavailable(
            "dest credentials cannot administer Kafka cluster for identity COPY"
        ) from exc
    finally:
        _close_quiet(admin)


def kafka_topic_exists(cfg: dict[str, Any], topic: str) -> bool:
    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        return bool(parts)
    finally:
        _close_quiet(consumer)


def kafka_partition_count(cfg: dict[str, Any], topic: str) -> int:
    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        return len(parts) if parts else 0
    finally:
        _close_quiet(consumer)


def _wait_partitions(cfg: dict[str, Any], topic: str, expected: int) -> None:
    deadline = time.monotonic() + 15
    last = 0
    while time.monotonic() < deadline:
        last = kafka_partition_count(cfg, topic)
        if last == int(expected):
            return
        time.sleep(0.2)
    raise ValueError(
        f"Kafka topic {topic} metadata not ready (partitions={last}, expected={expected})"
    )


def kafka_snapshot_watermarks(
    cfg: dict[str, Any], topic: str
) -> tuple[int, list[Any], dict[Any, int], dict[Any, int]]:
    """beginning_offsets + end_offsets before consume. Does not decode records."""
    from kafka import TopicPartition

    name = kafka_topic(topic)
    consumer = kafka_byte_consumer(cfg)
    try:
        parts = consumer.partitions_for_topic(name)
        if not parts:
            raise FastPathUnavailable("Kafka source topic missing")
        tps = [TopicPartition(name, int(p)) for p in sorted(parts)]
        begin = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        source_count = int(sum(int(end[tp]) - int(begin[tp]) for tp in tps))
        begin_i = {tp: int(begin[tp]) for tp in tps}
        end_i = {tp: int(end[tp]) for tp in tps}
        return source_count, tps, begin_i, end_i
    finally:
        _close_quiet(consumer)


def kafka_create_topic(cfg: dict[str, Any], topic: str, *, num_partitions: int) -> None:
    from kafka.admin import NewTopic

    name = kafka_topic(topic)
    parts = max(1, int(num_partitions))
    admin = kafka_admin(cfg)
    try:
        deadline = time.monotonic() + 30
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            existing = set(admin.list_topics() or [])
            if name in existing:
                admin.wait_for_topics([name], timeout_ms=15000)
                _wait_partitions(cfg, name, parts)
                return
            try:
                admin.create_topics(
                    [NewTopic(name=name, num_partitions=parts, replication_factor=1)],
                    timeout_ms=15000,
                    validate_only=False,
                    raise_errors=True,
                    wait_for_metadata=True,
                )
                admin.wait_for_topics([name], timeout_ms=15000)
                _wait_partitions(cfg, name, parts)
                return
            except Exception as exc:
                last_exc = exc
                text = str(exc).lower()
                if "marked for deletion" in text or "already exists" in text:
                    time.sleep(0.25)
                    continue
                raise
        raise ValueError(f"Kafka create_topics failed for {name}: {last_exc}")
    finally:
        _close_quiet(admin)


def kafka_delete_topic(cfg: dict[str, Any], topic: str) -> None:
    name = kafka_topic(topic)
    admin = kafka_admin(cfg)
    try:
        existing = set(admin.list_topics() or [])
        if name not in existing:
            return
        admin.delete_topics([name], timeout_ms=15000, raise_errors=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if name not in set(admin.list_topics() or []):
                return
            time.sleep(0.2)
        raise ValueError(f"Kafka delete_topics did not drop {name}")
    finally:
        _close_quiet(admin)


def kafka_copy_bytes(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    src_topic: str,
    dest_topic: str,
    tps: list[Any],
    begin: dict[Any, int],
    end: dict[Any, int],
    pin_partition: bool,
) -> None:
    """Consume raw bytes from source; produce identical bytes with dest credentials.

    No consumer group (must not move the transfer cursor). No deserializer.
    Consume only up to the snapshot end watermark.
    """
    src = kafka_topic(src_topic)
    dest = kafka_topic(dest_topic)
    del src
    consumer = kafka_byte_consumer(source_cfg)
    producer = kafka_byte_producer(dest_cfg)
    try:
        consumer.assign(tps)
        for tp in tps:
            consumer.seek(tp, begin[tp])
        idle = 0
        while True:
            caught_up = True
            for tp in tps:
                if consumer.position(tp) < end[tp]:
                    caught_up = False
                    break
            if caught_up:
                break
            batch = consumer.poll(timeout_ms=1000, max_records=500)
            if not batch:
                idle += 1
                if idle > 30:
                    raise ValueError(
                        "Kafka consume stalled before snapshot end watermark"
                    )
                continue
            idle = 0
            for tp, records in batch.items():
                snap_end = end[tp]
                for rec in records:
                    if rec.offset >= snap_end:
                        continue
                    send_kw: dict[str, Any] = {
                        "topic": dest,
                        "key": rec.key,
                        "value": rec.value,
                        "headers": list(rec.headers) if rec.headers else None,
                        "timestamp_ms": rec.timestamp,
                    }
                    if pin_partition:
                        send_kw["partition"] = rec.partition
                    producer.send(**send_kw)
        producer.flush(timeout=60)
    finally:
        _close_quiet(producer)
        _close_quiet(consumer)


def skip_complete_kafka(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "topic",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _close_quiet(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return
