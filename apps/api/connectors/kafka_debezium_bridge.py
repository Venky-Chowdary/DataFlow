"""Optional Kafka / Debezium envelope → Datawrap change-stream adapter.

Enterprises that already run Kafka Connect + Debezium can point Datawrap at a
topic and consume CDC envelopes without embedding Kafka in the default deploy.
This module is a thin bridge — not required for native PG/MySQL/Mongo CDC.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Iterator

from services.value_serializer import json_loads_exact

logger = logging.getLogger(__name__)


@dataclass
class DebeziumChange:
    op: str  # c | u | d | r
    table: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source_ts_ms: int = 0
    ls: str = ""  # source offset / LSN / binlog pos as string
    raw: dict[str, Any] = field(default_factory=dict)


def parse_debezium_envelope(payload: dict[str, Any] | str) -> DebeziumChange | None:
    """Parse a Debezium JSON envelope (payload or full message)."""
    if isinstance(payload, str):
        try:
            payload = json_loads_exact(payload, keep_literals=True)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    # Unwrap CloudEvents / Connect envelope if present
    body = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    if not isinstance(body, dict):
        return None
    op = str(body.get("op") or "")
    if not op:
        return None
    source = body.get("source") or {}
    table = str(source.get("table") or body.get("table") or "")
    ts = int(source.get("ts_ms") or body.get("ts_ms") or 0)
    ls = str(
        source.get("lsn")
        or source.get("pos")
        or source.get("sequence")
        or source.get("txId")
        or ""
    )
    before = body.get("before") if isinstance(body.get("before"), dict) else None
    after = body.get("after") if isinstance(body.get("after"), dict) else None
    return DebeziumChange(
        op=op,
        table=table,
        before=before,
        after=after,
        source_ts_ms=ts,
        ls=ls,
        raw=body,
    )


def kafka_value_to_row(
    val: Any,
    *,
    registry_url: str = "",
    meta: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """One Kafka value as a row. A tombstone (empty value) is ``None``.

    Debezium envelopes and plain JSON objects share this decoder. The
    transfer consumer and the sample reader must not grow a second parser.
    """
    if not val:
        return None
    from connectors.confluent_schema_registry import (
        SchemaRegistryError,
        decode_kafka_value,
    )

    try:
        parsed = decode_kafka_value(val, registry_url=registry_url)
    except SchemaRegistryError:
        raise
    except Exception:
        parsed = val
    change = parse_debezium_envelope(parsed if isinstance(parsed, (dict, str)) else None)
    if change is not None:
        return _kafka_row(debezium_to_row(change), meta=meta)
    if isinstance(parsed, dict):
        return _kafka_row(parsed, meta=meta)
    if isinstance(parsed, str):
        try:
            obj = json_loads_exact(parsed, keep_literals=True)
        except json.JSONDecodeError:
            return _kafka_row({"_kafka_value": parsed}, meta=meta, poison="non-JSON Kafka message")
        if isinstance(obj, dict):
            return _kafka_row(obj, meta=meta)
        return _kafka_row({"_kafka_value": str(obj)}, meta=meta, poison="non-object JSON Kafka message")
    return _kafka_row({"_kafka_value": str(parsed)}, meta=meta, poison="non-JSON Kafka message")


#: Broker provenance every consumed row carries, with its declared type. The
#: set and the types are fixed, not sampled: the plan and the run must show
#: the same columns whether or not the sampled messages had a key, and a key
#: is text even when it looks like a number (D151).
KAFKA_ENVELOPE_TYPES = {
    "_kafka_topic": "VARCHAR",
    "_kafka_partition": "INTEGER",
    "_kafka_offset": "BIGINT",
    "_kafka_key": "VARCHAR",
}


def kafka_message_meta(msg: Any) -> dict[str, Any]:
    """Broker coordinates of one consumed message, for :func:`kafka_value_to_row`."""
    key = msg.key
    return {
        "topic": msg.topic,
        "partition": int(msg.partition),
        "offset": int(msg.offset),
        "key": key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key,
    }


def _kafka_row(
    row: dict[str, Any],
    *,
    meta: dict[str, Any] | None,
    poison: str | None = None,
) -> dict[str, Any]:
    """Attach broker provenance (topic/partition/offset/key) so a quarantined
    or landed row can be traced back to the exact broker record. ``poison``
    marks a value that could not be decoded into columns — the reader routes
    it to the rejected-rows channel instead of writing an all-NULL row.
    """
    meta = meta or {}
    for _k, _col in (
        ("topic", "_kafka_topic"),
        ("partition", "_kafka_partition"),
        ("offset", "_kafka_offset"),
        ("key", "_kafka_key"),
    ):
        # A message without a key still has the column (NULL): the row shape
        # must not depend on whether the producer set one.
        if _k in meta:
            row[_col] = meta[_k]
    if poison:
        row["_kafka_poison"] = poison
    return row


def debezium_to_row(change: DebeziumChange) -> dict[str, Any] | None:
    """Map envelope to a destination row (delete → tombstone with __deleted)."""
    if change.op in ("c", "r", "u"):
        row = dict(change.after or {})
        row["__op"] = change.op
        row["__source_ts_ms"] = change.source_ts_ms
        row["__source_ls"] = change.ls
        return row
    if change.op == "d":
        row = dict(change.before or {})
        row["__op"] = "d"
        row["__deleted"] = True
        row["__source_ts_ms"] = change.source_ts_ms
        row["__source_ls"] = change.ls
        return row
    return None


def _seek_on_assign(
    consumer: Any,
    start_offsets: list[dict[str, Any]],
    *,
    from_beginning: bool = False,
) -> Any:
    """Rebalance listener that resumes assigned partitions at ``start_offsets``.

    A run never commits group offsets until it is accepted (D85), so the group
    position stays where the run began. A continuation batch must start after
    the offsets this run already read, or it re-reads them. Any other assigned
    partition starts at the log start when ``from_beginning`` (a full refresh
    re-reads the topic whatever the group has committed), else at the group's
    committed position.
    """
    from kafka import ConsumerRebalanceListener, TopicPartition  # type: ignore

    positions = {
        TopicPartition(str(item["topic"]), int(item["partition"])): int(item["offset"])
        for item in start_offsets
        if isinstance(item, dict)
        and item.get("topic") is not None
        and item.get("partition") is not None
        and item.get("offset") is not None
    }

    class _Listener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, revoked: Any) -> None:
            return None

        def on_partitions_assigned(self, assigned: Any) -> None:
            for tp in assigned:
                if tp in positions:
                    consumer.seek(tp, positions[tp])
                elif from_beginning:
                    consumer.seek_to_beginning(tp)

    return _Listener()


class KafkaDebeziumConsumer:
    """Optional kafka-python consumer. Requires ``kafka-python`` extra.

    Config: ``bootstrap_servers``, ``topic``, ``group_id``.

    Offsets are **not** auto-committed. Call ``commit_offsets`` only after the
    batch has been successfully applied (and preferably checkpointed) so a crash
    mid-write redelivers — at-least-once, never silent loss (Airbyte trap).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._consumer = None
        self._pending_offsets: dict[tuple[str, int], int] = {}
        self.tombstones_seen: int = 0
        # The run's frozen log end per partition, and where each began. A
        # continuation batch carries the snapshot in its config.
        self._end: dict[tuple[str, int], int] | None = {
            (str(i["topic"]), int(i["partition"])): int(i["offset"])
            for i in config.get("end_offsets") or []
        } or None
        self._start: dict[tuple[str, int], int] = {
            (str(i["topic"]), int(i["partition"])): int(i.get("start") or 0)
            for i in config.get("end_offsets") or []
        }
        # Where an earlier batch of this run left off beats where the run began.
        for i in config.get("start_offsets") or []:
            if isinstance(i, dict) and i.get("offset") is not None:
                self._start[(str(i["topic"]), int(i["partition"]))] = int(i["offset"])

    def connect(self) -> None:
        try:
            from kafka import KafkaConsumer  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "kafka-python is required for the Debezium bridge. "
                "Install with: pip install kafka-python"
            ) from exc
        from connectors.kafka_reader import _consumer_configs
        from services.copy_kafka_common import kafka_classified_bootstrap

        with kafka_classified_bootstrap(self.config):
            self._consumer = KafkaConsumer(
                self.config["topic"],
                **_consumer_configs(
                    self.config,
                    group_id=self.config.get("group_id") or "dataflow-debezium",
                    auto_offset_reset=self.config.get("auto_offset_reset") or "earliest",
                    enable_auto_commit=False,
                    # Keep raw bytes so Confluent Schema Registry wire (magic+id+payload) survives.
                    value_deserializer=lambda b: b if b is not None else b,
                ),
            )
        start = self.config.get("start_offsets") or []
        from_beginning = self.config.get("start_position") == "earliest"
        if start or from_beginning:
            # Same subscription, plus the listener that positions this read.
            self._consumer.subscribe(
                [self.config["topic"]],
                listener=_seek_on_assign(
                    self._consumer, start, from_beginning=from_beginning
                ),
            )

    def pending_offsets(self) -> list[dict[str, Any]]:
        """Return next-offset tokens for the current uncommitted poll batch."""
        return [
            {"topic": topic, "partition": partition, "offset": offset}
            for (topic, partition), offset in sorted(self._pending_offsets.items())
        ]

    def commit_offsets(self, offsets: list[dict[str, Any]] | None = None) -> None:
        """Commit offsets after destination apply + checkpoint (at-least-once)."""
        if self._consumer is None:
            self.connect()
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not connected")
        try:
            from kafka import OffsetAndMetadata, TopicPartition  # type: ignore
        except ImportError:
            return
        payload = offsets if offsets is not None else self.pending_offsets()
        if not payload:
            return
        meta = {
            TopicPartition(str(item["topic"]), int(item["partition"])): OffsetAndMetadata(
                int(item["offset"]), None, -1
            )
            for item in payload
            if item.get("topic") is not None and item.get("partition") is not None
        }
        if not meta:
            return
        self._consumer.commit(meta)
        if offsets is None:
            self._pending_offsets.clear()

    def has_assignment(self) -> bool:
        return bool(self._consumer is not None and self._consumer.assignment())

    def _snapshot_end_offsets(self) -> None:
        """Freeze the log end of every assigned partition for this run.

        A run reads the topic as it stood when it began: later arrivals belong
        to the next run. That makes the population (and so the final page)
        well-defined, which a staged overwrite needs to know when to publish.
        """
        if self._end is not None or self._consumer is None:
            return
        assigned = list(self._consumer.assignment() or [])
        if not assigned:
            return
        begin = self._consumer.beginning_offsets(assigned) or {}
        end = self._consumer.end_offsets(assigned) or {}
        committed = {}
        if self.config.get("start_position") != "earliest":
            committed = {tp: self._consumer.committed(tp) for tp in assigned}
        cursor = {
            (str(i["topic"]), int(i["partition"])): int(i["offset"])
            for i in self.config.get("start_offsets") or []
            if isinstance(i, dict) and i.get("offset") is not None
        }
        self._end = {}
        for tp in assigned:
            key = (tp.topic, int(tp.partition))
            self._end[key] = int(end.get(tp) or 0)
            self._start[key] = cursor.get(key) or int(
                committed.get(tp) or begin.get(tp) or 0
            )

    def end_offsets_snapshot(self) -> list[dict[str, Any]]:
        return [
            {"topic": t, "partition": p, "offset": off, "start": self._start.get((t, p), 0)}
            for (t, p), off in sorted((self._end or {}).items())
        ]

    def exhausted(self) -> bool:
        """True when every partition has been read up to its snapshot end."""
        if not self._end:
            return False
        return all(
            self._pending_offsets.get(key, self._start.get(key, 0)) >= stop
            for key, stop in self._end.items()
        )

    def caught_up(self) -> bool:
        """True once every assigned partition is read up to the run's frozen end.

        An empty poll right after the group join is not the end of the log:
        the join can land at the end of a poll, before the consumer looked up
        its start offsets or fetched anything (D151). The fetch position is
        the authority, so control records and compacted gaps past the last
        delivered message do not keep a finished read polling.
        """
        if self._consumer is None or not self._end:
            return False
        from kafka import TopicPartition  # type: ignore

        for key, stop in self._end.items():
            if self._pending_offsets.get(key, self._start.get(key, 0)) >= stop:
                continue
            try:
                position = self._consumer.position(TopicPartition(*key), timeout_ms=250)
            except Exception:  # noqa: BLE001 - an unknown position is "not yet"
                return False
            if position is None or int(position) < stop:
                return False
        return True

    def poll_rows(self, *, max_records: int = 500, timeout_ms: int = 1000) -> Iterator[dict[str, Any]]:
        """Yield destination rows — Debezium envelopes preferred, else JSON passthrough."""
        if self._consumer is None:
            self.connect()
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not connected")
        registry_url = str(self.config.get("schema_registry_url") or "").strip()
        batch = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        self._snapshot_end_offsets()
        for tp, messages in batch.items():
            for msg in messages:
                key = (msg.topic, int(msg.partition))
                stop = (self._end or {}).get(key)
                if stop is not None and int(msg.offset) >= stop:
                    # Arrived after the run's snapshot: the next run's.
                    continue
                # Track next offset to commit after successful apply.
                nxt = int(msg.offset) + 1
                prev = self._pending_offsets.get(key)
                if prev is None or nxt > prev:
                    self._pending_offsets[key] = nxt
                if not msg.value:
                    self.tombstones_seen += 1
                row = kafka_value_to_row(
                    msg.value,
                    registry_url=registry_url,
                    meta=kafka_message_meta(msg),
                )
                if row:
                    yield row

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:
                logger.warning("Exception suppressed: %s", exc, exc_info=exc)
            self._consumer = None
            self._pending_offsets.clear()
