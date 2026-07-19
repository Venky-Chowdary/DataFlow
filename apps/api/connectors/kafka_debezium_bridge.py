"""Optional Kafka / Debezium envelope → DataFlow change-stream adapter.

Enterprises that already run Kafka Connect + Debezium can point DataFlow at a
topic and consume CDC envelopes without embedding Kafka in the default deploy.
This module is a thin bridge — not required for native PG/MySQL/Mongo CDC.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterator


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
            payload = json.loads(payload)
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


class KafkaDebeziumConsumer:
    """Optional kafka-python consumer. Requires ``kafka-python`` extra.

    Config: ``bootstrap_servers``, ``topic``, ``group_id``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._consumer = None

    def connect(self) -> None:
        try:
            from kafka import KafkaConsumer  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "kafka-python is required for the Debezium bridge. "
                "Install with: pip install kafka-python"
            ) from exc
        self._consumer = KafkaConsumer(
            self.config["topic"],
            bootstrap_servers=self.config.get("bootstrap_servers") or "localhost:9092",
            group_id=self.config.get("group_id") or "dataflow-debezium",
            auto_offset_reset=self.config.get("auto_offset_reset") or "earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: b.decode("utf-8") if b else "",
        )

    def poll_changes(self, *, max_records: int = 500, timeout_ms: int = 1000) -> Iterator[DebeziumChange]:
        if self._consumer is None:
            self.connect()
        assert self._consumer is not None
        batch = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        for _tp, messages in batch.items():
            for msg in messages:
                change = parse_debezium_envelope(msg.value)
                if change is not None:
                    yield change

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
