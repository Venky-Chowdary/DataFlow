"""Kafka / Debezium topic reader — batch CDC envelopes into a ReadBatch matrix.

Honest scope: Debezium JSON envelopes (``op`` + ``before``/``after``) and plain
JSON object records. Requires ``kafka-python``. Durable offsets use the
consumer group (auto-commit).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys
from connectors.kafka_debezium_bridge import KafkaDebeziumConsumer

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def _bootstrap(cfg: dict[str, Any]) -> str:
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        if "bootstrap.servers=" in cs.lower():
            return cs.split("=", 1)[-1].strip()
        if "://" not in cs and ("|" in cs or ":" in cs):
            return cs.replace("bootstrap.servers=", "").strip()
    host = str(cfg.get("host") or cfg.get("bootstrap_servers") or "localhost").strip()
    port = int(cfg.get("port") or 9092)
    if "," in host:
        return host
    return f"{host}:{port}"


def read_topic_batch(
    *,
    cfg: dict[str, Any],
    topic: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    kafka_cursor: dict | None = None,
) -> tuple[ReadBatch, dict | None]:
    """Poll one batch from a Kafka topic. Returns ``(batch, cursor)``."""
    del offset
    topic_name = (topic or cfg.get("database") or cfg.get("table") or "").strip()
    if not topic_name:
        raise ValueError("Kafka source topic name required")

    group_id = str(
        (kafka_cursor or {}).get("group_id")
        or cfg.get("group_id")
        or cfg.get("username")
        or "dataflow-kafka-source"
    )
    consumer = KafkaDebeziumConsumer({
        "topic": topic_name,
        "bootstrap_servers": _bootstrap(cfg),
        "group_id": group_id,
        "auto_offset_reset": cfg.get("auto_offset_reset") or "earliest",
        "schema_registry_url": str(
            cfg.get("schema_registry_url") or cfg.get("registry_url") or ""
        ).strip(),
    })
    try:
        records: list[dict[str, Any]] = []
        polls = 0
        while len(records) < limit and polls < 5:
            polls += 1
            got = 0
            for row in consumer.poll_rows(
                max_records=min(limit - len(records), 500),
                timeout_ms=800,
            ):
                records.append(row)
                got += 1
                if len(records) >= limit:
                    break
            if got == 0:
                break

        page_keys: list[str] = []
        seen: set[str] = set()
        for rec in records:
            for k in rec.keys():
                if k not in seen:
                    seen.add(k)
                    page_keys.append(k)
        headers = union_attribute_keys(columns, page_keys) if columns else page_keys
        rows = [
            [cell_to_string(rec.get(h), preserve_sql_null=True) for h in headers]
            for rec in records
        ]
        total = known_total_rows if known_total_rows is not None else None
        batch = ReadBatch(headers=headers, rows=rows, offset=0, total_rows=total)
        next_cursor = {"group_id": group_id} if rows else None
        # Native types for schemaless absorb / Map honesty (pre-string samples).
        native_types: dict[str, str] = {}
        try:
            from services.schema_introspect import _kafka_value_to_logical

            for rec in records:
                for k, v in rec.items():
                    lt = _kafka_value_to_logical(v)
                    if not lt:
                        continue
                    prev = native_types.get(k)
                    if prev is None:
                        native_types[k] = lt
                    elif prev != lt and {prev, lt} <= {"INTEGER", "FLOAT", "DECIMAL"}:
                        native_types[k] = "DECIMAL" if "DECIMAL" in {prev, lt} else "FLOAT"
                    elif prev != lt:
                        native_types[k] = "TEXT"
            batch.meta = {"native_types": native_types}
        except Exception:
            pass
        return batch, next_cursor
    finally:
        consumer.close()


def infer_topic_schema(
    cfg: dict[str, Any],
    topic: str,
    *,
    sample_limit: int = 50,
) -> tuple[dict[str, str], dict[str, str], str]:
    """Poll a small sample and return ``(logical_schema, native_types, warning)``.

    Empty topics do **not** invent TEXT columns — Map/preflight must wait for samples.
    """
    batch, _ = read_topic_batch(cfg=cfg, topic=topic, limit=sample_limit)
    native = dict((batch.meta or {}).get("native_types") or {})
    if not batch.headers:
        return (
            {},
            {},
            "Kafka topic has no samples yet; field types are unknown until messages arrive.",
        )
    if native:
        return {h: native.get(h, "TEXT") for h in batch.headers}, native, ""
    # Fallback: infer from string matrix
    try:
        from services.file_parser import FileParser

        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        inferred = FileParser.infer_schema(records) if records else {}
        schema = {h: inferred.get(h, "TEXT") for h in batch.headers}
        return schema, schema, ""
    except Exception:
        return {h: "TEXT" for h in batch.headers}, {}, ""
