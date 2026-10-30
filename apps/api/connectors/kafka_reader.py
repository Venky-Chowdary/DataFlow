"""Kafka / Debezium topic reader — batch CDC envelopes into a ReadBatch matrix.

Honest scope: Debezium JSON envelopes (``op`` + ``before``/``after``) and plain
JSON object records. Requires ``kafka-python``.

Durable offsets use the consumer group with **commit-after-accept**. Once the
job is accepted (destination committed and Gate-8 passed),
``commit_kafka_offsets`` assigns the pending partitions and commits —
subscribe-then-commit on a fresh consumer is kicked out of the group.
Crash mid-write redelivers — at-least-once.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
import sys
import uuid
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys
from connectors.kafka_debezium_bridge import KAFKA_ENVELOPE_TYPES, KafkaDebeziumConsumer

_api_root = Path(__file__).resolve().parents[1]

logger = logging.getLogger(__name__)
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


def accepted_kafka_configs(kwargs: dict[str, Any], accepted: set[str] | None) -> dict[str, Any]:
    """Drop client configs this kafka-python build does not recognize.

    ``api_version_auto_timeout_ms`` is a real kafka-python timeout on some
    releases and ``KafkaConfigurationError: Unrecognized configs`` on others.
    Topic listing used to fail closed on that mismatch and Gate-2 then reported
    the topic as unknown.
    """
    if not accepted:
        return dict(kwargs)
    return {key: value for key, value in kwargs.items() if key in accepted}


def _consumer_configs(cfg: dict[str, Any], **extra: Any) -> dict[str, Any]:
    # SASL login is mapped once for every kafka-python client; a consumer
    # without it never authenticates and the broker only "times out" (MX1-03).
    from services.copy_kafka_common import kafka_client_kwargs

    kwargs: dict[str, Any] = kafka_client_kwargs(cfg)
    kwargs.update(extra)
    try:
        from kafka import KafkaConsumer
    except ImportError:
        return kwargs
    accepted = set(getattr(KafkaConsumer, "DEFAULT_CONFIG", {}) or {})
    return accepted_kafka_configs(kwargs, accepted or None)


def list_topics(cfg: dict[str, Any]) -> list[str]:
    """User topics on the cluster. Internal topics (``_`` prefix) stay out.

    An empty list is a real empty cluster, not a missing dialect. Import
    failure is raised so the caller can say kafka-python is absent.
    """
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise ImportError("kafka-python is required to list Kafka topics") from exc

    from services.copy_kafka_common import kafka_classified_bootstrap

    with kafka_classified_bootstrap(cfg):
        consumer = KafkaConsumer(
            **_consumer_configs(
                cfg,
                consumer_timeout_ms=2000,
                request_timeout_ms=8000,
                api_version_auto_timeout_ms=8000,
            )
        )
    try:
        names = consumer.topics() or set()
    finally:
        consumer.close()
    return sorted(
        name for name in names if name and not str(name).startswith("_")
    )


def _bootstrap(cfg: dict[str, Any]) -> str:
    cs = str(cfg.get("connection_string") or "").strip()
    if cs:
        if "bootstrap.servers=" in cs.lower():
            return cs.split("=", 1)[-1].strip()
        if "://" not in cs and ("|" in cs or ":" in cs):
            return cs.replace("bootstrap.servers=", "").strip()
    host = str(cfg.get("host") or cfg.get("bootstrap_servers") or "localhost").strip()
    port = int(cfg.get("port") or 9092)
    # D208 sibling: ``PLAINTEXT://h:9092`` / ``kafka://h:9092`` name their own
    # port; the scheme is not part of a bootstrap address.
    host = ",".join(re.sub(r"^[A-Za-z][A-Za-z0-9+._-]*://", "", h.strip()).rstrip("/")
                    for h in host.split(","))
    if "," in host or re.search(r"(^[^:]+|\])(:\d+)$", host):
        return host
    return f"{host}:{port}"


#: Endpoint extra the engine stamps with the route's derived consumer group.
KAFKA_ROUTE_GROUP_KEY = "kafka_route_group_id"


def configured_kafka_group_id(cfg: dict[str, Any]) -> str:
    """The consumer group a Kafka source reads and commits under (D84).

    The connector's (or endpoint's) explicit ``group_id`` wins; otherwise the
    group the engine derived for this route. Empty when neither is set. There
    is deliberately no shared default and no username fallback: every
    connector without one used to share "dataflow-kafka-source", so pipelines
    rebalanced each other and committed over each other's position.
    """
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    for key in ("group_id", KAFKA_ROUTE_GROUP_KEY):
        value = str(cfg.get(key) or extra.get(key) or "").strip()
        if value:
            return value
    return ""


#: Endpoint extra the engine stamps with where this run starts reading.
KAFKA_START_KEY = "kafka_start_position"


def configured_kafka_start(cfg: dict[str, Any]) -> str:
    """Where a Kafka source read starts: ``"committed"`` or ``"earliest"``.

    ``committed`` continues from the consumer group's committed offsets (an
    incremental run). Everything else reads from the log start: a full
    refresh re-reads the topic whatever an earlier run committed, and a read
    with no group of its own (a preview or Validate sample) has no business
    skipping messages. Reading the committed position on a full refresh loaded
    0 rows on every run after the first (D151).
    """
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    value = str(cfg.get(KAFKA_START_KEY) or extra.get(KAFKA_START_KEY) or "").strip()
    if value:
        return "committed" if value == "committed" else "earliest"
    # Not stamped by a run: a read under the operator's own group follows it.
    return "committed" if configured_kafka_group_id(cfg) else "earliest"


def kafka_route_group_id(source: Any, destination: Any) -> str:
    """A stable consumer group per route: source endpoint + topic → destination."""
    from services.sync_cursor import build_cursor_key, route_endpoint_identity

    route = build_cursor_key(
        source_type="kafka",
        source_database=route_endpoint_identity(source),
        source_object=str(
            getattr(source, "table", "")
            or getattr(source, "database", "")
            or getattr(source, "collection", "")
            or ""
        ),
        dest_type=str(getattr(destination, "format", "") or ""),
        dest_database=str(getattr(destination, "database", "") or ""),
        dest_object=str(
            getattr(destination, "table", "") or getattr(destination, "collection", "") or ""
        ),
        stream_name="kafka",
        dest_identity=route_endpoint_identity(destination),
    )
    return "dataflow-kafka-" + hashlib.sha256(route.encode("utf-8")).hexdigest()[:16]


def commit_kafka_offsets(cfg: dict[str, Any], cursor: dict[str, Any] | None) -> None:
    """Commit previously applied offsets (call after durable checkpoint).

    Manual ``assign`` — not subscribe. A fresh subscribed consumer has no
    partition assignment yet; kafka-python then raises CommitFailedError
    ("kicked out of the group") even though dest apply already succeeded.
    Assign + commit writes the group offsets without a rebalance race.
    At-least-once: dest apply happened first; a crash before this commit
    redelivers.
    """
    if not cursor:
        return
    pending = cursor.get("pending_offsets") or cursor.get("offsets")
    if not pending:
        return
    group_id = str(cursor.get("group_id") or configured_kafka_group_id(cfg)).strip()
    if not group_id:
        raise ValueError("Kafka offset commit needs the consumer group the run read under")
    try:
        from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
    except ImportError as exc:
        raise ImportError(
            "kafka-python is required to commit Kafka offsets"
        ) from exc

    tps: list[Any] = []
    tps_meta: dict[Any, Any] = {}
    for item in pending:
        if not isinstance(item, dict):
            continue
        topic = str(
            item.get("topic")
            or cursor.get("topic")
            or cfg.get("table")
            or cfg.get("database")
            or ""
        ).strip()
        if not topic or item.get("partition") is None or item.get("offset") is None:
            continue
        tp = TopicPartition(topic, int(item["partition"]))
        tps.append(tp)
        tps_meta[tp] = OffsetAndMetadata(int(item["offset"]), None, -1)
    if not tps_meta:
        return
    from services.copy_kafka_common import kafka_classified_bootstrap

    # Same SASL mapping as every other client: a consumer without it never
    # authenticates and the fail-closed commit fails the job (MX1-03).
    with kafka_classified_bootstrap(cfg):
        consumer = KafkaConsumer(
            **_consumer_configs(
                cfg,
                group_id=group_id,
                enable_auto_commit=False,
                consumer_timeout_ms=2000,
                request_timeout_ms=8000,
            )
        )
    try:
        consumer.assign(tps)
        consumer.commit(tps_meta)
    finally:
        consumer.close()


_JOIN_WAIT_S = 15.0


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

    # Not configured and not a transfer run (a preview or Validate sample):
    # a group of its own, which never commits, so it cannot rebalance a
    # running pipeline's consumer away from its partitions (D87).
    group_id = str(
        (kafka_cursor or {}).get("group_id")
        or configured_kafka_group_id(cfg)
        or f"dataflow-kafka-probe-{uuid.uuid4().hex[:12]}"
    )
    # Durable commits happen only once the job is accepted (engine
    # _commit_kafka_offsets_on_accept). Never commit prior pending offsets
    # here — prefetch can race ahead of sink durability. The group therefore
    # still sits where the run began, so a continuation batch resumes at the
    # offsets this run already read instead of re-reading them (D85).
    start_position = (kafka_cursor or {}).get("start_position") or configured_kafka_start(cfg)
    consumer = KafkaDebeziumConsumer({
        **cfg,
        "topic": topic_name,
        "bootstrap_servers": _bootstrap(cfg),
        "group_id": group_id,
        "start_position": start_position,
        "start_offsets": list((kafka_cursor or {}).get("pending_offsets") or []),
        "end_offsets": list((kafka_cursor or {}).get("end_offsets") or []),
        "auto_offset_reset": cfg.get("auto_offset_reset") or "earliest",
        "schema_registry_url": str(
            cfg.get("schema_registry_url") or cfg.get("registry_url") or ""
        ).strip(),
    })
    try:
        records: list[dict[str, Any]] = []
        polls = 0
        # Until the group join assigns partitions, an empty poll is not
        # "caught up": the broker delays a new group's first join (3 s default).
        join_deadline = time.monotonic() + _JOIN_WAIT_S
        while len(records) < limit and polls < 5:
            got = 0
            for row in consumer.poll_rows(
                max_records=min(limit - len(records), 500),
                timeout_ms=800,
            ):
                records.append(row)
                got += 1
                if len(records) >= limit:
                    break
            if consumer.has_assignment():
                polls += 1
            elif time.monotonic() < join_deadline:
                continue
            if got == 0:
                break

        # Poison (undecodable) messages go to the rejected-rows channel with
        # their broker coordinates — never land as all-NULL destination rows.
        data_records = [r for r in records if not r.get("_kafka_poison")]
        rejected = [
            {
                "row": {k: str(v) for k, v in r.items()},
                "reason": f"kafka poison message: {r['_kafka_poison']}",
            }
            for r in records
            if r.get("_kafka_poison")
        ]
        tombstones = int(getattr(consumer, "tombstones_seen", 0) or 0)
        batch = _records_to_batch(data_records, columns, known_total_rows)
        if rejected or tombstones:
            batch.meta = dict(batch.meta or {})
            if rejected:
                batch.meta["rejected_rows"] = rejected
            if tombstones:
                batch.meta["kafka_tombstones_seen"] = tombstones
        pending = consumer.pending_offsets()
        # The run reads the log up to the end it saw at its first poll; the
        # batch that reaches it is the last page (a staged overwrite
        # publishes there).
        exhausted = consumer.exhausted()
        if exhausted:
            batch.meta = dict(batch.meta or {})
            batch.meta["final_page"] = True
        # Emit a continuation cursor whenever offsets were consumed — including
        # tombstone / decode-skip batches that produce no output rows. Without
        # this, those offsets never checkpoint and the consumer re-reads forever.
        next_cursor = (
            {
                "group_id": group_id,
                "topic": topic_name,
                "pending_offsets": pending,
                "end_offsets": consumer.end_offsets_snapshot(),
                "exhausted": exhausted,
                "start_position": start_position,
                "delivery": "at_least_once",
            }
            if pending
            else None
        )
        return batch, next_cursor
    finally:
        consumer.close()


_KAFKA_NUMERIC_LOGICALS = frozenset({"INTEGER", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE"})


def merge_kafka_logicals(prev: str | None, logical: str) -> str:
    """One column's type across messages: widen within numbers, else TEXT.

    The decoder types a JSON float DOUBLE and a large int BIGINT, but the
    merge only knew INTEGER/FLOAT/DECIMAL, so ``"price": 3`` in one message
    and ``2.5`` in the next typed the column TEXT (D161 on the Kafka path).
    A JSON number is IEEE on the wire: an integer meeting a double is the
    double, and DECIMAL (a decimal string) stays DECIMAL.
    """
    if not prev or prev == logical:
        return logical
    pair = {prev, logical}
    if not pair <= _KAFKA_NUMERIC_LOGICALS:
        return "TEXT"
    if "DECIMAL" in pair:
        return "DECIMAL"
    if "DOUBLE" in pair:
        return "DOUBLE"
    if "FLOAT" in pair:
        return "FLOAT"
    return "BIGINT"


def _records_to_batch(
    records: list[dict[str, Any]],
    columns: list[str] | None,
    known_total_rows: int | None,
) -> ReadBatch:
    """Headers, string matrix, and native types for one page of Kafka rows."""
    page_keys: list[str] = []
    seen: set[str] = set()
    for rec in records:
        for key in rec.keys():
            if key not in seen:
                seen.add(key)
                page_keys.append(key)
    headers = union_attribute_keys(columns, page_keys) if columns else page_keys
    rows = [
        [cell_to_string(rec.get(header), preserve_sql_null=True) for header in headers]
        for rec in records
    ]
    total = known_total_rows if known_total_rows is not None else None
    batch = ReadBatch(headers=headers, rows=rows, offset=0, total_rows=total)
    native_types: dict[str, str] = {}
    try:
        from services.schema_introspect import _kafka_value_to_logical

        for rec in records:
            for key, value in rec.items():
                logical = _kafka_value_to_logical(value)
                if logical:
                    native_types[key] = merge_kafka_logicals(native_types.get(key), logical)
        native_types.update(
            (col, logical)
            for col, logical in KAFKA_ENVELOPE_TYPES.items()
            if col in headers
        )
        batch.meta = {"native_types": native_types}
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return batch


def _registry_url(cfg: dict[str, Any]) -> str:
    return str(cfg.get("schema_registry_url") or cfg.get("registry_url") or "").strip()


def _open_assigned(
    cfg: dict[str, Any], topic_name: str
) -> tuple[Any, list[Any], dict[Any, int], dict[Any, int]]:
    """A consumer assigned every partition of ``topic_name``, with its offsets.

    Returns ``(consumer, partitions, beginning, end)``; no partitions when the
    topic does not exist. It joins no consumer group and never commits. The
    caller closes the consumer.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError as exc:
        raise ImportError("kafka-python is required to read a Kafka topic") from exc
    from services.copy_kafka_common import kafka_classified_bootstrap

    with kafka_classified_bootstrap(cfg):
        consumer = KafkaConsumer(
            **_consumer_configs(
                cfg,
                enable_auto_commit=False,
                consumer_timeout_ms=2000,
                request_timeout_ms=8000,
                value_deserializer=lambda raw: raw if raw is not None else raw,
            )
        )
    try:
        parts = consumer.partitions_for_topic(topic_name)
        if not parts:
            # Metadata is often empty until the first lookup. ``None`` here
            # is "not loaded yet", which a sample used to treat as no rows.
            try:
                consumer.topics()
            except Exception as exc:
                logger.debug("Kafka metadata refresh failed: %s", exc)
            parts = consumer.partitions_for_topic(topic_name)
        if not parts:
            return consumer, [], {}, {}
        tps = [TopicPartition(topic_name, int(part)) for part in sorted(parts)]
        consumer.assign(tps)
        begin = consumer.beginning_offsets(tps) or {}
        end = consumer.end_offsets(tps) or {}
        return consumer, tps, begin, end
    except BaseException:
        consumer.close()
        raise


#: How long a read-to-end may take before it reports the topic unread.
_READ_TO_END_S = 120.0


def read_topic_to_end(
    *,
    cfg: dict[str, Any],
    topic: str,
    columns: list[str] | None = None,
    limit: int,
) -> tuple[ReadBatch, bool]:
    """Every message from the log start to the end offsets at call time.

    For Gate-8 read-back of a Kafka destination. The transfer reader stops
    after a few polls (one batch), so a read-back of a 3,000-message produce
    saw 2,500 and failed a job whose every row was written — an invitation
    to a duplicating retry (D152). This reads each partition up to its end
    offset. Returns ``(batch, complete)``; ``complete`` is False when ``limit``
    or the time budget stopped the read first, so the caller reports the
    topic unverified instead of under-counted. No group, no commit.
    """
    topic_name = (topic or cfg.get("database") or cfg.get("table") or "").strip()
    if not topic_name:
        raise ValueError("Kafka topic name required")
    from connectors.kafka_debezium_bridge import kafka_message_meta, kafka_value_to_row

    consumer, tps, begin, end = _open_assigned(cfg, topic_name)
    registry_url = _registry_url(cfg)
    try:
        remaining = {
            tp: int(end.get(tp, 0) or 0)
            for tp in tps
            if int(end.get(tp, 0) or 0) > int(begin.get(tp, 0) or 0)
        }
        for tp in remaining:
            consumer.seek(tp, int(begin.get(tp, 0) or 0))
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + _READ_TO_END_S
        while remaining and len(records) < limit and time.monotonic() < deadline:
            polled = consumer.poll(timeout_ms=800, max_records=500) or {}
            for tp, messages in polled.items():
                stop = remaining.get(tp)
                for msg in messages:
                    if stop is None or int(msg.offset) >= stop:
                        break
                    row = kafka_value_to_row(
                        msg.value, registry_url=registry_url, meta=kafka_message_meta(msg)
                    )
                    if row:
                        records.append(row)
            for tp in list(remaining):
                if consumer.position(tp) >= remaining[tp]:
                    del remaining[tp]
        return _records_to_batch(records, columns, None), not remaining
    finally:
        consumer.close()


def sample_topic_batch(
    *,
    cfg: dict[str, Any],
    topic: str,
    limit: int = 50,
) -> ReadBatch:
    """Read up to ``limit`` records that are already on the topic.

    The transfer reader subscribes with the pipeline consumer group. After a
    load that group sits at the end, and the first poll during a rebalance
    is empty, so a sample reported 0 rows on a topic that had data. This
    assigns the partitions and seeks to the tail. It does not join that
    group and it does not commit.
    """
    topic_name = (topic or cfg.get("database") or cfg.get("table") or "").strip()
    if not topic_name:
        raise ValueError("Kafka source topic name required")
    from connectors.kafka_debezium_bridge import kafka_message_meta, kafka_value_to_row

    consumer, tps, begin, end = _open_assigned(cfg, topic_name)
    registry_url = _registry_url(cfg)
    try:
        if not tps:
            return _records_to_batch([], None, 0)
        window = max(1, int(limit))
        for tp in tps:
            start = max(int(begin.get(tp, 0) or 0), int(end.get(tp, 0) or 0) - window)
            consumer.seek(tp, start)
        records: list[dict[str, Any]] = []
        polls = 0
        while len(records) < window and polls < 5:
            polls += 1
            got = 0
            polled = consumer.poll(timeout_ms=800, max_records=window) or {}
            for messages in polled.values():
                for msg in messages:
                    # Same envelope as the transfer consumer, so the plan
                    # built from this sample shows every column the run emits.
                    row = kafka_value_to_row(
                        msg.value, registry_url=registry_url, meta=kafka_message_meta(msg)
                    )
                    if not row:
                        continue
                    records.append(row)
                    got += 1
                    if len(records) >= window:
                        break
                if len(records) >= window:
                    break
            if got == 0:
                break
        return _records_to_batch(records, None, None)
    finally:
        consumer.close()


def infer_topic_schema(
    cfg: dict[str, Any],
    topic: str,
    *,
    sample_limit: int = 50,
) -> tuple[dict[str, str], dict[str, str], str]:
    """Resolve topic field types from Schema Registry first, else poll samples.

    Empty topics do **not** invent TEXT columns when no registry contract exists.
    Schema sampling uses an ephemeral group so it never advances the transfer cursor.
    """
    registry = str(cfg.get("schema_registry_url") or "").strip()
    if registry and topic:
        from connectors.confluent_schema_registry import (
            SchemaRegistryError,
            fetch_latest_subject_schema,
            schema_map_from_registry_doc,
        )

        # Confluent TopicNameStrategy subjects — try value then topic bare.
        subjects = [f"{topic}-value", topic, f"{topic}-key"]
        last_err = ""
        for subject in subjects:
            try:
                doc = fetch_latest_subject_schema(registry, subject)
                schema_map = schema_map_from_registry_doc(doc)
                if schema_map:
                    # The registry describes the value only; the reader also
                    # emits the broker envelope, which the plan must show.
                    schema_map = {**schema_map, **KAFKA_ENVELOPE_TYPES}
                    return (
                        schema_map,
                        dict(schema_map),
                        f"Schema from Registry subject {subject} "
                        f"(schemaType={doc.get('schemaType') or 'AVRO'})",
                    )
            except SchemaRegistryError as exc:
                last_err = str(exc)
                continue
            except Exception as exc:
                last_err = str(exc)
                continue
        # Registry configured but no usable subject — fall through to samples with warning.
        registry_warning = (
            f"Schema Registry configured but no usable subject for topic {topic}"
            + (f": {last_err}" if last_err else "")
        )
    else:
        registry_warning = ""

    # Schema sampling must not join the transfer group. A committed offset
    # at the end, or the empty poll during rebalance, made this look like
    # a topic with no fields.
    batch = sample_topic_batch(cfg=cfg, topic=topic, limit=sample_limit)
    native = dict((batch.meta or {}).get("native_types") or {})
    if not batch.headers:
        warning = registry_warning or (
            "Kafka topic has no samples yet; field types are unknown until messages arrive."
        )
        return {}, {}, warning
    if native:
        return {h: native.get(h, "TEXT") for h in batch.headers}, native, registry_warning
    # Fallback: infer from string matrix
    try:
        from services.file_parser import FileParser

        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        inferred = FileParser.infer_schema(records) if records else {}
        schema = {h: inferred.get(h, "TEXT") for h in batch.headers}
        return schema, schema, registry_warning
    except Exception:
        return {h: "TEXT" for h in batch.headers}, {}, registry_warning
