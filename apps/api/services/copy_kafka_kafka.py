"""Kafka → Kafka consume-produce of raw bytes (identity bulk).

Apache Kafka has no broker ``COPY TOPIC``. MirrorMaker 2 / Cluster Linking
is consume+produce of bytes across clusters — this path is **not** MM2
and stays on one cluster. Dest COUNT is log-end minus log-start via
``destination_row_count``, never producer ack, never poll count, never
leftover compaction MERGE. Empty dest is consume raw bytes (no
deserializer / no ``json.loads``) and produce identical
key/value/headers/timestamp with dest credentials. Occupied dest whose
COUNT already equals the source COUNT is skip-complete. Occupied dest
with a different COUNT declines. Same bootstrap+topic declines.
Cross-endpoint declines. Occupancy is counted **before** delete.
Desktop-lab Kafka / Redpanda on :9092 is not a customer-tenant
PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, cross-endpoint,
copy onto the same topic, public proxy, partition-count mismatch on an
existing empty dest.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_kafka_common import (
    kafka_copy_bytes,
    kafka_create_topic,
    kafka_delete_topic,
    kafka_dest_can_write,
    kafka_dest_count,
    kafka_endpoint_key,
    kafka_object_id,
    kafka_partition_count,
    kafka_proxy_fail_closed,
    kafka_snapshot_watermarks,
    kafka_topic,
    kafka_topic_exists,
    kafka_type_is_copy_safe,
    skip_complete_kafka,
)
from services.copy_pg_mysql import mapping_is_plain_carry


def kafka_kafka_copy_enabled() -> bool:
    raw = (getenv_brand("KAFKA_KAFKA_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_kafka_to_kafka(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    kafka_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Consume source bytes onto dest. Dest watermark COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(kafka_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not kafka_kafka_copy_enabled():
        raise FastPathUnavailable("Kafka→Kafka COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Kafka identity COPY cannot rename columns")
    for declared in kafka_ddls:
        if declared and not kafka_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Kafka COPY-safe"
            )
    if kafka_proxy_fail_closed(source_cfg) or kafka_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Kafka bulk copy not assumed")
    src_topic = kafka_topic(source_table)
    dest_topic = kafka_topic(dest_table)
    if kafka_object_id(source_cfg, src_topic) == kafka_object_id(dest_cfg, dest_topic):
        raise FastPathUnavailable(
            "Kafka COPY onto the same topic stays on the row path"
        )
    if kafka_endpoint_key(source_cfg) != kafka_endpoint_key(dest_cfg):
        raise FastPathUnavailable("cross-endpoint Kafka COPY stays on the row path")

    kafka_dest_can_write(dest_cfg)
    source_count, tps, begin, end = kafka_snapshot_watermarks(source_cfg, src_topic)
    if source_count <= 0:
        raise FastPathUnavailable("Kafka source topic missing")
    src_parts = len(tps)
    dest_existed = kafka_topic_exists(dest_cfg, dest_topic)
    dest_count_before = (
        kafka_dest_count(dest_cfg, dest_topic) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_kafka(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"kafka_write": "skip", "kafka_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Kafka dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = not dest_existed
    if replace_destination and dest_existed:
        kafka_delete_topic(dest_cfg, dest_topic)
        created_here = True
        dest_existed = False

    if dest_existed:
        dest_parts = kafka_partition_count(dest_cfg, dest_topic)
        if dest_parts != src_parts:
            raise FastPathUnavailable(
                "Kafka dest partition count does not match source"
            )
        pin_partition = True
    else:
        kafka_create_topic(dest_cfg, dest_topic, num_partitions=src_parts)
        pin_partition = True

    try:
        kafka_copy_bytes(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            src_topic=src_topic,
            dest_topic=dest_topic,
            tps=tps,
            begin=begin,
            end=end,
            pin_partition=pin_partition,
        )
        dest_count = kafka_dest_count(dest_cfg, dest_topic)
        if dest_count != source_count:
            raise ValueError(
                "Kafka→Kafka COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        if created_here or dest_count_before == 0:
            kafka_delete_topic(dest_cfg, dest_topic)
        raise

    kafka_write = "overwrite" if replace_destination and dest_occupied else "insert"
    proof = f"dest_count:{dest_count}"
    return FastPathResult(
        rows_copied=dest_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot={
            "copy_workers": 1,
            "copy_split": "serial",
            "copy_partitions": src_parts,
            "partitions_skipped": 0,
            "partitions_loaded": 1,
            "shard_mode": "topic",
            "kafka_read": "byte_records",
            "kafka_write": kafka_write,
            "kafka_topic": dest_topic,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
