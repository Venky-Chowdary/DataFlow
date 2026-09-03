"""Elasticsearch → Elasticsearch cluster ``_reindex`` (identity bulk).

Dest COUNT is cluster ``_count`` after refresh via
``destination_row_count``, never ``_cat/indices`` ``docs.count``, never
reindex ``created`` ack, never ``helpers.bulk`` / ``helpers.scan``.
Empty dest is ``_reindex``. Python never scrolls the payload for the
copy. Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines. Same
host+port+index declines. Cross-endpoint declines. Occupancy is counted
**before** delete. Desktop-lab Elasticsearch on :9200 is not a
customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, cross-endpoint,
copy onto the same index, public proxy, scripts, leftover upsert.
This is **not** clone (clone requires source read-only) and **not**
snapshot restore.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_elasticsearch_common import (
    elasticsearch_delete_index,
    elasticsearch_dest_can_read_source,
    elasticsearch_dest_count,
    elasticsearch_endpoint_key,
    elasticsearch_index,
    elasticsearch_index_exists,
    elasticsearch_object_id,
    elasticsearch_proxy_fail_closed,
    elasticsearch_reindex,
    elasticsearch_type_is_copy_safe,
    skip_complete_elasticsearch,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry


def elasticsearch_elasticsearch_copy_enabled() -> bool:
    raw = (getenv_brand("ELASTICSEARCH_ELASTICSEARCH_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_elasticsearch_to_elasticsearch(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    elasticsearch_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """``_reindex`` source index onto dest index. Dest ``_count`` is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(elasticsearch_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not elasticsearch_elasticsearch_copy_enabled():
        raise FastPathUnavailable("Elasticsearch→Elasticsearch COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Elasticsearch _reindex cannot rename columns")
    for declared in elasticsearch_ddls:
        if declared and not elasticsearch_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Elasticsearch COPY-safe"
            )
    if elasticsearch_proxy_fail_closed(source_cfg) or elasticsearch_proxy_fail_closed(
        dest_cfg
    ):
        raise FastPathUnavailable("public proxy: Elasticsearch bulk copy not assumed")
    src_index = elasticsearch_index(source_table)
    dest_index = elasticsearch_index(dest_table)
    if elasticsearch_object_id(source_cfg, src_index) == elasticsearch_object_id(
        dest_cfg, dest_index
    ):
        raise FastPathUnavailable(
            "Elasticsearch COPY onto the same index stays on the row path"
        )
    if elasticsearch_endpoint_key(source_cfg) != elasticsearch_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-endpoint Elasticsearch COPY stays on the row path"
        )
    if not elasticsearch_index_exists(source_cfg, src_index):
        raise FastPathUnavailable("Elasticsearch source index missing")
    elasticsearch_dest_can_read_source(dest_cfg, src_index)

    source_count = elasticsearch_dest_count(source_cfg, src_index)
    if source_count <= 0:
        raise FastPathUnavailable("Elasticsearch source index missing")
    dest_existed = elasticsearch_index_exists(dest_cfg, dest_index)
    dest_count_before = (
        elasticsearch_dest_count(dest_cfg, dest_index) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_elasticsearch(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"elasticsearch_write": "skip", "elasticsearch_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Elasticsearch dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = not dest_existed
    if replace_destination and dest_existed:
        elasticsearch_delete_index(dest_cfg, dest_index)
        created_here = True

    try:
        elasticsearch_reindex(
            dest_cfg=dest_cfg,
            src_index=src_index,
            dest_index=dest_index,
        )
        dest_count = elasticsearch_dest_count(dest_cfg, dest_index)
        if dest_count != source_count:
            raise ValueError(
                "Elasticsearch→Elasticsearch COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        if created_here or dest_count_before == 0:
            elasticsearch_delete_index(dest_cfg, dest_index)
        raise

    es_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "copy_partitions": 1,
            "partitions_skipped": 0,
            "partitions_loaded": 1,
            "shard_mode": "index",
            "elasticsearch_read": "reindex",
            "elasticsearch_write": es_write,
            "elasticsearch_index": dest_index,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
