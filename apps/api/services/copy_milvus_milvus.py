"""Milvus → Milvus query + upsert (identity bulk).

Dest COUNT is ``count(*)`` from entities/query, never ``scan_source_ids``
DISTINCT source_id, never upsert ack, never writer ``rows_written``. Empty
dest is query+upsert of raw entity fields. Python never vectorizes or
re-embeds. Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines. Same
host+port+collection declines. Cross-endpoint declines. Desktop-lab Milvus
on :19530 is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, copy onto the
same collection, public proxy, leftover upsert.
This is **not** Milvus backup/restore and **not** the Milvus migration CLI.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_milvus_common import (
    milvus_collection,
    milvus_collection_exists,
    milvus_create_collection_from_source,
    milvus_delete_collection,
    milvus_endpoint_key,
    milvus_entity_count,
    milvus_object_id,
    milvus_proxy_fail_closed,
    milvus_query_upsert,
    milvus_type_is_copy_safe,
    skip_complete_milvus,
)
from services.copy_pg_mysql import mapping_is_plain_carry


def milvus_milvus_copy_enabled() -> bool:
    raw = (getenv_brand("MILVUS_MILVUS_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_milvus_to_milvus(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    milvus_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Query source entities and upsert to dest. Dest count(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(milvus_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not milvus_milvus_copy_enabled():
        raise FastPathUnavailable("Milvus→Milvus COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Milvus query+upsert cannot rename columns")
    for declared in milvus_ddls:
        if declared and not milvus_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Milvus COPY-safe"
            )
    if milvus_proxy_fail_closed(source_cfg) or milvus_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Milvus bulk copy not assumed")
    src_collection = milvus_collection(source_table, source_cfg)
    dest_collection = milvus_collection(dest_table, dest_cfg)
    if milvus_object_id(source_cfg, src_collection) == milvus_object_id(
        dest_cfg, dest_collection
    ):
        raise FastPathUnavailable(
            "Milvus COPY onto the same collection stays on the row path"
        )
    if milvus_endpoint_key(source_cfg) != milvus_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-endpoint Milvus COPY stays on the row path"
        )
    if not milvus_collection_exists(source_cfg, src_collection):
        raise FastPathUnavailable("Milvus source collection missing")

    source_count = milvus_entity_count(source_cfg, src_collection)
    if source_count <= 0:
        raise FastPathUnavailable("Milvus source collection empty")
    dest_existed = milvus_collection_exists(dest_cfg, dest_collection)
    dest_count_before = (
        milvus_entity_count(dest_cfg, dest_collection) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_milvus(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"milvus_write": "skip", "milvus_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Milvus dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = not dest_existed
    if replace_destination and dest_existed:
        milvus_delete_collection(dest_cfg, dest_collection)
        created_here = True

    try:
        if not milvus_collection_exists(dest_cfg, dest_collection):
            milvus_create_collection_from_source(
                dest_cfg=dest_cfg,
                source_cfg=source_cfg,
                source_collection=src_collection,
                dest_collection=dest_collection,
            )
            created_here = True
        milvus_query_upsert(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            src_collection=src_collection,
            dest_collection=dest_collection,
        )
        dest_count = milvus_entity_count(dest_cfg, dest_collection)
        if dest_count != source_count:
            raise ValueError(
                "Milvus→Milvus COPY refused: dest count(*) "
                f"{dest_count} != source count(*) {source_count}"
            )
    except Exception:
        if created_here or dest_count_before == 0:
            milvus_delete_collection(dest_cfg, dest_collection)
        raise

    m_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "collection",
            "milvus_read": "pk_query",
            "milvus_write": m_write,
            "milvus_collection": dest_collection,
            "delivery_class": "at_least_once_upsert",
            "cdc_exactly_once_claimed": False,
            "production_sku": False,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
