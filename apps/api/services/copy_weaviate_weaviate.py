"""Weaviate → Weaviate list + batch upsert (identity bulk).

Dest COUNT is Aggregate ``meta.count`` via GraphQL, never
``scan_source_ids`` DISTINCT source_id, never batch ack, never writer
``rows_written``. Empty dest is list+batch of raw id/properties/vector.
Python never vectorizes or re-embeds. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. Same host+port+class declines. Cross-endpoint declines.
Desktop-lab Weaviate on :8080 is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, copy onto the
same class, public proxy, leftover upsert.
This is **not** Weaviate backup/restore.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_weaviate_common import (
    skip_complete_weaviate,
    weaviate_class,
    weaviate_class_exists,
    weaviate_create_class_from_source,
    weaviate_delete_class,
    weaviate_endpoint_key,
    weaviate_list_batch_upsert,
    weaviate_object_count,
    weaviate_object_id,
    weaviate_proxy_fail_closed,
    weaviate_type_is_copy_safe,
)


def weaviate_weaviate_copy_enabled() -> bool:
    raw = (getenv_brand("WEAVIATE_WEAVIATE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_weaviate_to_weaviate(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    weaviate_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """List source objects and batch-upsert to dest. Dest meta.count is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(weaviate_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not weaviate_weaviate_copy_enabled():
        raise FastPathUnavailable("Weaviate→Weaviate COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Weaviate list+batch cannot rename columns")
    for declared in weaviate_ddls:
        if declared and not weaviate_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Weaviate COPY-safe"
            )
    if weaviate_proxy_fail_closed(source_cfg) or weaviate_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Weaviate bulk copy not assumed")
    src_class = weaviate_class(source_table, source_cfg)
    dest_class = weaviate_class(dest_table, dest_cfg)
    if weaviate_object_id(source_cfg, src_class) == weaviate_object_id(
        dest_cfg, dest_class
    ):
        raise FastPathUnavailable(
            "Weaviate COPY onto the same class stays on the row path"
        )
    if weaviate_endpoint_key(source_cfg) != weaviate_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-endpoint Weaviate COPY stays on the row path"
        )
    if not weaviate_class_exists(source_cfg, src_class):
        raise FastPathUnavailable("Weaviate source class missing")

    source_count = weaviate_object_count(source_cfg, src_class)
    if source_count <= 0:
        raise FastPathUnavailable("Weaviate source class empty")
    dest_existed = weaviate_class_exists(dest_cfg, dest_class)
    dest_count_before = (
        weaviate_object_count(dest_cfg, dest_class) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_weaviate(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"weaviate_write": "skip", "weaviate_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Weaviate dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = not dest_existed
    if replace_destination and dest_existed:
        weaviate_delete_class(dest_cfg, dest_class)
        created_here = True

    try:
        if not weaviate_class_exists(dest_cfg, dest_class):
            weaviate_create_class_from_source(
                dest_cfg=dest_cfg,
                source_cfg=source_cfg,
                source_class=src_class,
                dest_class=dest_class,
            )
            created_here = True
        weaviate_list_batch_upsert(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            src_class=src_class,
            dest_class=dest_class,
        )
        dest_count = weaviate_object_count(dest_cfg, dest_class)
        if dest_count != source_count:
            raise ValueError(
                "Weaviate→Weaviate COPY refused: dest meta.count "
                f"{dest_count} != source meta.count {source_count}"
            )
    except Exception:
        if created_here or dest_count_before == 0:
            weaviate_delete_class(dest_cfg, dest_class)
        raise

    w_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "class",
            "weaviate_read": "cursor_list",
            "weaviate_write": w_write,
            "weaviate_class": dest_class,
            "delivery_class": "at_least_once_upsert",
            "cdc_exactly_once_claimed": False,
            "production_sku": False,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
