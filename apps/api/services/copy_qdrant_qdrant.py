"""Qdrant → Qdrant scroll + upsert (identity bulk).

Dest COUNT is collection ``points_count`` from GET /collections/{name},
never ``scan_source_ids`` DISTINCT source_id, never upsert ack, never
writer ``rows_written``. Empty dest is scroll+upsert of raw
id/vector/payload. Python never vectorizes or re-embeds. Occupied dest
whose COUNT already equals the source COUNT is skip-complete. Occupied
dest with a different COUNT declines. Same host+port+collection declines.
Cross-endpoint declines. Desktop-lab Qdrant on :6333 is not a
customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, copy onto the
same collection, public proxy, leftover upsert.
This is **not** snapshot restore and **not** the qdrant-migration CLI.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_qdrant_common import (
    qdrant_collection,
    qdrant_collection_exists,
    qdrant_create_collection_from_source,
    qdrant_delete_collection,
    qdrant_endpoint_key,
    qdrant_object_id,
    qdrant_points_count,
    qdrant_proxy_fail_closed,
    qdrant_scroll_upsert,
    qdrant_type_is_copy_safe,
    skip_complete_qdrant,
)


def qdrant_qdrant_copy_enabled() -> bool:
    raw = (getenv_brand("QDRANT_QDRANT_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_qdrant_to_qdrant(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    qdrant_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Scroll source points and upsert to dest. Dest points_count is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(qdrant_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not qdrant_qdrant_copy_enabled():
        raise FastPathUnavailable("Qdrant→Qdrant COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Qdrant scroll+upsert cannot rename columns")
    for declared in qdrant_ddls:
        if declared and not qdrant_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Qdrant COPY-safe"
            )
    if qdrant_proxy_fail_closed(source_cfg) or qdrant_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Qdrant bulk copy not assumed")
    src_collection = qdrant_collection(source_table)
    dest_collection = qdrant_collection(dest_table)
    if qdrant_object_id(source_cfg, src_collection) == qdrant_object_id(
        dest_cfg, dest_collection
    ):
        raise FastPathUnavailable(
            "Qdrant COPY onto the same collection stays on the row path"
        )
    if qdrant_endpoint_key(source_cfg) != qdrant_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-endpoint Qdrant COPY stays on the row path"
        )
    if not qdrant_collection_exists(source_cfg, src_collection):
        raise FastPathUnavailable("Qdrant source collection missing")

    source_count = qdrant_points_count(source_cfg, src_collection)
    if source_count <= 0:
        raise FastPathUnavailable("Qdrant source collection empty")
    dest_existed = qdrant_collection_exists(dest_cfg, dest_collection)
    dest_count_before = (
        qdrant_points_count(dest_cfg, dest_collection) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_qdrant(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"qdrant_write": "skip", "qdrant_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Qdrant dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = not dest_existed
    if replace_destination and dest_existed:
        qdrant_delete_collection(dest_cfg, dest_collection)
        created_here = True

    try:
        if not qdrant_collection_exists(dest_cfg, dest_collection):
            qdrant_create_collection_from_source(
                dest_cfg=dest_cfg,
                source_cfg=source_cfg,
                source_collection=src_collection,
                dest_collection=dest_collection,
            )
            created_here = True
        qdrant_scroll_upsert(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            src_collection=src_collection,
            dest_collection=dest_collection,
        )
        dest_count = qdrant_points_count(dest_cfg, dest_collection)
        if dest_count != source_count:
            raise ValueError(
                "Qdrant→Qdrant COPY refused: dest points_count "
                f"{dest_count} != source points_count {source_count}"
            )
    except Exception:
        if created_here or dest_count_before == 0:
            qdrant_delete_collection(dest_cfg, dest_collection)
        raise

    q_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "qdrant_read": "scroll",
            "qdrant_write": q_write,
            "qdrant_collection": dest_collection,
            "delivery_class": "at_least_once_upsert",
            "cdc_exactly_once_claimed": False,
            "production_sku": False,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
