"""GCS → GCS server-side copy_blob / rewrite (identity bulk).

Dest COUNT is object-store artifact COUNT (GET streams / Parquet
footers), never ListObjects length, never writer rewrite ack. Empty dest
is ``copy_blob`` (rewrite). Python never GETs the payload for the copy.
Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines. Same
endpoint+bucket+object declines. Cross-endpoint declines. This is **not**
``gsutil cp`` / GET+PUT. Occupancy is counted **before** delete.

Declines (row path keeps quarantine): transforms that change values,
column renames, extension mismatch, occupied dest with dest COUNT ≠
source, cross-endpoint, copy onto the same object, public proxy.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_gcs_common import (
    gcs_bucket,
    gcs_copy_object,
    gcs_delete_keys,
    gcs_dest_count,
    gcs_ensure_bucket,
    gcs_endpoint_key,
    gcs_ext,
    gcs_list_keys,
    gcs_object_id,
    gcs_proxy_fail_closed,
    gcs_remap_keys,
    gcs_type_is_copy_safe,
    skip_complete_gcs,
)
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)


def gcs_gcs_copy_enabled() -> bool:
    raw = (getenv_brand("GCS_GCS_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_gcs_to_gcs(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    gcs_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """copy_blob source keys onto dest keys. Dest artifact COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(gcs_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not gcs_gcs_copy_enabled():
        raise FastPathUnavailable("GCS→GCS COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("GCS copy_blob cannot rename columns")
    if gcs_proxy_fail_closed(source_cfg) or gcs_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: GCS bulk copy not assumed")
    if gcs_object_id(source_cfg, source_table) == gcs_object_id(dest_cfg, dest_table):
        raise FastPathUnavailable("GCS COPY onto the same object stays on the row path")
    if gcs_endpoint_key(source_cfg) != gcs_endpoint_key(dest_cfg):
        raise FastPathUnavailable("cross-endpoint GCS COPY stays on the row path")

    src_keys = gcs_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("GCS source object missing")
    for key in src_keys:
        if not gcs_type_is_copy_safe(key):
            raise FastPathUnavailable(f"source object {key!r} is not GCS COPY-safe")
    remap = gcs_remap_keys(src_keys, source_table, dest_table)
    source_count = gcs_dest_count(source_cfg, source_table)
    gcs_ensure_bucket(dest_cfg)
    dest_count_before = gcs_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_gcs(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"gcs_write": "skip", "gcs_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied GCS dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    if replace_destination:
        existing = gcs_list_keys(dest_cfg, dest_table)
        gcs_delete_keys(dest_cfg, existing)
    src_bucket = gcs_bucket(source_cfg)
    dest_bucket = gcs_bucket(dest_cfg)
    written: list[str] = []
    try:
        for src_key, dest_key in remap:
            gcs_copy_object(
                src_cfg=source_cfg,
                src_bucket=src_bucket,
                src_key=src_key,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
            )
            written.append(dest_key)
        dest_count = gcs_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "GCS→GCS COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        gcs_delete_keys(dest_cfg, written)
        raise
    gcs_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "object",
            "gcs_read": "copy_blob",
            "gcs_write": gcs_write,
            "gcs_key": remap[0][1] if remap else dest_table,
            "gcs_ext": gcs_ext(remap[0][0]) if remap else "",
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
