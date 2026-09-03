"""S3 → S3 server-side CopyObject (identity bulk).

Dest COUNT is object-store artifact COUNT (GET streams / Parquet
footers), never ListObjects length, never writer PUT ack. Empty dest is
``CopyObject`` / ``UploadPartCopy``. Python never GETs the payload for
the copy. Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines. Same
endpoint+bucket+key declines. Cross-endpoint declines. This is **not**
``aws s3 cp`` / ``aws s3 sync`` / GET+PUT.

Declines (row path keeps quarantine): transforms that change values,
column renames, extension mismatch, occupied dest with dest COUNT ≠
source, cross-endpoint, copy onto the same object.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_copy_object,
    s3_delete_keys,
    s3_dest_count,
    s3_ensure_bucket,
    s3_endpoint_key,
    s3_ext,
    s3_list_keys,
    s3_object_id,
    s3_remap_keys,
    s3_type_is_copy_safe,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def s3_s3_copy_enabled() -> bool:
    raw = (getenv_brand("S3_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_s3_to_s3(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    s3_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """CopyObject source keys onto dest keys. Dest artifact COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_s3_copy_enabled():
        raise FastPathUnavailable("S3→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("S3 CopyObject cannot rename columns")
    if s3_object_id(source_cfg, source_table) == s3_object_id(dest_cfg, dest_table):
        raise FastPathUnavailable("S3 COPY onto the same object stays on the row path")
    if s3_endpoint_key(source_cfg) != s3_endpoint_key(dest_cfg):
        raise FastPathUnavailable("cross-endpoint S3 COPY stays on the row path")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if not s3_type_is_copy_safe(key):
            raise FastPathUnavailable(f"source object {key!r} is not S3 COPY-safe")
    remap = s3_remap_keys(src_keys, source_table, dest_table)
    source_count = s3_dest_count(source_cfg, source_table)
    s3_ensure_bucket(dest_cfg)
    dest_count_before = s3_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_s3(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"s3_write": "skip", "s3_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied S3 dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    if replace_destination:
        existing = s3_list_keys(dest_cfg, dest_table)
        s3_delete_keys(dest_cfg, existing)
    src_client = s3_client(source_cfg)
    src_bucket = s3_bucket(source_cfg)
    dest_bucket = s3_bucket(dest_cfg)
    written: list[str] = []
    try:
        for src_key, dest_key in remap:
            s3_copy_object(
                src_client=src_client,
                src_bucket=src_bucket,
                src_key=src_key,
                dest_bucket=dest_bucket,
                dest_key=dest_key,
            )
            written.append(dest_key)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "S3→S3 COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        s3_delete_keys(dest_cfg, written)
        raise
    s3_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "s3_read": "copy_object",
            "s3_write": s3_write,
            "s3_key": remap[0][1] if remap else dest_table,
            "s3_ext": s3_ext(remap[0][0]) if remap else "",
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
