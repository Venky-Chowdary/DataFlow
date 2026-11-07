"""ADLS → ADLS server-side start_copy_from_url (identity bulk).

Dest COUNT is object-store artifact COUNT (GET streams / Parquet
footers), never ListBlobs length, never copy ack. Empty dest is
``start_copy_from_url`` (requires_sync). Python never GETs the payload
for the copy. Occupied dest whose COUNT already equals the source COUNT
is skip-complete. Occupied dest with a different COUNT declines. Same
endpoint+container+blob declines. Cross-endpoint declines. This is
**not** ``azcopy`` / GET+PUT. Occupancy is counted **before** delete.
Azurite on :10000 is an emulator, not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, extension mismatch, occupied dest with dest COUNT ≠
source, cross-endpoint, copy onto the same object, public proxy.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_adls_common import (
    adls_container,
    adls_copy_object,
    adls_delete_keys,
    adls_dest_count,
    adls_ensure_container,
    adls_endpoint_key,
    adls_ext,
    adls_list_keys,
    adls_object_id,
    adls_proxy_fail_closed,
    adls_remap_keys,
    adls_type_is_copy_safe,
    skip_complete_adls,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry


def adls_adls_copy_enabled() -> bool:
    raw = (getenv_brand("ADLS_ADLS_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_adls_to_adls(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    adls_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """start_copy_from_url source keys onto dest keys. Dest artifact COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(adls_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not adls_adls_copy_enabled():
        raise FastPathUnavailable("ADLS→ADLS COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("ADLS start_copy_from_url cannot rename columns")
    if adls_proxy_fail_closed(source_cfg) or adls_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: ADLS bulk copy not assumed")
    if adls_object_id(source_cfg, source_table) == adls_object_id(dest_cfg, dest_table):
        raise FastPathUnavailable("ADLS COPY onto the same object stays on the row path")
    if adls_endpoint_key(source_cfg) != adls_endpoint_key(dest_cfg):
        raise FastPathUnavailable("cross-endpoint ADLS COPY stays on the row path")
    if not adls_type_is_copy_safe(source_table):
        raise FastPathUnavailable(
            f"source object {source_table!r} is not ADLS COPY-safe"
        )
    if not adls_type_is_copy_safe(dest_table):
        raise FastPathUnavailable(
            f"dest object {dest_table!r} is not ADLS COPY-safe"
        )

    src_keys = adls_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("ADLS source object missing")
    for key in src_keys:
        if not adls_type_is_copy_safe(key):
            raise FastPathUnavailable(f"source object {key!r} is not ADLS COPY-safe")
    remap = adls_remap_keys(src_keys, source_table, dest_table)
    source_count = adls_dest_count(source_cfg, source_table)
    adls_ensure_container(dest_cfg)
    dest_count_before = adls_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_adls(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"adls_write": "skip", "adls_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied ADLS dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    if replace_destination:
        existing = adls_list_keys(dest_cfg, dest_table)
        adls_delete_keys(dest_cfg, existing)
    src_container = adls_container(source_cfg)
    dest_container = adls_container(dest_cfg)
    written: list[str] = []
    try:
        for src_key, dest_key in remap:
            adls_copy_object(
                src_cfg=source_cfg,
                src_container=src_container,
                src_key=src_key,
                dest_container=dest_container,
                dest_key=dest_key,
            )
            written.append(dest_key)
        dest_count = adls_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "ADLS→ADLS COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        adls_delete_keys(dest_cfg, written)
        raise
    adls_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "adls_read": "start_copy_from_url",
            "adls_write": adls_write,
            "adls_key": remap[0][1] if remap else dest_table,
            "adls_ext": adls_ext(remap[0][0]) if remap else "",
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
