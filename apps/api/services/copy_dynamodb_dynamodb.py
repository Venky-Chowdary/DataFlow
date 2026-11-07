"""DynamoDB → DynamoDB Scan + BatchWriteItem (identity bulk).

Dest COUNT is ``Scan Select=COUNT``, never ``DescribeTable.ItemCount``,
never ListTables length, never write ack. Empty dest is BatchWriteItem of
raw AttributeValue maps (whole items — sparse attrs travel). Occupied dest
whose COUNT already equals the source COUNT is skip-complete. Occupied
dest with a different COUNT declines. Same endpoint+table declines.
Public proxy declines. Occupancy is counted **before** any delete.
This is **not** ``aws dynamodb export-table`` / ImportTable / PutItem
one-by-one. DynamoDB Local on :8000 is an emulator, not a customer-tenant
PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, copy onto the same table, public proxy, occupied dest
with dest COUNT ≠ source, missing source KeySchema.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_dynamodb_common import (
    dynamodb_batch_put_items,
    dynamodb_delete_table,
    dynamodb_describe_table,
    dynamodb_dest_count,
    dynamodb_ensure_table_like_source,
    dynamodb_key_names,
    dynamodb_object_id,
    dynamodb_proxy_fail_closed,
    dynamodb_scan_items,
    dynamodb_table,
    dynamodb_type_is_copy_safe,
    skip_complete_dynamodb,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry

_SCAN_FLUSH = 25


def dynamodb_dynamodb_copy_enabled() -> bool:
    raw = (getenv_brand("DYNAMODB_DYNAMODB_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_dynamodb_to_dynamodb(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    dynamodb_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Scan source items onto dest via BatchWriteItem. Dest Scan COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(dynamodb_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not dynamodb_dynamodb_copy_enabled():
        raise FastPathUnavailable("DynamoDB→DynamoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("DynamoDB identity COPY cannot rename attributes")
    if dynamodb_proxy_fail_closed(source_cfg) or dynamodb_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: DynamoDB bulk copy not assumed")
    if dynamodb_object_id(source_cfg, source_table) == dynamodb_object_id(
        dest_cfg, dest_table
    ):
        raise FastPathUnavailable("DynamoDB COPY onto the same table stays on the row path")
    for declared in dynamodb_ddls:
        if not dynamodb_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not DynamoDB COPY-safe"
            )

    src_name = dynamodb_table(source_cfg, source_table)
    dest_name = dynamodb_table(dest_cfg, dest_table)
    source_info = dynamodb_describe_table(source_cfg, src_name)
    if source_info is None:
        raise FastPathUnavailable("DynamoDB source table missing")
    key_names = dynamodb_key_names(source_info)
    if not key_names:
        raise FastPathUnavailable("DynamoDB source KeySchema missing")
    mapped = {s for s, _t in pairs}
    missing_keys = [k for k in key_names if k not in mapped]
    if missing_keys:
        raise FastPathUnavailable(
            "DynamoDB identity COPY requires mapped HASH/RANGE keys "
            f"(missing {missing_keys})"
        )

    source_count = dynamodb_dest_count(source_cfg, src_name)
    dest_info = dynamodb_describe_table(dest_cfg, dest_name)
    dest_count_before = dynamodb_dest_count(dest_cfg, dest_name) if dest_info else 0
    dest_occupied = dest_count_before > 0
    if dest_info and not (dest_occupied and replace_destination):
        dest_keys = dynamodb_key_names(dest_info)
        if dest_keys != key_names:
            raise FastPathUnavailable(
                "DynamoDB identity COPY requires matching HASH/RANGE KeySchema "
                f"(source={key_names!r} dest={dest_keys!r})"
            )
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_dynamodb(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"dynamodb_write": "skip", "dynamodb_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied DynamoDB dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    created_here = dest_info is None
    if dest_occupied and replace_destination:
        dynamodb_delete_table(dest_cfg, dest_name)
        created_here = True
    dynamodb_ensure_table_like_source(dest_cfg, dest_name, source_info)

    written = 0
    batch: list[dict[str, Any]] = []
    try:
        for item in dynamodb_scan_items(source_cfg, src_name):
            batch.append(item)
            if len(batch) >= _SCAN_FLUSH:
                written += dynamodb_batch_put_items(dest_cfg, dest_name, batch)
                batch.clear()
        if batch:
            written += dynamodb_batch_put_items(dest_cfg, dest_name, batch)
        dest_count = dynamodb_dest_count(dest_cfg, dest_name)
        if dest_count != source_count:
            raise ValueError(
                "DynamoDB→DynamoDB COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
    except Exception:
        if created_here:
            dynamodb_delete_table(dest_cfg, dest_name)
        raise

    dynamodb_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "table",
            "dynamodb_read": "scan",
            "dynamodb_write": dynamodb_write,
            "dynamodb_table": dest_name,
            "copied_items": written,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
