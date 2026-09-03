"""Redis → Redis server-side COPY (identity bulk).

Dest COUNT is prefix SCAN cardinality via ``destination_row_count``,
never ``DBSIZE`` / ``INFO keyspace``, never COPY ack. Empty dest is
Redis ``COPY`` of each ``prefix:identity`` key. Python never GET/SET/
DUMP/RESTORE the payload for the copy. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. Same host+port+db+prefix declines. Cross-endpoint
declines. Occupancy is counted **before** delete. Desktop-lab Redis on
:6379 is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, cross-endpoint,
copy onto the same prefix, public proxy, missing COPY command, non-native
Redis TYPE. TTL is carried by Redis 7 ``COPY`` when the source key has
one; the row path still does not productize TTL (see
``docs/REDIS_TTL_SEMANTICS.md``).
"""

from __future__ import annotations

from typing import Any

from connectors.redis_reader import _redis_client
from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_redis_common import (
    redis_copy_available,
    redis_copy_pairs,
    redis_db,
    redis_delete_keys,
    redis_dest_count,
    redis_endpoint_key,
    redis_key_type_is_copy_safe,
    redis_key_types,
    redis_list_keys,
    redis_object_id,
    redis_prefix,
    redis_proxy_fail_closed,
    redis_remap_keys,
    redis_type_is_copy_safe,
    skip_complete_redis,
)


def redis_redis_copy_enabled() -> bool:
    raw = (getenv_brand("REDIS_REDIS_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_redis_to_redis(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    redis_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """COPY source keys onto dest keys. Dest prefix COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(redis_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not redis_redis_copy_enabled():
        raise FastPathUnavailable("Redis→Redis COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Redis COPY cannot rename columns")
    for declared in redis_ddls:
        if declared and not redis_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Redis COPY-safe"
            )
    if redis_proxy_fail_closed(source_cfg) or redis_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Redis bulk copy not assumed")
    src_prefix = redis_prefix(source_table)
    dest_prefix = redis_prefix(dest_table)
    if redis_object_id(source_cfg, src_prefix) == redis_object_id(dest_cfg, dest_prefix):
        raise FastPathUnavailable(
            "Redis COPY onto the same prefix stays on the row path"
        )
    if redis_endpoint_key(source_cfg) != redis_endpoint_key(dest_cfg):
        raise FastPathUnavailable("cross-endpoint Redis COPY stays on the row path")

    src_keys = redis_list_keys(source_cfg, src_prefix)
    if not src_keys:
        raise FastPathUnavailable("Redis source prefix missing")
    src_client = _redis_client(source_cfg)
    try:
        if not redis_copy_available(src_client):
            raise FastPathUnavailable("Redis COPY command unavailable")
        types = redis_key_types(src_client, src_keys)
        for key, ktype in types.items():
            if not redis_key_type_is_copy_safe(ktype):
                raise FastPathUnavailable(
                    f"source key {key!r} type {ktype!r} is not Redis COPY-safe"
                )
        remap = redis_remap_keys(src_keys, src_prefix, dest_prefix)
        source_count = redis_dest_count(source_cfg, src_prefix)
        dest_count_before = redis_dest_count(dest_cfg, dest_prefix)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_redis(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"redis_write": "skip", "redis_read": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied Redis dest stays on the row path "
                "(identity COPY would duplicate)"
            )

        dest_keys = [dest_key for _src, dest_key in remap]
        if replace_destination:
            existing = redis_list_keys(dest_cfg, dest_prefix)
            redis_delete_keys(dest_cfg, existing)
        src_db = redis_db(source_cfg)
        dest_db = redis_db(dest_cfg)
        dest_db_arg = None if src_db == dest_db else dest_db
        written: list[str] = []
        try:
            redis_copy_pairs(src_client, remap, dest_db=dest_db_arg)
            written = dest_keys
            dest_count = redis_dest_count(dest_cfg, dest_prefix)
            if dest_count != source_count:
                raise ValueError(
                    "Redis→Redis COPY refused: dest COUNT "
                    f"{dest_count} != source COUNT {source_count}"
                )
        except Exception:
            redis_delete_keys(dest_cfg, written or dest_keys)
            raise
    finally:
        try:
            src_client.close()
        except Exception:
            pass

    redis_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "prefix",
            "redis_read": "copy",
            "redis_write": redis_write,
            "redis_prefix": dest_prefix,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
