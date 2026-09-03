"""Pinecone → Pinecone list + fetch + upsert (identity bulk).

Dest COUNT is ``describe_index_stats`` namespace vectorCount, never
``scan_source_ids`` DISTINCT source_id, never upsert ack, never writer
``rows_written``. Empty dest is list+fetch+upsert of raw id/values/metadata.
Python never vectorizes or re-embeds. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. Same index+namespace declines. Cross-index declines.
Pod indexes without list API decline. Desktop-lab Pinecone is not a
customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, occupied dest with dest COUNT ≠ source, copy onto the
same namespace, public proxy, leftover upsert.
This is **not** Pinecone backup/restore.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pinecone_common import (
    pinecone_delete_namespace,
    pinecone_endpoint_key,
    pinecone_list_fetch_upsert,
    pinecone_namespace,
    pinecone_namespace_exists,
    pinecone_object_id,
    pinecone_proxy_fail_closed,
    pinecone_type_is_copy_safe,
    pinecone_vector_count,
    skip_complete_pinecone,
)


def pinecone_pinecone_copy_enabled() -> bool:
    raw = (getenv_brand("PINECONE_PINECONE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_pinecone_to_pinecone(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    pinecone_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """List+fetch source vectors and upsert to dest. Dest vectorCount is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(pinecone_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pinecone_pinecone_copy_enabled():
        raise FastPathUnavailable("Pinecone→Pinecone COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Pinecone list+fetch cannot rename columns")
    for declared in pinecone_ddls:
        if declared and not pinecone_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not Pinecone COPY-safe"
            )
    if pinecone_proxy_fail_closed(source_cfg) or pinecone_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Pinecone bulk copy not assumed")
    if not pinecone_endpoint_key(source_cfg) or not pinecone_endpoint_key(dest_cfg):
        raise FastPathUnavailable("Pinecone index host required")
    src_ns = pinecone_namespace(source_table, source_cfg)
    dest_ns = pinecone_namespace(dest_table, dest_cfg)
    if pinecone_object_id(source_cfg, src_ns) == pinecone_object_id(dest_cfg, dest_ns):
        raise FastPathUnavailable(
            "Pinecone COPY onto the same namespace stays on the row path"
        )
    if pinecone_endpoint_key(source_cfg) != pinecone_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-index Pinecone COPY stays on the row path"
        )

    source_count = pinecone_vector_count(source_cfg, src_ns)
    if source_count <= 0:
        raise FastPathUnavailable("Pinecone source namespace empty")
    dest_existed = pinecone_namespace_exists(dest_cfg, dest_ns)
    dest_count_before = (
        pinecone_vector_count(dest_cfg, dest_ns) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_pinecone(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"pinecone_write": "skip", "pinecone_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied Pinecone dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    cleared_here = False
    if replace_destination and dest_occupied:
        pinecone_delete_namespace(dest_cfg, dest_ns)
        cleared_here = True

    try:
        pinecone_list_fetch_upsert(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            src_namespace=src_ns,
            dest_namespace=dest_ns,
        )
        dest_count = pinecone_vector_count(dest_cfg, dest_ns)
        if dest_count != source_count:
            raise ValueError(
                "Pinecone→Pinecone COPY refused: dest vectorCount "
                f"{dest_count} != source vectorCount {source_count}"
            )
    except Exception:
        if cleared_here or dest_count_before == 0:
            pinecone_delete_namespace(dest_cfg, dest_ns)
        raise

    p_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "shard_mode": "namespace",
            "pinecone_read": "list_fetch",
            "pinecone_write": p_write,
            "pinecone_namespace": dest_ns,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )
