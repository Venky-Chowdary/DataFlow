"""pgvector → pgvector binary COPY (identity bulk).

Dest COUNT is ``COUNT(*)``, never ``scan_source_ids`` DISTINCT source_id,
never upsert ack, never writer ``rows_written``. Delegates to
``copy_between_postgres`` (binary COPY server-to-server). Python never
vectorizes or re-embeds. Occupied dest whose COUNT already equals the
source COUNT is skip-complete. Occupied dest with a different COUNT
declines. Same host+port+database+schema+table declines. Cross-endpoint
declines. Desktop-lab pgvector is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, unsupported source structure, occupied dest with dest
COUNT ≠ source, copy onto the same table, public proxy.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, copy_between_postgres
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pgvector_common import (
    pgvector_endpoint_key,
    pgvector_object_id,
    pgvector_proxy_fail_closed,
    pgvector_row_count,
    pgvector_schema,
    pgvector_table,
    pgvector_table_exists,
    pgvector_type_is_copy_safe,
    skip_complete_pgvector,
)


def pgvector_pgvector_copy_enabled() -> bool:
    raw = (getenv_brand("PGVECTOR_PGVECTOR_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_pgvector_to_pgvector(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    pgvector_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Binary COPY between pgvector tables. Dest COUNT(*) is the proof."""
    src_schema = source_schema or pgvector_schema(source_cfg)
    dest_schema = pgvector_schema(dest_cfg)
    if not pairs or len(pairs) != len(pgvector_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pgvector_pgvector_copy_enabled():
        raise FastPathUnavailable("pgvector→pgvector COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("pgvector binary COPY cannot rename columns")
    for declared in pgvector_ddls:
        if declared and not pgvector_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not pgvector COPY-safe"
            )
    if pgvector_proxy_fail_closed(source_cfg) or pgvector_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: pgvector bulk copy not assumed")
    src_table = pgvector_table(source_table, source_cfg)
    dest_table_name = pgvector_table(dest_table, dest_cfg)
    if pgvector_object_id(source_cfg, src_table) == pgvector_object_id(
        dest_cfg, dest_table_name
    ):
        raise FastPathUnavailable(
            "pgvector COPY onto the same table stays on the row path"
        )
    if pgvector_endpoint_key(source_cfg) != pgvector_endpoint_key(dest_cfg):
        raise FastPathUnavailable(
            "cross-endpoint pgvector COPY stays on the row path"
        )
    if not pgvector_table_exists(source_cfg, src_table):
        raise FastPathUnavailable("pgvector source table missing")

    source_count = pgvector_row_count(source_cfg, src_table)
    if source_count <= 0:
        raise FastPathUnavailable("pgvector source table empty")
    dest_existed = pgvector_table_exists(dest_cfg, dest_table_name)
    dest_count_before = (
        pgvector_row_count(dest_cfg, dest_table_name) if dest_existed else 0
    )
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_pgvector(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={"pgvector_write": "skip", "pgvector_read": "skip"},
            )
        raise FastPathUnavailable(
            "append into occupied pgvector dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    result = copy_between_postgres(
        source_cfg=source_cfg,
        source_schema=src_schema,
        source_table=src_table,
        dest_cfg=dest_cfg,
        dest_schema=dest_schema,
        dest_table=dest_table_name,
        pairs=pairs,
        replace_destination=replace_destination,
    )
    dest_count = pgvector_row_count(dest_cfg, dest_table_name)
    if dest_count != source_count:
        raise ValueError(
            "pgvector→pgvector COPY refused: dest COUNT(*) "
            f"{dest_count} != source COUNT(*) {source_count}"
        )

    p_write = "overwrite" if replace_destination and dest_occupied else "insert"
    proof = f"dest_count:{dest_count}"
    snapshot = dict(result.source_snapshot or {})
    snapshot.update({
        "pgvector_read": "binary_copy",
        "pgvector_write": p_write,
        "pgvector_table": dest_table_name,
    })
    return FastPathResult(
        rows_copied=dest_count,
        source_rows=source_count,
        source_checksum=result.source_checksum or proof,
        target_rows=dest_count,
        target_checksum=result.target_checksum or proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
        indexes_carried=list(result.indexes_carried or ()),
    )
