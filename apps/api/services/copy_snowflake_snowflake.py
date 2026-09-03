"""Snowflake → Snowflake identity bulk (same-account INSERT SELECT / CTAS).

Mapped columns only: ``CREATE TABLE dest AS SELECT mapped_cols FROM src``
when dest is missing; ``INSERT INTO dest (cols) SELECT cols FROM src``
when dest exists empty. Python never formats a row. Dest COUNT is
``destination_row_count`` → ``SELECT COUNT(*)``. Occupied dest whose
COUNT already equals the source COUNT is skip-complete. Occupied dest
with a different COUNT declines. Overwrite counts occupancy **before**
``DROP TABLE``, then CTAS. This is **not** ``COPY INTO`` from a stage,
**not** leftover MERGE, **not** ``CLONE`` (CLONE would copy unmapped
columns). fakesnow is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, GEOGRAPHY/GEOMETRY/VECTOR, public proxy, cross-account,
copy onto the same table, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_snowflake_common import (
    require_safe_table,
    skip_complete_snowflake,
    snowflake_connect,
    snowflake_dest_count,
    snowflake_execute,
    snowflake_ident,
    snowflake_proxy_fail_closed,
    snowflake_same_account,
    snowflake_same_table,
    snowflake_schema_of,
    snowflake_table_ref,
    snowflake_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def snowflake_snowflake_copy_enabled() -> bool:
    raw = (getenv_brand("SNOWFLAKE_SNOWFLAKE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_snowflake_to_snowflake(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    snowflake_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """CTAS / INSERT SELECT of mapped columns. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(snowflake_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not snowflake_snowflake_copy_enabled():
        raise FastPathUnavailable("Snowflake→Snowflake COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("Snowflake INSERT SELECT cannot rename columns")
    if snowflake_proxy_fail_closed(source_cfg) or snowflake_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Snowflake bulk copy not assumed")
    if snowflake_same_table(source_cfg, dest_cfg, source_table, dest_table):
        raise FastPathUnavailable(
            "Snowflake COPY onto the same table stays on the row path"
        )
    if not snowflake_same_account(source_cfg, dest_cfg):
        raise FastPathUnavailable("cross-account Snowflake COPY stays on the row path")
    for col, declared in zip((p[0] for p in pairs), snowflake_ddls):
        if not snowflake_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"source column {col!r} type {declared} is not Snowflake COPY-safe"
            )

    source_count = snowflake_dest_count(source_cfg, source_table)
    dest_count_before = snowflake_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_snowflake(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={
                    "snowflake_write": "skip",
                    "snowflake_read": "skip",
                },
            )
        raise FastPathUnavailable(
            "append into occupied Snowflake dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    src_schema = snowflake_schema_of(source_cfg)
    dest_schema = snowflake_schema_of(dest_cfg)
    source_cols = [p[0] for p in pairs]
    dest_cols = [p[1] for p in pairs]
    col_sql = ", ".join(snowflake_ident(c) for c in source_cols)
    dest_col_sql = ", ".join(snowflake_ident(c) for c in dest_cols)
    dest_ref = snowflake_table_ref(dest_schema, dest_table)
    created_here = False
    conn = snowflake_connect(dest_cfg)
    try:
        from connectors.snowflake_conn import (
            resolve_snowflake_table_name,
            snowflake_qualified_table,
        )

        with conn.cursor() as cur:
            src_resolved = resolve_snowflake_table_name(cur, src_schema, source_table)
            if src_resolved is None:
                raise FastPathUnavailable(f"Snowflake source table {source_table!r} missing")
            src_ref = snowflake_qualified_table(src_schema, src_resolved)
            dest_resolved = resolve_snowflake_table_name(cur, dest_schema, dest_table)
            exists = dest_resolved is not None
            if replace_destination and exists:
                drop_ref = snowflake_qualified_table(
                    dest_schema, dest_resolved or require_safe_table(dest_table)
                )
                snowflake_execute(cur, f"DROP TABLE IF EXISTS {drop_ref}")  # nosec B608
                exists = False
            if not exists:
                created_here = True
                snowflake_execute(
                    cur,
                    f"CREATE TABLE {dest_ref} AS SELECT {col_sql} FROM {src_ref}",  # nosec B608
                )
            else:
                insert_ref = snowflake_qualified_table(dest_schema, dest_resolved)
                snowflake_execute(
                    cur,
                    (
                        f"INSERT INTO {insert_ref} ({dest_col_sql}) "  # nosec B608
                        f"SELECT {col_sql} FROM {src_ref}"
                    ),
                )
        try:
            conn.commit()
        except Exception:
            logger.debug("Snowflake commit skipped", exc_info=True)

        dest_count = snowflake_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "Snowflake→Snowflake COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        snowflake_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "snowflake_read": "insert_select",
                "snowflake_write": snowflake_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                with conn.cursor() as cur:
                    snowflake_execute(cur, f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                try:
                    conn.commit()
                except Exception:
                    logger.debug("Snowflake fail-closed drop commit skipped", exc_info=True)
            except Exception:
                logger.debug("Snowflake dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("Snowflake dest close skipped", exc_info=True)
