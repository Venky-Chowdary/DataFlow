"""BigQuery → BigQuery identity bulk (same-project INSERT SELECT / CTAS).

Mapped columns only: ``CREATE TABLE dest AS SELECT mapped_cols FROM src``
when dest is missing; ``INSERT INTO dest (cols) SELECT cols FROM src``
when dest exists empty. Python never formats a row. Dest COUNT is
``destination_row_count`` → ``SELECT COUNT(*)``, never ``Table.num_rows``.
Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines. Overwrite
counts occupancy **before** ``DROP TABLE``, then CTAS. This is **not**
``insert_rows_json``, **not** leftover MERGE, **not** ``CLONE``.
goccy/bigquery-emulator is not a customer-tenant PRODUCTION_SKU.

Declines (row path keeps quarantine): transforms that change values,
column renames, GEOGRAPHY/STRUCT/INTERVAL, public proxy, cross-project,
copy onto the same table, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_bigquery_common import (
    bigquery_connect,
    bigquery_dataset_of,
    bigquery_dest_count,
    bigquery_ident,
    bigquery_project_of,
    bigquery_proxy_fail_closed,
    bigquery_run_sql,
    bigquery_same_project,
    bigquery_same_table,
    bigquery_table_exists,
    bigquery_table_ref,
    bigquery_type_is_copy_safe,
    skip_complete_bigquery,
)

logger = logging.getLogger(__name__)


def bigquery_bigquery_copy_enabled() -> bool:
    raw = (getenv_brand("BIGQUERY_BIGQUERY_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_bigquery_to_bigquery(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    bigquery_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """CTAS / INSERT SELECT of mapped columns. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(bigquery_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not bigquery_bigquery_copy_enabled():
        raise FastPathUnavailable("BigQuery→BigQuery COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("BigQuery INSERT SELECT cannot rename columns")
    if bigquery_proxy_fail_closed(source_cfg) or bigquery_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: BigQuery bulk copy not assumed")
    if bigquery_same_table(source_cfg, dest_cfg, source_table, dest_table):
        raise FastPathUnavailable(
            "BigQuery COPY onto the same table stays on the row path"
        )
    if not bigquery_same_project(source_cfg, dest_cfg):
        raise FastPathUnavailable("cross-project BigQuery COPY stays on the row path")
    for col, declared in zip((p[0] for p in pairs), bigquery_ddls):
        if not bigquery_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"source column {col!r} type {declared} is not BigQuery COPY-safe"
            )

    source_count = bigquery_dest_count(source_cfg, source_table)
    dest_count_before = bigquery_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_bigquery(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot={
                    "bigquery_write": "skip",
                    "bigquery_read": "skip",
                },
            )
        raise FastPathUnavailable(
            "append into occupied BigQuery dest stays on the row path "
            "(identity COPY would duplicate)"
        )

    src_project = bigquery_project_of(source_cfg)
    dest_project = bigquery_project_of(dest_cfg)
    src_dataset = bigquery_dataset_of(source_cfg)
    dest_dataset = bigquery_dataset_of(dest_cfg)
    src_ref = bigquery_table_ref(src_project, src_dataset, source_table)
    dest_ref = bigquery_table_ref(dest_project, dest_dataset, dest_table)
    col_sql = ", ".join(bigquery_ident(c) for c, _t in pairs)
    dest_col_sql = ", ".join(bigquery_ident(t) for _s, t in pairs)
    created_here = False
    client = bigquery_connect(dest_cfg)
    try:
        exists = bigquery_table_exists(client, dest_project, dest_dataset, dest_table)
        if replace_destination and exists:
            bigquery_run_sql(client, f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if not exists:
            created_here = True
            bigquery_run_sql(
                client,
                f"CREATE TABLE {dest_ref} AS SELECT {col_sql} FROM {src_ref}",  # nosec B608
            )
        else:
            bigquery_run_sql(
                client,
                (
                    f"INSERT INTO {dest_ref} ({dest_col_sql}) "  # nosec B608
                    f"SELECT {col_sql} FROM {src_ref}"
                ),
            )

        dest_count = bigquery_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "BigQuery→BigQuery COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        bigquery_write = (
            "overwrite" if replace_destination and dest_occupied else "insert"
        )
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
                "bigquery_read": "insert_select",
                "bigquery_write": bigquery_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                bigquery_run_sql(client, f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            except Exception:
                logger.debug("BigQuery dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            client.close()
        except Exception:
            logger.debug("BigQuery dest close skipped", exc_info=True)
