"""Iceberg snapshot Parquet → SQL Server fast_executemany (cross-engine bulk).

The reverse of ``copy_sqlserver_iceberg``. Source COUNT is Iceberg file
footers via ``destination_row_count`` / ``iceberg_mor`` — never
``scan().count()``. Payload is current-snapshot data files read as
Arrow (no ``scan().to_arrow()``). Each batch is bound with pyodbc
``fast_executemany``. Dest ``COUNT(*)`` must equal that footer COUNT.

This is **not** BCP / ``BULK INSERT`` CSV (quoted empty string collapses
to NULL on Linux SQL Server). Empty dest loads the snapshot once.
Occupied dest whose ``COUNT(*)`` already equals the source footer COUNT
is skip-complete (COUNT only). Occupied dest with a different COUNT
declines. Iceberg MoR (delete files) declines. Filesystem CoW declines.

Reuses ``_arrow_from_iceberg_files`` from ``copy_iceberg_pg``.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    settle_fast_path_create_on,
)
from services.copy_iceberg_pg import (
    _ARROW_BATCH,
    _arrow_from_iceberg_files,
    _iceberg_source_count,
)
from services.copy_pg_iceberg import iceberg_copy_endpoint
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pg_sqlserver import (
    _enable_fast_executemany,
    bind_row_for_sqlserver,
    bulk_executemany,
    driver_failure_decline,
    pg_sqlserver_copy_batch,
    set_wide_decimal_inputsizes,
    sqlserver_insert_placeholders,
)
from services.copy_sqlserver_pg import _close_ss
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _create_sql as _ss_create_sql,
    _drop_sql as _ss_drop_sql,
    _has_identity,
    _ident as _ss_ident,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _table_exists as _ss_table_exists,
    _table_ref as _ss_table_ref,
)

logger = logging.getLogger(__name__)


def iceberg_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _arrow_into_sqlserver(
    dst_cur: Any,
    table: Any,
    insert_sql: str,
    batch_size: int,
) -> int:
    copied = 0
    batch: list[tuple[Any, ...]] = []
    for arrow_batch in table.to_batches(max_chunksize=_ARROW_BATCH):
        cols = [arrow_batch.column(i).to_pylist() for i in range(arrow_batch.num_columns)]
        if not cols:
            continue
        for row in zip(*cols):
            batch.append(tuple(row))
            if len(batch) >= batch_size:
                bulk_executemany(dst_cur, insert_sql, [bind_row_for_sqlserver(dst_cur, r) for r in batch])
                copied += len(batch)
                batch.clear()
    if batch:
        bulk_executemany(dst_cur, insert_sql, [bind_row_for_sqlserver(dst_cur, r) for r in batch])
        copied += len(batch)
    return copied


def copy_iceberg_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Bind Iceberg snapshot files into SQL Server. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_sqlserver_copy_enabled():
        raise FastPathUnavailable("Iceberg→SQL Server COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = sqlserver_insert_placeholders(sqlserver_ddls)
    batch_size = pg_sqlserver_copy_batch()

    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    dst_cur = dest_conn.cursor()
    try:
        source_count = _iceberg_source_count(endpoint)

        exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
        dest_occupied = False
        swap_at_end = False
        swap_schema = dst_schema
        swap_dest_ref = dest_ref
        swap_dest_table = dest_table
        if replace_destination and exists:
            # C03/X-OW2 — stage the overwrite, sp_rename swap inside the txn.
            from services.dest_overwrite_swap import (
                overwrite_trash_name,
                staged_overwrite_name,
            )

            stage_table = staged_overwrite_name(dest_table)
            swap_trash_table = overwrite_trash_name(dest_table)
            dest_ref = _ss_table_ref(swap_schema, stage_table)
            dest_table = stage_table
            swap_at_end = True
            exists = False
        if exists:
            dest_count_before = _ss_count(dst_cur, dest_ref)
            dest_occupied = dest_count_before > 0
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied SQL Server dest stays on the row path "
                    "(Iceberg source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(
                _ss_create_sql(dest_ref, dest_table, pairs, sqlserver_ddls, [])
            )
            settle_fast_path_create_on(
                dst_cur,
                dest_dialect="sqlserver",
                dest_table=dest_table,
                dest_schema=dst_schema,
            )
            dest_conn.commit()
            created_here = True

        pa_table = _arrow_from_iceberg_files(endpoint, source_cols)
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→SQL Server COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)

        # Built after the overwrite decision: a staged overwrite loads the
        # staging table, never the live destination it later replaces.
        insert_sql = (
            f"INSERT INTO {dest_ref} WITH (TABLOCK) ({col_sql}) "  # nosec B608
            f"VALUES ({placeholders})"
        )
        identity = _has_identity(dst_cur, dst_schema, dest_table)
        if identity:
            dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
        # Wide-decimal descriptors live ONLY on this write cursor — pyodbc
        # applies setinputsizes positionally to every execute, so declaring
        # them on dst_cur poisons the OBJECT_ID probe/counts (X-40: HY104).
        write_cur = dest_conn.cursor()
        _enable_fast_executemany(write_cur)
        set_wide_decimal_inputsizes(write_cur, sqlserver_ddls)
        try:
            copied = _arrow_into_sqlserver(write_cur, pa_table, insert_sql, batch_size)
            if swap_at_end:
                from services.dest_overwrite_swap import sqlserver_swap_staged_dest

                sqlserver_swap_staged_dest(
                    dst_cur,
                    schema=swap_schema,
                    dest_table=swap_dest_table,
                    staging_table=dest_table,
                    trash_table=swap_trash_table,
                    quote=_ss_ident,
                )
                dest_ref = swap_dest_ref
                dest_table = swap_dest_table
            dest_conn.commit()
        finally:
            try:
                write_cur.close()
            except Exception:
                logger.debug("SQL Server write cursor close skipped", exc_info=True)
            if identity:
                try:
                    dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} OFF")  # nosec B608
                except Exception:
                    logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)
        if copied != source_count:
            raise ValueError(
                "Iceberg→SQL Server COPY refused: bound rows "
                f"{copied} != source footer COUNT {source_count}"
            )

        dest_count = _ss_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "Iceberg→SQL Server COPY refused: dest COUNT(*) "
                f"{dest_count} != source footer COUNT {source_count}"
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
                "iceberg_read": "snapshot_parquet",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception as exc:
        if created_here:
            try:
                dst_cur.execute(_ss_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("SQL Server dest drop after copy failure skipped", exc_info=True)
        # Residual driver failure declines to the row path — typed finding,
        # never raw ODBC text in the job error (X-40).
        _decline = driver_failure_decline(exc)
        if _decline is not None:
            raise _decline from exc
        raise
    finally:
        try:
            dst_cur.close()
        except Exception:
            logger.debug("SQL Server dest cursor close skipped", exc_info=True)
        try:
            _close_ss(dest_conn)
        except Exception:
            logger.debug("SQL Server dest close skipped", exc_info=True)
