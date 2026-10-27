"""MongoDB snapshot find → SQL Server fast_executemany (cross-engine bulk).

The reverse of ``copy_sqlserver_mongo``. Source COUNT is
``count_documents({})`` inside a replica-set snapshot transaction —
never ``estimatedDocumentCount``. Payload is ``find()`` in that same
snapshot. Batches are bound with pyodbc ``fast_executemany``. Dest
``COUNT(*)`` must equal that snapshot COUNT.

This is **not** BCP / ``BULK INSERT`` CSV (quoted empty string collapses
to NULL on Linux SQL Server) and **not** ``mongoexport``. Empty dest
loads the snapshot once. Occupied dest whose ``COUNT(*)`` already equals
the source snapshot COUNT is skip-complete. Occupied dest with a
different COUNT declines. Standalone Mongo declines.

Nested documents / arrays / binary decline. ``_id`` is omitted unless
mapped. BSON Date stored as UTC midnight (from SQL Server DATE)
round-trips as DATE.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, snapshot read concern unavailable.
"""

from __future__ import annotations

import logging
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    settle_fast_path_create_on,
)
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session
from services.copy_mongo_sink import bson_to_python
from services.copy_pg_mongo import mongo_collection
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


def mongo_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_mongo_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Bind Mongo snapshot documents into SQL Server. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_sqlserver_copy_enabled():
        raise FastPathUnavailable("MongoDB→SQL Server COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or "") or is_public_proxy_host(
        source_cfg.get("host") or source_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = sqlserver_insert_placeholders(sqlserver_ddls)
    batch_size = pg_sqlserver_copy_batch()

    client, coll = mongo_collection(source_cfg, source_table)
    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    session = None
    dst_cur = dest_conn.cursor()
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
        dest_occupied = False
        swap_at_end = False
        swap_schema = dst_schema
        swap_dest_ref = dest_ref
        swap_dest_table = dest_table
        if replace_destination and exists:
            # C03/X-OW2 — stage the overwrite, sp_rename swap inside the run
            # transaction: the old dest stays visible until COMMIT.
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
                    "(Mongo source has no PK-range skip on this path)"
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

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        copied = 0
        batch: list[tuple[Any, ...]] = []
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
            while True:
                docs = list(islice(cursor, _FIND_BATCH))
                if not docs:
                    break
                for doc in docs:
                    batch.append(
                        tuple(
                            bson_to_python(doc.get(col), ddl)
                            for col, ddl in zip(source_cols, sqlserver_ddls, strict=True)
                        )
                    )
                    if len(batch) >= batch_size:
                        bulk_executemany(write_cur, insert_sql, [bind_row_for_sqlserver(write_cur, r) for r in batch])
                        copied += len(batch)
                        batch.clear()
            if batch:
                bulk_executemany(write_cur, insert_sql, [bind_row_for_sqlserver(write_cur, r) for r in batch])
                copied += len(batch)
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
                "Mongo→SQL Server COPY refused: bound rows "
                f"{copied} != source snapshot {source_count}"
            )
        dest_count = _ss_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "Mongo→SQL Server COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
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
                "mongo_read": "snapshot_find",
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
        if session is not None:
            try:
                session.abort_transaction()
            except Exception:
                logger.debug("Mongo snapshot abort skipped", exc_info=True)
            try:
                session.end_session()
            except Exception:
                logger.debug("Mongo snapshot session close skipped", exc_info=True)
        _close_ss(dest_conn)
