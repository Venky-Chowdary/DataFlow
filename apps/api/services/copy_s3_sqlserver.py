"""S3 CSV GET → SQL Server fast_executemany (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
pyodbc ``fast_executemany`` INSERT. Dest ``COUNT(*)`` must equal that
source COUNT **before commit**. This is **not** BCP / ``BULK INSERT``
CSV (quoted empty string collapses to NULL on Linux SQL Server) and
**not** ``aws s3 cp``. Empty dest is INSERT, **not** upsert. Occupied
dest whose COUNT already equals the source COUNT is skip-complete.
Occupied dest with a different COUNT declines. JSON/JSONL/Parquet stay
on the row path (CSV is the COPY-native wire). Occupancy is counted
**before DROP**. DATE ISO text binds as SQL Server DATE when the dest
DDL is DATE; TEXT ISO stays a string.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source,
DATETIME/TIMESTAMP/BLOB/JSON dest DDL.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pg_sqlserver import _enable_fast_executemany, pg_sqlserver_copy_batch
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_iter_delimited_rows,
    s3_list_keys,
    skip_complete_s3,
)
from services.copy_sqlserver_pg import _close_ss, sqlserver_type_is_copy_safe
from services.copy_sqlserver_s3 import _s3_proxy_fail_closed, _sqlserver_proxy_fail_closed
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
from services.copy_sqlite_sqlserver import sqlite_value_to_sqlserver

logger = logging.getLogger(__name__)


def s3_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("S3_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_s3_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into SQL Server fast_executemany. Dest COUNT(*) before commit is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_sqlserver_copy_enabled():
        raise FastPathUnavailable("S3→SQL Server COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlserver_ddls:
        if not sqlserver_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQL Server COPY-safe")

    if _s3_proxy_fail_closed(source_cfg) or _sqlserver_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: SQL Server bulk copy not assumed")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→SQL Server COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = ", ".join(["%s"] * len(target_cols))
    insert_sql = (
        f"INSERT INTO {dest_ref} WITH (TABLOCK) ({col_sql}) "  # nosec B608
        f"VALUES ({placeholders})"
    )
    batch_size = pg_sqlserver_copy_batch()

    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    tmp_paths: list[str] = []
    dst_cur = dest_conn.cursor()
    _enable_fast_executemany(dst_cur)
    try:
        exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = _ss_count(dst_cur, dest_ref)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"s3_read": "skip", "sqlserver_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied SQL Server dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dst_cur.execute(_ss_drop_sql(dest_ref))
            dest_conn.commit()
            exists = False
        if not exists:
            dst_cur.execute(
                _ss_create_sql(dest_ref, dest_table, pairs, sqlserver_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        copied = 0
        batch: list[tuple[Any, ...]] = []
        identity = _has_identity(dst_cur, dst_schema, dest_table)
        if identity:
            dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
        try:
            for src_key in src_keys:
                ext = s3_ext(src_key)
                fd, tmp_path = tempfile.mkstemp(prefix="df-s3-sqlserver-", suffix=f".{ext}")
                os.close(fd)
                tmp_paths.append(tmp_path)
                client.download_file(bucket, src_key, tmp_path)
                delim = "\t" if ext == "tsv" else ","
                for cells in s3_iter_delimited_rows(tmp_path, delim):
                    if len(cells) != len(sqlserver_ddls):
                        raise ValueError(
                            f"CSV width {len(cells)} != dest columns {len(sqlserver_ddls)}"
                        )
                    batch.append(
                        tuple(
                            sqlite_value_to_sqlserver(val, ddl)
                            for val, ddl in zip(cells, sqlserver_ddls, strict=True)
                        )
                    )
                    if len(batch) >= batch_size:
                        dst_cur.executemany(insert_sql, batch)
                        copied += len(batch)
                        batch.clear()
            if batch:
                dst_cur.executemany(insert_sql, batch)
                copied += len(batch)
            dest_count = _ss_count(dst_cur, dest_ref)
            if dest_count != source_count or copied != source_count:
                dest_conn.rollback()
                raise ValueError(
                    "S3→SQL Server COPY refused: dest COUNT(*) "
                    f"{dest_count} inserted {copied} != source COUNT {source_count}"
                )
            dest_conn.commit()
        finally:
            if identity:
                try:
                    dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} OFF")  # nosec B608
                except Exception:
                    logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)

        sqlserver_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "s3_read": "get_csv",
                "sqlserver_write": sqlserver_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQL Server dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dst_cur.execute(_ss_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("SQL Server dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→SQL Server tempfile unlink skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("SQL Server dest cursor close skipped", exc_info=True)
        try:
            _close_ss(dest_conn)
        except Exception:
            logger.debug("SQL Server dest close skipped", exc_info=True)
