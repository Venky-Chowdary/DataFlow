"""S3 CSV GET → MySQL STRICT LOAD DATA (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
STRICT ``LOAD DATA LOCAL INFILE`` (HEADER skipped). Dest ``COUNT(*)``
must equal that source COUNT **before commit**. Empty dest loads once.
Occupied dest whose COUNT already equals the source COUNT is
skip-complete. Occupied dest with a different COUNT declines.
JSON/JSONL/Parquet stay on the row path (CSV is the COPY-native wire).
This is **not** ``aws s3 cp``.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source,
LOAD DATA ineligible sessions.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_pg import _mysql_connect, _mysql_ident
from services.copy_pg_mysql import _mysql_create_sql, mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_list_keys,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def s3_mysql_copy_enabled() -> bool:
    raw = (getenv_brand("S3_MYSQL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _mysql_table_exists(cur: Any, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return int(cur.fetchone()[0]) > 0


def _load_delimited_into_mysql(
    dest_conn: Any,
    dst_cur: Any,
    *,
    path: str,
    table_q: str,
    columns: list[str],
    ext: str,
) -> None:
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        mysql_load_data_session_ready,
        quote_load_data_path,
    )

    ready, why = mysql_load_data_session_ready(dst_cur, dest_conn)
    if not ready:
        raise FastPathUnavailable(why)
    csv_mode = ext != "tsv"
    load_sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
        field_terminator="," if csv_mode else "\t",
        optionally_enclosed=csv_mode,
        ignore_lines=1,
    )
    dst_cur.execute(load_sql)
    dst_cur.execute("SHOW WARNINGS")
    blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
    if blocked:
        raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")


def copy_s3_to_mysql(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into MySQL LOAD DATA. Dest COUNT(*) before commit is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_mysql_copy_enabled():
        raise FastPathUnavailable("S3→MySQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: LOAD DATA not assumed")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→MySQL COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    dest_q = _mysql_ident(dest_table)

    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    tmp_paths: list[str] = []
    dst_cur = dest_conn.cursor()
    try:
        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count and not replace_destination:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"s3_read": "skip", "load_data": "skip"},
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied MySQL dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
        else:
            dst_cur.execute(_mysql_create_sql(dest_table, pairs, mysql_ddls, []))
            dest_conn.commit()
            created_here = True

        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        for src_key in src_keys:
            ext = s3_ext(src_key)
            fd, tmp_path = tempfile.mkstemp(prefix="df-s3-mysql-", suffix=f".{ext}")
            os.close(fd)
            tmp_paths.append(tmp_path)
            client.download_file(bucket, src_key, tmp_path)
            _load_delimited_into_mysql(
                dest_conn,
                dst_cur,
                path=tmp_path,
                table_q=dest_q,
                columns=target_cols,
                ext=ext,
            )
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "S3→MySQL COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        dest_conn.commit()
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
                "load_data": "tempfile",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("MySQL dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("MySQL dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→MySQL tempfile unlink skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("MySQL dest cursor close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("MySQL dest close skipped", exc_info=True)
