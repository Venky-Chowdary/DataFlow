"""S3 CSV GET → PostgreSQL COPY FROM STDIN (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
``COPY … FROM STDIN`` CSV HEADER. Dest ``COUNT(*)`` must equal that
source COUNT. Empty dest COPYs once. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. JSON/JSONL/Parquet stay on the row path (CSV is the
COPY-native wire). This is **not** ``aws s3 cp``.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_fast_path import _table_ref as _pg_table_ref
from services.copy_mysql_pg import _pg_connect, _pg_create_sql
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_list_keys,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def s3_pg_copy_enabled() -> bool:
    raw = (getenv_brand("S3_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _pg_ident(name: str) -> str:
    return _quote(name)


def copy_s3_to_postgres(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into PostgreSQL COPY FROM STDIN. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_pg_copy_enabled():
        raise FastPathUnavailable("S3→PostgreSQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→PG COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _pg_table_ref(dest_schema_n, dest_table)

    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    tmp_paths: list[str] = []
    try:
        dst_cur = dest_conn.cursor()
        dst_cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (dest_schema_n, dest_table),
        )
        exists = dst_cur.fetchone() is not None
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count and not replace_destination:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"s3_read": "skip"},
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied PostgreSQL dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
        else:
            dst_cur.execute(
                _pg_create_sql(dest_schema_n, dest_table, pairs, pg_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        col_list = ", ".join(_pg_ident(c) for c in target_cols)
        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        for src_key in src_keys:
            ext = s3_ext(src_key)
            delim = "E'\\t'" if ext == "tsv" else "','"
            fd, tmp_path = tempfile.mkstemp(prefix="df-s3-pg-", suffix=f".{ext}")
            os.close(fd)
            tmp_paths.append(tmp_path)
            client.download_file(bucket, src_key, tmp_path)
            copy_sql = (
                f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
                f"(FORMAT csv, HEADER true, DELIMITER {delim}, NULL '\\N')"
            )
            with open(tmp_path, "r", encoding="utf-8") as handle:
                dst_cur.copy_expert(copy_sql, handle)
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "S3→PG COPY refused: dest COUNT(*) "
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
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("PostgreSQL dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("PG dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→PG tempfile unlink skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("PostgreSQL dest close skipped", exc_info=True)
