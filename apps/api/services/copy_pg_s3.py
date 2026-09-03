"""PostgreSQL COPY CSV → S3 PUT (cross-engine bulk).

One PostgreSQL ``REPEATABLE READ`` snapshot streams ``COPY (SELECT …)
TO STDOUT`` CSV (HEADER) into a tempfile, then ``upload_file``. Dest
COUNT is object-store artifact COUNT of that CSV (header skipped) —
never writer PUT ack, never ListObjects length. Empty dest is PUT, **not**
upsert / ``aws s3 cp``. Occupied dest whose COUNT already equals the
source snapshot is skip-complete. Occupied dest with a different COUNT
declines. Dest key must be ``.csv`` / ``.tsv``.

Declines (row path keeps quarantine): transforms that change values,
bytea/jsonb/timestamptz, public proxy, occupied dest with dest COUNT ≠
source, non-CSV dest key.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_fast_path import _table_ref as _pg_table_ref
from services.copy_fast_path import source_column_types
from services.copy_pg_mysql import (
    _pg_connect,
    mapping_is_plain_carry,
    pg_type_is_load_safe,
)
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_delete_keys,
    s3_dest_count,
    s3_ensure_bucket,
    s3_ext,
    s3_list_keys,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def pg_s3_copy_enabled() -> bool:
    raw = (getenv_brand("PG_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pg_s3_type_is_copy_safe(declared: str) -> bool:
    return pg_type_is_load_safe(declared)


def _pg_ident(name: str) -> str:
    from services.copy_fast_path import _quote

    return _quote(name)


def copy_postgres_to_s3(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    s3_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """COPY CSV from PostgreSQL into one S3 object. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_s3_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or dest_cfg.get("endpoint_url") or ""
    ):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    ext = s3_ext(dest_table)
    if ext not in {"csv", "tsv"}:
        raise FastPathUnavailable("PostgreSQL→S3 COPY writes CSV/TSV")

    source_cols = [p[0] for p in pairs]
    src_schema = source_schema or source_cfg.get("schema") or "public"
    source_ref = _pg_table_ref(src_schema, source_table)
    delim = "E'\\t'" if ext == "tsv" else "','"

    source_conn = _pg_connect(source_cfg)
    created_here = False
    tmp_path = ""
    try:
        source_conn.autocommit = False
        src_cur = source_conn.cursor()
        src_cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        live = source_column_types(src_cur, src_schema, source_table, source_cols)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not declared:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not pg_s3_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not S3 COPY-safe"
                )
        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
        source_count = int(src_cur.fetchone()[0])
        src_cur.execute("SELECT pg_export_snapshot()")
        snapshot_id = str(src_cur.fetchone()[0])

        s3_ensure_bucket(dest_cfg)
        dest_count_before = s3_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "pg_snapshot": snapshot_id,
                        "s3_write": "skip",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied S3 dest stays on the row path "
                "(identity COPY would duplicate)"
            )

        if replace_destination:
            s3_delete_keys(dest_cfg, s3_list_keys(dest_cfg, dest_table))
        created_here = dest_count_before == 0 or replace_destination

        select_list = ", ".join(
            f"{_pg_ident(src)} AS {_pg_ident(tgt)}" if src != tgt else _pg_ident(src)
            for src, tgt in pairs
        )
        copy_sql = (
            f"COPY (SELECT {select_list} FROM {source_ref}) "  # nosec B608
            f"TO STDOUT WITH (FORMAT csv, HEADER true, DELIMITER {delim}, NULL '\\N')"
        )
        fd, tmp_path = tempfile.mkstemp(prefix="df-pg-s3-", suffix=f".{ext}")
        os.close(fd)
        with open(tmp_path, "wb") as handle:
            src_cur.copy_expert(copy_sql, handle)
        client = s3_client(dest_cfg)
        client.upload_file(tmp_path, s3_bucket(dest_cfg), dest_table)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "PostgreSQL→S3 COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("PostgreSQL source commit skipped", exc_info=True)
        s3_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "pg_snapshot": snapshot_id,
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "object",
                "s3_write": s3_write,
                "s3_key": dest_table,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                s3_delete_keys(dest_cfg, [dest_table] + s3_list_keys(dest_cfg, dest_table))
            except Exception:
                logger.debug("S3 dest delete after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("PG→S3 tempfile unlink skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("PostgreSQL source close skipped", exc_info=True)
