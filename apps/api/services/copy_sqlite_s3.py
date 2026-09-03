"""SQLite SELECT CSV → S3 PUT (cross-engine bulk).

One ``BEGIN`` on the source file streams ``SELECT`` into a CSV tempfile
(HEADER, ``\\N`` = NULL, ``""`` = empty string), then ``upload_file``.
Dest COUNT is object-store artifact COUNT of that CSV (header skipped) —
never writer PUT ack, never ListObjects length. Empty dest is PUT, **not**
``.dump`` / ``aws s3 cp``. Occupied dest whose COUNT already equals the
source COUNT is skip-complete. Occupied dest with a different COUNT
declines. Dest key must be ``.csv`` / ``.tsv``. DATE affinity is allowed
(SQLite stores DATE as TEXT; identity of that storage). BLOB declines.

Declines (row path keeps quarantine): transforms that change values,
BLOB, public proxy, occupied dest with dest COUNT ≠ source, non-CSV dest
key, ``:memory:``.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_delete_keys,
    s3_dest_count,
    s3_ensure_bucket,
    s3_ext,
    s3_iter_fetchmany,
    s3_list_keys,
    s3_write_delimited,
    skip_complete_s3,
)
from services.copy_sqlite_common import (
    sqlite_connect,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192


def sqlite_s3_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlite_s3_type_is_copy_safe(declared: str) -> bool:
    return sqlite_type_is_copy_safe(declared)


def copy_sqlite_to_s3(
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
    """SELECT CSV from SQLite into one S3 object. Dest COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_s3_copy_enabled():
        raise FastPathUnavailable("SQLite→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        dest_cfg.get("host")
        or dest_cfg.get("connection_string")
        or dest_cfg.get("endpoint_url")
        or ""
    ):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    ext = s3_ext(dest_table)
    if ext not in {"csv", "tsv"}:
        raise FastPathUnavailable("SQLite→S3 COPY writes CSV/TSV")

    sqlite_resolved_path(source_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_ref = sqlite_ident(source_table)
    src_col_sql = ", ".join(
        f"{sqlite_ident(src)} AS {sqlite_ident(tgt)}" if src != tgt else sqlite_ident(src)
        for src, tgt in pairs
    )
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}"  # nosec B608
    delim = "\t" if ext == "tsv" else ","

    source_conn = sqlite_connect(source_cfg)
    created_here = False
    tmp_path = ""
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_s3_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not S3 COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}").fetchone()[0]  # nosec B608
        )

        s3_ensure_bucket(dest_cfg)
        dest_count_before = s3_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "sqlite_read": "skip",
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

        fd, tmp_path = tempfile.mkstemp(prefix="df-sqlite-s3-", suffix=f".{ext}")
        os.close(fd)
        src_cur = source_conn.cursor()
        src_cur.execute(select_sql)
        csv_rows = s3_write_delimited(
            tmp_path,
            target_cols,
            s3_iter_fetchmany(src_cur, _FETCH_BATCH),
            delim,
        )
        if csv_rows != source_count:
            raise ValueError(
                "SQLite→S3 COPY refused: CSV rows "
                f"{csv_rows} != source snapshot {source_count}"
            )
        client = s3_client(dest_cfg)
        client.upload_file(tmp_path, s3_bucket(dest_cfg), dest_table)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "SQLite→S3 COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
        s3_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "sqlite_read": "select",
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
                logger.debug("SQLite→S3 tempfile unlink skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
