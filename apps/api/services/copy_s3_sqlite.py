"""S3 CSV GET → SQLite executemany (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
``executemany`` INSERT. Dest ``COUNT(*)`` must equal that source COUNT
**before commit**. Empty dest loads once. Occupied dest whose COUNT
already equals the source COUNT is skip-complete. Occupied dest with a
different COUNT declines. JSON/JSONL/Parquet stay on the row path (CSV
is the COPY-native wire). ``:memory:`` declines. BLOB dest DDL declines.
This is **not** sqlite3 ``.import`` / ``aws s3 cp``.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source,
BLOB, ``:memory:``.
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
    s3_dest_count,
    s3_ext,
    s3_iter_delimited_rows,
    s3_list_keys,
    skip_complete_s3,
)
from services.copy_sqlite_common import (
    sqlite_bind_from_text,
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_table_exists,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def s3_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("S3_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def s3_sqlite_copy_batch() -> int:
    raw = (getenv_brand("S3_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def copy_s3_to_sqlite(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into SQLite executemany. Dest COUNT(*) before commit is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_sqlite_copy_enabled():
        raise FastPathUnavailable("S3→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        source_cfg.get("host")
        or source_cfg.get("connection_string")
        or source_cfg.get("endpoint_url")
        or ""
    ):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    sqlite_resolved_path(dest_cfg)
    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→SQLite COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    converters = [sqlite_bind_from_text(ddl) for ddl in sqlite_ddls]
    batch_size = s3_sqlite_copy_batch()

    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    tmp_paths: list[str] = []
    try:
        dest_conn.execute("BEGIN IMMEDIATE")
        exists = sqlite_table_exists(dest_conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                dest_conn.rollback()
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"s3_read": "skip", "sqlite_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if exists:
            live = sqlite_pragma_types(dest_conn, dest_table)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in target_cols:
                declared = live_l.get(col.lower())
                if declared is None:
                    raise FastPathUnavailable(f"dest column {col!r} absent")
                if not sqlite_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"dest column {col!r} type {declared} is not SQLite COPY-safe"
                    )
        else:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        pending: list[tuple[Any, ...]] = []
        inserted = 0
        for src_key in src_keys:
            ext = s3_ext(src_key)
            fd, tmp_path = tempfile.mkstemp(prefix="df-s3-sqlite-", suffix=f".{ext}")
            os.close(fd)
            tmp_paths.append(tmp_path)
            client.download_file(bucket, src_key, tmp_path)
            delim = "\t" if ext == "tsv" else ","
            for cells in s3_iter_delimited_rows(tmp_path, delim):
                if len(cells) != len(converters):
                    raise ValueError(
                        f"CSV width {len(cells)} != dest columns {len(converters)}"
                    )
                pending.append(
                    tuple(conv(cell) for conv, cell in zip(converters, cells, strict=True))
                )
                if len(pending) >= batch_size:
                    dest_conn.executemany(insert_sql, pending)
                    inserted += len(pending)
                    pending.clear()
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "S3→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source COUNT {source_count}"
            )
        dest_conn.commit()
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "sqlite_write": sqlite_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("SQLite dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→SQLite tempfile unlink skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
