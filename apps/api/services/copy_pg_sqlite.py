"""PostgreSQL COPY text → SQLite executemany (cross-engine bulk).

One PostgreSQL ``REPEATABLE READ`` snapshot streams ``COPY (SELECT …)
TO STDOUT`` text; decoded rows bind with ``executemany``. Dest COUNT is
``SELECT COUNT(*)``. Empty dest is insert, **not** upsert. Occupied dest
whose COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines. DATE lands as SQLite TEXT
(ISO calendar day) — SQLite has no DATE affinity (engine law, not a row
drop). TIMESTAMP / TIMESTAMPTZ / BYTEA / JSONB decline.

This is **not** the sqlite3 ``.import`` CLI.

Declines (row path keeps quarantine): transforms that change values,
bytea/jsonb/timestamptz, public proxy, occupied dest with dest COUNT ≠
source, ``:memory:``.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_fast_path import _table_ref as _pg_table_ref
from services.copy_fast_path import source_column_types
from services.copy_pg_mysql import (
    _copy_select_sql,
    _pg_base,
    _pg_connect,
    _pg_copy_select_expr,
    mapping_is_plain_carry,
    pg_type_is_load_safe,
)
from services.copy_pg_sqlserver import _CopyExecutemanySink
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_bind_from_text,
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_table_exists,
)

logger = logging.getLogger(__name__)


def pg_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("PG_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pg_sqlite_copy_batch() -> int:
    raw = (getenv_brand("PG_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def pg_sqlite_type_is_copy_safe(declared: str) -> bool:
    if not pg_type_is_load_safe(declared):
        return False
    base = _pg_base(declared)
    return base not in {"TIMESTAMP", "DATETIME", "TIMESTAMPTZ", "BYTEA", "JSONB", "JSON"}


def copy_postgres_to_sqlite(
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
    """COPY text from PostgreSQL into SQLite. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_sqlite_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: SQLite bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = source_schema or source_cfg.get("schema") or "public"
    source_ref = _pg_table_ref(src_schema, source_table)
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    converters = [sqlite_bind_from_text(ddl) for ddl in sqlite_ddls]

    source_conn = _pg_connect(source_cfg)
    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
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
            if not pg_sqlite_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not SQLite COPY-safe"
                )
        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
        source_count = int(src_cur.fetchone()[0])
        src_cur.execute("SELECT pg_export_snapshot()")
        snapshot_id = str(src_cur.fetchone()[0])

        dest_conn.execute("BEGIN IMMEDIATE")
        exists = sqlite_table_exists(dest_conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                dest_conn.execute(
                    f"SELECT COUNT(*) FROM {dest_ref}"  # nosec B608
                ).fetchone()[0]
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                dest_conn.rollback()
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "pg_snapshot": snapshot_id,
                        "sqlite_write": "skip",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE {dest_ref}")  # nosec B608
            exists = False
        if not exists:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        select_list = ", ".join(
            _pg_copy_select_expr(col, live_l[col.lower()]) for col in source_cols
        )
        copy_sql = _copy_select_sql(select_list, source_ref, "")
        dst_cur = dest_conn.cursor()
        sink = _CopyExecutemanySink(
            dst_cur,
            insert_sql,
            converters,
            pg_sqlite_copy_batch(),
            len(converters),
        )
        try:
            src_cur.copy_expert(copy_sql, sink)
        finally:
            sink.close()
        if sink.rows != source_count:
            raise ValueError(
                "PostgreSQL→SQLite COPY refused: inserted "
                f"{sink.rows} != source snapshot {source_count}"
            )
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count:
            raise ValueError(
                "PostgreSQL→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("PostgreSQL source commit skipped", exc_info=True)
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "shard_mode": "table",
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
        try:
            source_conn.close()
        except Exception:
            logger.debug("PostgreSQL source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
