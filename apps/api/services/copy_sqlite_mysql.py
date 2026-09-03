"""SQLite SELECT → MySQL STRICT LOAD DATA (cross-engine bulk).

One ``BEGIN`` on the source file streams ``SELECT``; each cell is encoded
as LOAD DATA TSV into a tempfile, then STRICT ``LOAD DATA LOCAL INFILE``.
Dest ``COUNT(*)`` must equal the source COUNT **before commit**. Empty
dest is LOAD DATA, **not** upsert / ``.dump`` / sqlldr. Occupied dest
whose COUNT already equals the source COUNT is skip-complete. Occupied
dest with a different COUNT declines. ``:memory:`` / BLOB decline.
DATE ISO text or a calendar day loads as MySQL DATE. DATETIME /
TIMESTAMP decline (would invent a MySQL datetime). JSON declines.

Declines (row path keeps quarantine): transforms that change values,
BLOB/DATETIME/JSON, public proxy, occupied dest with dest COUNT ≠ source,
``:memory:``, LOAD DATA ineligible sessions.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_mysql import fast_load_data_text_value
from services.copy_mysql_pg import _mysql_connect, _mysql_ident
from services.copy_pg_mysql import _mysql_create_sql, mapping_is_plain_carry
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192
_UNSAFE_SQLITE_MYSQL_BASES = frozenset({
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "JSON",
    "JSONB",
})


def sqlite_mysql_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_MYSQL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlite_mysql_type_is_copy_safe(declared: str) -> bool:
    if not sqlite_type_is_copy_safe(declared):
        return False
    base = (declared or "").strip().upper().replace(" ", "").split("(", 1)[0]
    return base not in _UNSAFE_SQLITE_MYSQL_BASES


def sqlite_value_to_load_data(value: Any, ddl: str) -> str:
    """SQLite cell → LOAD DATA TSV. DATE ISO is a calendar day; DATETIME declines."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("BLOB values are not MySQL COPY-safe")
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in _UNSAFE_SQLITE_MYSQL_BASES:
        raise FastPathUnavailable(
            f"{base} SQLite value is not MySQL COPY-safe"
        )
    if base == "DATE":
        if value is None:
            return "\\N"
        if isinstance(value, str):
            try:
                value = date.fromisoformat(value[:10])
            except ValueError as exc:
                raise FastPathUnavailable(
                    f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
                ) from exc
    return fast_load_data_text_value(value)


def _mysql_table_exists(cur: Any, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return int(cur.fetchone()[0]) > 0


def _load_tsv_into_mysql(
    dest_conn: Any,
    dst_cur: Any,
    *,
    path: str,
    table_q: str,
    columns: list[str],
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
    load_sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
    )
    dst_cur.execute(load_sql)
    dst_cur.execute("SHOW WARNINGS")
    blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
    if blocked:
        raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")


def copy_sqlite_to_mysql(
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
    """SELECT SQLite into MySQL LOAD DATA. Dest COUNT(*) before commit is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_mysql_copy_enabled():
        raise FastPathUnavailable("SQLite→MySQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: LOAD DATA not assumed")

    sqlite_resolved_path(source_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_ref = sqlite_ident(source_table)
    dest_q = _mysql_ident(dest_table)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}"  # nosec B608

    source_conn = sqlite_connect(source_cfg)
    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    tmp_path = ""
    dst_cur = dest_conn.cursor()
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col, ddl in zip(source_cols, mysql_ddls, strict=True):
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_mysql_type_is_copy_safe(declared) or (
                ddl and not sqlite_mysql_type_is_copy_safe(ddl)
            ):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not MySQL COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}").fetchone()[0]  # nosec B608
        )

        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_count_before = 0
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"sqlite_read": "skip", "load_data": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied MySQL dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if not exists:
            dst_cur.execute(_mysql_create_sql(dest_table, pairs, mysql_ddls, []))
            dest_conn.commit()
            created_here = True

        handle, tmp_path = tempfile.mkstemp(prefix="df_sqlite_mysql_", suffix=".tsv")
        os.close(handle)
        written = 0
        src_cur = source_conn.cursor()
        src_cur.execute(select_sql)
        join = "\t".join
        with open(tmp_path, "wb", buffering=1 << 20) as writer:
            while True:
                rows = src_cur.fetchmany(_FETCH_BATCH)
                if not rows:
                    break
                lines = [
                    join(
                        sqlite_value_to_load_data(val, ddl)
                        for val, ddl in zip(row, mysql_ddls, strict=True)
                    )
                    for row in rows
                ]
                written += len(lines)
                if lines:
                    writer.write(("\n".join(lines) + "\n").encode("utf-8"))
        if written != source_count:
            raise ValueError(
                "SQLite→MySQL COPY refused: TSV rows "
                f"{written} != source COUNT {source_count}"
            )
        _load_tsv_into_mysql(
            dest_conn,
            dst_cur,
            path=tmp_path,
            table_q=dest_q,
            columns=target_cols,
        )
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "SQLite→MySQL COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        mysql_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "sqlite_read": "select",
                "load_data": "tempfile",
                "mysql_write": mysql_write,
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
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("SQLite→MySQL TSV unlink skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("MySQL dest cursor close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("MySQL dest close skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
