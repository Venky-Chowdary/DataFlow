"""SQLite SELECT → PostgreSQL COPY FROM STDIN (cross-engine bulk).

SQLite has no server COPY. One ``BEGIN`` on the source file streams
``SELECT``; each cell is encoded as PostgreSQL COPY text into
``COPY … FROM STDIN``. Dest ``COUNT(*)`` must equal the source COUNT.
Empty dest COPYs once. Occupied dest whose COUNT already equals the
source COUNT is skip-complete. Occupied dest with a different COUNT
declines. DATE ISO calendar-day lands as PostgreSQL DATE. Naive ISO
DATETIME lands as ``TIMESTAMP`` (not ``TIMESTAMPTZ``). BOOLEAN 0/1
lands as PostgreSQL BOOLEAN. INTEGER unix, REAL julian, tz-aware, and
date-only DATETIME decline (would invent a clock). BLOB / JSON /
TIMESTAMPTZ decline. This is **not** ``.dump``.

``source_where`` is a pre-quoted SQL fragment (incremental cursor predicate).
When set, COUNT and SELECT use that filter and dest-occupied skip is disabled.

Declines (row path keeps quarantine): transforms that change values,
BLOB/unix DATETIME/boolean synonyms, public proxy, occupied dest with dest
COUNT ≠ source, ``:memory:``.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_fast_path import _table_ref as _pg_table_ref
from services.copy_mysql_pg import _pg_connect, _pg_create_sql, fast_copy_text_value
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_copy_bool_value,
    sqlite_copy_date_value,
    sqlite_copy_naive_datetime_value,
    sqlite_ddl_base,
    sqlite_ident,
    sqlite_pg_type_is_copy_safe,
    sqlite_pragma_types,
    sqlite_resolved_path,
)

logger = logging.getLogger(__name__)

_READ_CHUNK = 1 << 20
_FETCH_BATCH = 8192


def sqlite_pg_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _pg_ident(name: str) -> str:
    return _quote(name)


def _pg_ddl_is_timestamptz(ddl: str) -> bool:
    upper = (ddl or "").strip().upper()
    compact = upper.replace(" ", "")
    if compact.startswith("TIMESTAMPTZ") or compact.startswith("TIMETZ"):
        return True
    return "WITH TIME ZONE" in upper and "WITHOUT TIME ZONE" not in upper


def sqlite_value_to_pg_copy(value: Any, ddl: str) -> str:
    """SQLite cell → PostgreSQL COPY text. DATE/naive DATETIME are proven first."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("BLOB values are not PostgreSQL COPY-safe")
    if _pg_ddl_is_timestamptz(ddl):
        raise FastPathUnavailable(f"{ddl} is not PostgreSQL COPY-safe")
    base = sqlite_ddl_base(ddl)
    if base == "DATE":
        parsed = sqlite_copy_date_value(value)
        return "\\N" if parsed is None else parsed.isoformat()
    if base in {"BOOLEAN", "BOOL"}:
        parsed = sqlite_copy_bool_value(value)
        return "\\N" if parsed is None else ("1" if parsed else "0")
    if base in {"DATETIME", "TIMESTAMP"} or base.startswith("TIMESTAMP"):
        parsed = sqlite_copy_naive_datetime_value(value)
        return "\\N" if parsed is None else str(parsed)
    return fast_copy_text_value(value)


class _SqliteCopyReader:
    """File-like: encode SQLite fetch batches as COPY text on read()."""

    def __init__(self, cursor: Any, select_sql: str, ddls: list[str]) -> None:
        self._cursor = cursor
        self._select_sql = select_sql
        self._ddls = ddls
        self._buf = b""
        self._started = False
        self._done = False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        want = _READ_CHUNK if size is None or size < 0 else max(int(size), 1)
        if not self._started:
            self._cursor.execute(self._select_sql)
            self._started = True
        join = "\t".join
        ddls = self._ddls
        while not self._done and len(self._buf) < want:
            batch = self._cursor.fetchmany(_FETCH_BATCH)
            if not batch:
                self._done = True
                break
            payload = "\n".join(
                join(
                    sqlite_value_to_pg_copy(v, ddl)
                    for v, ddl in zip(row, ddls, strict=True)
                )
                for row in batch
            )
            if payload:
                self._buf += (payload + "\n").encode("utf-8")
        out = self._buf[:want]
        self._buf = self._buf[want:]
        return out


def copy_sqlite_to_postgres(
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
    source_where: str = "",
) -> FastPathResult:
    """SELECT SQLite into PostgreSQL COPY FROM STDIN. Dest COUNT(*) is the proof.

    ``source_where`` is a pre-quoted SQL fragment (incremental cursor predicate).
    When set, COUNT and SELECT use that filter and dest-occupied skip is disabled.
    """
    del source_schema
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_pg_copy_enabled():
        raise FastPathUnavailable("SQLite→PostgreSQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")

    sqlite_resolved_path(source_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _pg_table_ref(dest_schema_n, dest_table)
    src_ref = sqlite_ident(source_table)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    cursor_where = (source_where or "").strip()
    where_sql = f" WHERE {cursor_where}" if cursor_where else ""
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}{where_sql}"  # nosec B608

    source_conn = sqlite_connect(source_cfg)
    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col, ddl in zip(source_cols, pg_ddls, strict=True):
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_pg_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not PostgreSQL COPY-safe"
                )
            if ddl and (not sqlite_pg_type_is_copy_safe(ddl) or _pg_ddl_is_timestamptz(ddl)):
                raise FastPathUnavailable(
                    f"dest column type {ddl} is not PostgreSQL COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}{where_sql}").fetchone()[0]  # nosec B608
        )

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
            if dest_occupied and not replace_destination:
                if cursor_where:
                    raise FastPathUnavailable(
                        "filtered COPY into occupied dest stays on the incremental staging path"
                    )
                if dest_count_before == source_count:
                    return skip_complete_sqlite(
                        source_count=source_count,
                        dest_count=dest_count_before,
                        extra_snapshot={"sqlite_read": "skip"},
                    )
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
        copy_sql = (
            f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
            "(FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        )
        src_cur = source_conn.cursor()
        dst_cur.copy_expert(copy_sql, _SqliteCopyReader(src_cur, select_sql, pg_ddls))
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "SQLite→PG COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "copy_workers": 1,
                "copy_split": "cursor" if cursor_where else "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "cursor" if cursor_where else "table",
                "source_where": bool(cursor_where),
                "sqlite_read": "select",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except FastPathUnavailable:
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
    except Exception as exc:
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
        wrapped = str(exc)
        if "FastPathUnavailable" in wrapped or "not COPY-safe" in wrapped:
            raise FastPathUnavailable(wrapped) from exc
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("PostgreSQL dest close skipped", exc_info=True)
