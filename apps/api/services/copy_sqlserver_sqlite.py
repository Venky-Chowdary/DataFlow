"""SQL Server HOLDLOCK SELECT → SQLite executemany (cross-engine bulk).

The reverse of ``copy_sqlite_sqlserver``. SQL Server has no ``COPY TO
STDOUT`` and this host has no client ``bcp``. One HOLDLOCK (or SNAPSHOT)
transaction streams ``SELECT``; Python values bind with ``executemany``
INSERT. Dest ``COUNT(*)`` must equal the source snapshot COUNT **before
commit**. Empty dest is INSERT, **not** upsert / sqlite3 ``.import`` /
BCP. Occupied dest whose COUNT already equals the source snapshot is
skip-complete. Occupied dest with a different COUNT declines.
``:memory:`` / BLOB dest DDL decline. DATE / DATETIME-NTZ land as SQLite
TEXT (ISO — SQLite has no DATE affinity). DATETIMEOFFSET / varbinary /
xml / rowversion decline.

Declines (row path keeps quarantine): transforms that change values,
varbinary/xml/geography/rowversion/datetimeoffset, public proxy,
occupied dest with dest COUNT ≠ source, ``:memory:``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlserver_pg import (
    _FETCH_BATCH,
    _close_ss,
    _select_sql,
    sqlserver_type_is_copy_safe,
)
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _prepare_source_read,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _ss_table_pk_and_types,
    _table_ref as _ss_table_ref,
)
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_table_exists,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def sqlserver_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlserver_sqlite_copy_batch() -> int:
    raw = (getenv_brand("SQLSERVER_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def sqlserver_value_to_sqlite(value: Any) -> Any:
    """Bind a SQL Server Python value. DATE/DATETIME-NTZ land as ISO TEXT."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "timestamptz SQL Server value is not SQLite COPY-safe"
            )
        if value.hour or value.minute or value.second or value.microsecond:
            return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        return date(value.year, value.month, value.day).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary SQL Server field is not SQLite COPY-safe")
    return value


def copy_sqlserver_to_sqlite(
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
    """INSERT SQL Server snapshot rows into SQLite. Dest COUNT(*) before commit is the proof."""
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_sqlite_copy_enabled():
        raise FastPathUnavailable("SQL Server→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: SQLite bulk copy not assumed")

    sqlite_resolved_path(dest_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ss_schema_of(source_cfg, source_schema)
    source_ref = _ss_table_ref(src_schema, source_table)
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    batch_size = sqlserver_sqlite_copy_batch()

    source_conn = _ss_connect(source_cfg)
    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    src_cur = source_conn.cursor()
    try:
        _pk_cols, live = _ss_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not sqlserver_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not SQLite COPY-safe"
                )
        isolation = _prepare_source_read(src_cur, source_conn)
        source_hint = "WITH (HOLDLOCK, TABLOCK)" if isolation == "holdlock" else ""
        source_count = _ss_count(src_cur, source_ref, source_hint)
        select_sql = _select_sql(source_ref, source_cols, "", source_hint)
        src_cur.close()
        src_cur = None  # type: ignore[assignment]

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
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "sqlserver_isolation": isolation,
                        "sqlserver_read": "skip",
                        "sqlite_write": "skip",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if exists:
            live_dest = sqlite_pragma_types(dest_conn, dest_table)
            live_dest_l = {k.lower(): v for k, v in live_dest.items()}
            for col in target_cols:
                declared = live_dest_l.get(col.lower())
                if declared is None:
                    raise FastPathUnavailable(f"dest column {col!r} absent")
                if not sqlite_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"dest column {col!r} type {declared} is not SQLite COPY-safe"
                    )
        else:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        pending: list[tuple[Any, ...]] = []
        inserted = 0
        cur = source_conn.cursor()
        try:
            try:
                cur.arraysize = _FETCH_BATCH
            except Exception:
                logger.debug("SQL Server arraysize skipped", exc_info=True)
            cur.execute(select_sql)
            while True:
                rows = cur.fetchmany(_FETCH_BATCH)
                if not rows:
                    break
                for row in rows:
                    pending.append(tuple(sqlserver_value_to_sqlite(v) for v in row))
                    if len(pending) >= batch_size:
                        dest_conn.executemany(insert_sql, pending)
                        inserted += len(pending)
                        pending.clear()
        finally:
            try:
                cur.close()
            except Exception:
                logger.debug("SQL Server stream cursor close skipped", exc_info=True)
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "SQL Server→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source snapshot {source_count}"
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
                "sqlserver_isolation": isolation,
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "sqlserver_read": "holdlock_select",
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
        if src_cur is not None:
            try:
                src_cur.close()
            except Exception:
                logger.debug("SQL Server source cursor close skipped", exc_info=True)
        try:
            _close_ss(source_conn)
        except Exception:
            logger.debug("SQL Server source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
