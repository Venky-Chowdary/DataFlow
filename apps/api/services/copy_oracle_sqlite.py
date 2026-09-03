"""Oracle SHARE-lock SELECT → SQLite executemany (cross-engine bulk).

The reverse of ``copy_sqlite_oracle``. This host has no client ``sqlldr``
/ Data Pump. One ``LOCK TABLE src IN SHARE MODE`` transaction streams
``SELECT``; Python values bind with ``executemany`` INSERT. Dest
``COUNT(*)`` must equal the source snapshot COUNT **before commit**.
Empty dest is INSERT, **not** upsert / sqlite3 ``.import`` / sqlldr.
Occupied dest whose COUNT already equals the source snapshot is
skip-complete. Occupied dest with a different COUNT declines.
Occupancy is counted **before** DROP so overwrite stamps
``sqlite_write`` correctly. ``:memory:`` / BLOB dest DDL decline.
DATE / DATETIME-NTZ land as SQLite TEXT (ISO — SQLite has no DATE
affinity). BLOB/RAW/XMLTYPE/SDO_GEOMETRY decline.

Oracle ``VARCHAR2`` stores ``''`` as ``NULL`` (engine law). Source cells
that were originally empty strings therefore arrive here as ``None`` and
SQLite stores NULL. That is not a row drop.

Declines (row path keeps quarantine): transforms that change values,
BLOB/RAW/XMLTYPE, public proxy, occupied dest with dest COUNT ≠ source,
``:memory:``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _ora_table_pk_and_types,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_ref as _ora_table_ref,
    oracle_cfg_is_public_proxy,
)
from services.copy_oracle_pg import (
    _select_sql,
    _tune_fetch,
    oracle_type_is_copy_safe,
)
from services.copy_pg_mysql import mapping_is_plain_carry
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

_FETCH_BATCH = 8192


def oracle_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("ORACLE_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def oracle_sqlite_copy_batch() -> int:
    raw = (getenv_brand("ORACLE_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def oracle_value_to_sqlite(value: Any) -> Any:
    """Bind an Oracle Python value. DATE/DATETIME-NTZ land as ISO TEXT."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "timestamptz Oracle value is not SQLite COPY-safe"
            )
        if value.hour or value.minute or value.second or value.microsecond:
            return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        return date(value.year, value.month, value.day).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Oracle field is not SQLite COPY-safe")
    return value


def copy_oracle_to_sqlite(
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
    """INSERT Oracle snapshot rows into SQLite. Dest COUNT(*) before commit is the proof."""
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_sqlite_copy_enabled():
        raise FastPathUnavailable("Oracle→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")

    if oracle_cfg_is_public_proxy(source_cfg) or oracle_cfg_is_public_proxy(dest_cfg):
        raise FastPathUnavailable("public proxy: SQLite bulk copy not assumed")

    sqlite_resolved_path(dest_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ora_schema_of(source_cfg, source_schema)
    source_ref = _ora_table_ref(src_schema, source_table)
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    batch_size = oracle_sqlite_copy_batch()

    source_conn = _oracle_connect(source_cfg)
    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    src_cur = source_conn.cursor()
    try:
        _pk_cols, live = _ora_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not oracle_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not SQLite COPY-safe"
                )
        src_cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _ora_count(src_cur, source_ref)
        select_sql = _select_sql(source_ref, source_cols, "")
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
                try:
                    source_conn.rollback()
                except Exception:
                    logger.debug("Oracle source rollback on skip skipped", exc_info=True)
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "oracle_lock": "share",
                        "oracle_read": "skip",
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
            _tune_fetch(cur)
            cur.execute(select_sql)
            while True:
                rows = cur.fetchmany(_FETCH_BATCH)
                if not rows:
                    break
                for row in rows:
                    pending.append(tuple(oracle_value_to_sqlite(v) for v in row))
                    if len(pending) >= batch_size:
                        dest_conn.executemany(insert_sql, pending)
                        inserted += len(pending)
                        pending.clear()
        finally:
            try:
                cur.close()
            except Exception:
                logger.debug("Oracle stream cursor close skipped", exc_info=True)
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "Oracle→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source snapshot {source_count}"
            )
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("Oracle source commit skipped", exc_info=True)
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "oracle_lock": "share",
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "oracle_read": "share_select",
                "sqlite_write": sqlite_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("Oracle source rollback skipped", exc_info=True)
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
                logger.debug("Oracle source cursor close skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("Oracle source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
