"""SQLite SELECT → Oracle executemany (cross-engine bulk).

One ``BEGIN`` on the source file streams ``SELECT``; Python values bind
with ``oracledb.executemany``. Dest ``COUNT(*)`` must equal the source
COUNT. This is **not** ``sqlldr`` / Data Pump / sqlite3 ``.dump``.
Empty dest is INSERT, **not** upsert. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. Occupancy is counted **before** DROP so overwrite
stamps ``oracle_write`` correctly. ``:memory:`` / BLOB / JSON /
DATETIME / TIMESTAMP decline. DATE ISO text or a calendar day binds as
Oracle DATE when the dest DDL is DATE; TEXT ISO stays a string.

Oracle VARCHAR2 cannot store empty string: ``''`` IS NULL. That is
engine law, not a silent row drop. Empty-string cells from SQLite are
bound as NULL and counted in ``empty_string_as_null_cells``. Rows
still land; dest ``COUNT(*)`` still equals source COUNT.

Declines (row path keeps quarantine): transforms that change values,
BLOB/JSON/DATETIME, public proxy, occupied dest with dest COUNT ≠
source, ``:memory:``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_oracle import python_bind_for_ora_ddl
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _create_sql as _ora_create_sql,
    _drop_sql as _ora_drop_sql,
    _ident as _ora_ident,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_exists as _ora_table_exists,
    _table_ref as _ora_table_ref,
    oracle_cfg_is_public_proxy,
)
from services.copy_oracle_pg import oracle_type_is_copy_safe
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pg_oracle import pg_oracle_copy_batch
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
_UNSAFE_SQLITE_ORA_BASES = frozenset({
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "JSON",
    "JSONB",
})


def sqlite_oracle_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_ORACLE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlite_oracle_type_is_copy_safe(declared: str) -> bool:
    if not sqlite_type_is_copy_safe(declared):
        return False
    base = (declared or "").strip().upper().replace(" ", "").split("(", 1)[0]
    return base not in _UNSAFE_SQLITE_ORA_BASES


_SQLITE_TEXT_BASES = frozenset({
    "TEXT",
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "NCHAR",
    "STRING",
    "CLOB",
    "NCLOB",
})


def sqlite_declared_to_oracle_ddl(declared: str) -> str:
    """Oracle CREATE DDL for a SQLite identity mapping.

    ``ddl_type("oracle", "TEXT")`` is CLOB. CLOB is not COPY-safe to
    read back (LOB locators), so identity SQLite TEXT lands as
    VARCHAR2(4000). Values longer than 4000 fail closed at bind.
    """
    from services.type_system import ddl_type

    dest_ddl = ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
    dest_base = dest_ddl.split("(")[0].strip().upper().replace(" ", "")
    src_base = (declared or "").split("(")[0].strip().upper().replace(" ", "")
    if dest_base in {"CLOB", "NCLOB"} and src_base in _SQLITE_TEXT_BASES:
        return "VARCHAR2(4000)"
    return dest_ddl


def sqlite_value_to_oracle(value: Any, ddl: str, coerced: list[int]) -> Any:
    """SQLite cell → Oracle bind. DATE ISO is a calendar day; ``''`` on VARCHAR2 → NULL."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("BLOB values are not Oracle COPY-safe")
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in _UNSAFE_SQLITE_ORA_BASES:
        raise FastPathUnavailable(
            f"{base} SQLite value is not Oracle COPY-safe"
        )
    if base == "DATE":
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                raise FastPathUnavailable(
                    "timestamptz SQLite value is not Oracle COPY-safe"
                )
            if value.hour or value.minute or value.second or value.microsecond:
                raise FastPathUnavailable(
                    "DATETIME SQLite value is not Oracle DATE COPY-safe"
                )
            return date(value.year, value.month, value.day)
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                text = value.replace("T", " ").strip()
                if " " in text:
                    clock = text.split(" ", 1)[1]
                    if clock and clock not in {"00:00:00", "00:00:00.000"}:
                        raise FastPathUnavailable(
                            "DATETIME SQLite value is not Oracle DATE COPY-safe"
                        )
                    text = text.split(" ", 1)[0]
                return date.fromisoformat(text[:10])
            except FastPathUnavailable:
                raise
            except ValueError as exc:
                raise FastPathUnavailable(
                    f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
                ) from exc
        raise FastPathUnavailable(
            f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
        )
    return python_bind_for_ora_ddl(ddl, coerced)(value)


def copy_sqlite_to_oracle(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    oracle_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """SELECT SQLite into Oracle executemany. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_oracle_copy_enabled():
        raise FastPathUnavailable("SQLite→Oracle COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in oracle_ddls:
        if ddl and not oracle_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not Oracle COPY-safe")

    if oracle_cfg_is_public_proxy(dest_cfg):
        raise FastPathUnavailable("public proxy: Oracle bulk copy not assumed")

    sqlite_resolved_path(source_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_ref = sqlite_ident(source_table)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}"  # nosec B608
    dst_schema = _ora_schema_of(dest_cfg, dest_schema)
    dest_ref = _ora_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ora_ident(c) for c in target_cols)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(target_cols)))
    insert_sql = (
        f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    )
    batch_size = pg_oracle_copy_batch()
    coerced = [0]

    source_conn = sqlite_connect(source_cfg)
    dest_conn = _oracle_connect(dest_cfg)
    created_here = False
    dst_cur = dest_conn.cursor()
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_oracle_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Oracle COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}").fetchone()[0]  # nosec B608
        )

        exists = _ora_table_exists(dst_cur, dst_schema, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = _ora_count(dst_cur, dest_ref)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                try:
                    source_conn.rollback()
                except Exception:
                    logger.debug("SQLite source rollback on skip skipped", exc_info=True)
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "sqlite_read": "skip",
                        "oracle_write": "skip",
                        "empty_string_as_null_cells": 0,
                    },
                )
            raise FastPathUnavailable(
                "append into occupied Oracle dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dst_cur.execute(_ora_drop_sql(dest_ref))
            dest_conn.commit()
            exists = False
        if not exists:
            dst_cur.execute(
                _ora_create_sql(dest_ref, dest_table, pairs, oracle_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        copied = 0
        batch: list[tuple[Any, ...]] = []
        src_cur = source_conn.cursor()
        try:
            src_cur.execute(select_sql)
            while True:
                rows = src_cur.fetchmany(_FETCH_BATCH)
                if not rows:
                    break
                for row in rows:
                    batch.append(
                        tuple(
                            sqlite_value_to_oracle(val, ddl, coerced)
                            for val, ddl in zip(row, oracle_ddls, strict=True)
                        )
                    )
                    if len(batch) >= batch_size:
                        dst_cur.executemany(insert_sql, batch)
                        copied += len(batch)
                        batch.clear()
            if batch:
                dst_cur.executemany(insert_sql, batch)
                copied += len(batch)
            if copied != source_count:
                dest_conn.rollback()
                raise ValueError(
                    "SQLite→Oracle COPY refused: bound rows "
                    f"{copied} != source COUNT {source_count}"
                )
            dest_count = _ora_count(dst_cur, dest_ref)
            if dest_count != source_count:
                dest_conn.rollback()
                raise ValueError(
                    "SQLite→Oracle COPY refused: dest COUNT(*) "
                    f"{dest_count} != source COUNT {source_count}"
                )
            dest_conn.commit()
        finally:
            try:
                src_cur.close()
            except Exception:
                logger.debug("SQLite stream cursor close skipped", exc_info=True)

        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
        oracle_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "shard_mode": "table",
                "sqlite_read": "select",
                "oracle_write": oracle_write,
                "empty_string_as_null_cells": coerced[0],
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(_ora_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("Oracle dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dst_cur.close()
        except Exception:
            logger.debug("Oracle dest cursor close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("Oracle dest close skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
