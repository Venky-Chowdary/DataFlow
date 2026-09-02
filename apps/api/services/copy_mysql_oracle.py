"""MySQL SELECT → Oracle executemany (cross-engine bulk).

MySQL has no ``COPY TO STDOUT``. One ``START TRANSACTION WITH CONSISTENT
SNAPSHOT`` streams ``SELECT`` (SSCursor); each row is bound with
``oracledb.executemany``. Not ``sqlldr`` / Data Pump. Dest ``COUNT(*)``
must equal the source snapshot.

Oracle VARCHAR2 cannot store empty string: ``''`` IS NULL. That is
engine law, not a silent row drop. Empty-string cells from MySQL are
bound as NULL and counted in ``empty_string_as_null_cells``. Rows
still land.

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips complete ranges and DELETE+reloads partial ones. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
blob/json/geometry/bit, public proxy, occupied dest without a mapped
single PK.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_pg import (
    _FETCH_BATCH,
    _mysql_connect,
    _mysql_ident,
    _mysql_table_pk_and_types,
    _plan_pk_partitions,
    _select_sql,
    mysql_type_is_copy_safe,
)
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _create_sql as _ora_create_sql,
    _delete_range as _ora_delete_range,
    _drop_sql as _ora_drop_sql,
    _ident as _ora_ident,
    _oracle_connect,
    _range_count as _ora_range_count,
    _schema_of as _ora_schema_of,
    _table_exists as _ora_table_exists,
    _table_ref as _ora_table_ref,
)
from services.copy_pg_mysql import (
    _jsonable_bound,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)
from services.copy_pg_oracle import _VARCHAR2_BASES, pg_oracle_copy_batch

logger = logging.getLogger(__name__)


def mysql_oracle_copy_enabled() -> bool:
    raw = (getenv_brand("MYSQL_ORACLE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def python_bind_for_ora_ddl(
    ddl: str, coerced: list[int]
) -> Callable[[Any], Any]:
    """Bind a native Python cell into Oracle. ``''`` on VARCHAR2 → NULL."""
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in _VARCHAR2_BASES:

        def _varchar2(value: Any) -> Any:
            if value is None:
                return None
            if value == "":
                coerced[0] += 1
                return None
            return value

        return _varchar2
    if base in {
        "NUMBER",
        "INTEGER",
        "INT",
        "SMALLINT",
        "INT4",
        "INT8",
        "BIGINT",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARYFLOAT",
        "BINARY_DOUBLE",
        "BINARYDOUBLE",
    }:

        def _number(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, bool):
                return int(value)
            return value

        return _number
    return lambda value: value


def _select_into_oracle(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    insert_sql: str,
    converters: list[Callable[[Any], Any]],
    batch_size: int,
) -> int:
    from pymysql.cursors import SSCursor

    copied = 0
    cur = source_conn.cursor(SSCursor)
    try:
        cur.execute(select_sql)
        batch: list[tuple[Any, ...]] = []
        while True:
            rows = cur.fetchmany(_FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                batch.append(tuple(conv(val) for conv, val in zip(converters, row)))
                if len(batch) >= batch_size:
                    dst_cur.executemany(insert_sql, batch)
                    copied += len(batch)
                    batch.clear()
        if batch:
            dst_cur.executemany(insert_sql, batch)
            copied += len(batch)
        return copied
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("MySQL stream cursor close skipped", exc_info=True)


def copy_mysql_to_oracle(
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
    """Stream MySQL rows into Oracle. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mysql_oracle_copy_enabled():
        raise FastPathUnavailable("MySQL→Oracle COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: Oracle bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    table_q = _mysql_ident(source_table)
    dst_schema = _ora_schema_of(dest_cfg, dest_schema)
    dest_ref = _ora_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ora_ident(c) for c in target_cols)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(target_cols)))
    insert_sql = (
        f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    )
    coerced = [0]
    converters = [python_bind_for_ora_ddl(ddl, coerced) for ddl in oracle_ddls]
    batch_size = pg_oracle_copy_batch()

    source_conn = _mysql_connect(source_cfg)
    dest_conn = _oracle_connect(dest_cfg)
    created_here = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    dst_cur = dest_conn.cursor()
    try:
        with source_conn.cursor() as src_cur:
            src_cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            src_cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            pk_cols, live = _mysql_table_pk_and_types(src_cur, source_table, source_cols)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in source_cols:
                declared = live_l.get(col.lower()) or ""
                if not mysql_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"source column {col!r} type {declared} is not COPY-safe"
                    )
            pk_map = mapped_single_pk(pk_cols, pairs)

            exists = _ora_table_exists(dst_cur, dst_schema, dest_table)
            dest_occupied = False
            if replace_destination and exists:
                dst_cur.execute(_ora_drop_sql(dest_ref))
                dest_conn.commit()
                exists = False
            if exists:
                dest_occupied = _ora_count(dst_cur, dest_ref) > 0
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty Oracle dest stays on the row path"
                    )
            else:
                pk_dest = [
                    rename
                    for src_pk in pk_cols
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                dst_cur.execute(
                    _ora_create_sql(
                        dest_ref, dest_table, pairs, oracle_ddls, pk_dest
                    )
                )
                dest_conn.commit()
                created_here = True

            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            copy_split = "serial"
            to_copy: list[str] = [_select_sql(table_q, source_cols, "")]

            if pk_map is not None:
                src_pk, dest_pk = pk_map
                src_ident = _mysql_ident(src_pk)
                shard_mode = "pk"
                preserve_dest_on_failure = True
                pk_declared = live_l.get(src_pk.lower()) or ""
                partitions = _plan_pk_partitions(
                    src_cur, table_q, src_ident, pk_declared, n_parts, source_count
                )
                if dest_occupied:
                    copy_split = "pk"
                    dest_ident = _ora_ident(dest_pk)
                    dest_conn.commit()
                    to_copy = []
                    for part in partitions:
                        already = _ora_range_count(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                            to_copy.append(
                                _select_sql(
                                    table_q,
                                    source_cols,
                                    str(part.get("predicate") or ""),
                                )
                            )
                        else:
                            _ora_delete_range(
                                dst_cur, dest_ref, dest_ident, part
                            )
                            part["action"] = "reload"
                            to_copy.append(
                                _select_sql(
                                    table_q,
                                    source_cols,
                                    str(part.get("predicate") or ""),
                                )
                            )
                    dest_conn.commit()
                else:
                    to_copy = [_select_sql(table_q, source_cols, "")]

        for sql in to_copy:
            _select_into_oracle(
                source_conn,
                dst_cur,
                select_sql=sql,
                insert_sql=insert_sql,
                converters=converters,
                batch_size=batch_size,
            )
            dest_conn.commit()

        dest_count = _ora_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "MySQL→Oracle COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        if shard_mode == "pk" and pk_map is not None:
            dest_ident = _ora_ident(pk_map[1])
            dest_conn.commit()
            for part in partitions:
                dest_part = _ora_range_count(dst_cur, dest_ref, dest_ident, part)
                part["dest_count"] = dest_part
                if dest_part != int(part["source_count"]):
                    raise ValueError(
                        "PK range dest COUNT "
                        f"{dest_part} != source {part['source_count']} "
                        f"(lo={part['lo']!r} hi={part['hi']!r})"
                    )
        dest_conn.commit()
        try:
            source_conn.commit()
        except Exception:
            logger.debug("MySQL source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        partition_proof = [
            {
                "lo": _jsonable_bound(p.get("lo")),
                "hi": _jsonable_bound(p.get("hi")),
                "null_shard": bool(p.get("null_shard")),
                "source_count": int(p["source_count"]),
                "dest_count": int(p.get("dest_count") or 0),
                "action": str(p.get("action") or "load"),
            }
            for p in partitions
        ]
        skipped = sum(1 for p in partitions if p.get("action") == "skip")
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "mysql_consistent_snapshot": True,
                "copy_workers": 1,
                "copy_split": copy_split,
                "copy_partitions": max(len(partitions), 1),
                "partitions_skipped": skipped,
                "partitions_loaded": len(to_copy),
                "shard_mode": shard_mode,
                "empty_string_as_null_cells": coerced[0],
                "partition_proof": partition_proof,
            },
            proof_scope=(
                "partition_dest_count_equals_source_snapshot"
                if partition_proof
                else "dest_count_equals_source_snapshot_count"
            ),
        )
    except Exception:
        if preserve_dest_on_failure:
            raise
        if created_here:
            try:
                dst_cur.execute(_ora_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
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
            logger.debug("MySQL source close skipped", exc_info=True)
