"""Oracle SELECT → SQL Server fast_executemany (cross-engine bulk).

The reverse of ``copy_sqlserver_oracle``. One ``LOCK TABLE src IN SHARE
MODE`` transaction streams ``SELECT``; each row is bound with pyodbc
``fast_executemany``. Not BCP / ``BULK INSERT`` CSV (quoted empty string
collapses to NULL on Linux SQL Server). Dest ``COUNT(*)`` must equal
the source snapshot.

Oracle VARCHAR2 stores ``''`` as NULL (engine law). Source cells that
were originally empty strings therefore arrive here as ``None`` and
bind as NULL. That is not a row drop.

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips complete ranges and DELETE+reloads partial ones. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
BLOB/RAW/XMLTYPE/SDO_GEOMETRY, public proxy, occupied dest without a
mapped single PK.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _ident as _ora_ident,
    _ora_table_pk_and_types,
    _oracle_connect,
    _plan_pk_partitions,
    _schema_of as _ora_schema_of,
    _table_ref as _ora_table_ref,
)
from services.copy_oracle_pg import _select_sql, _tune_fetch, oracle_type_is_copy_safe
from services.copy_pg_mysql import (
    _jsonable_bound,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)
from services.copy_pg_sqlserver import _enable_fast_executemany, pg_sqlserver_copy_batch
from services.copy_sqlserver_pg import _FETCH_BATCH, _close_ss
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _create_sql as _ss_create_sql,
    _delete_range as _ss_delete_range,
    _drop_sql as _ss_drop_sql,
    _has_identity,
    _ident as _ss_ident,
    _range_count as _ss_range_count,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _table_exists as _ss_table_exists,
    _table_ref as _ss_table_ref,
)

logger = logging.getLogger(__name__)


def oracle_sqlserver_copy_enabled() -> bool:
    raw = (getenv_brand("ORACLE_SQLSERVER_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _select_into_sqlserver(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    params: list[Any],
    insert_sql: str,
    batch_size: int,
) -> int:
    copied = 0
    cur = source_conn.cursor()
    _tune_fetch(cur)
    try:
        if params:
            cur.execute(select_sql, params)
        else:
            cur.execute(select_sql)
        batch: list[tuple[Any, ...]] = []
        while True:
            rows = cur.fetchmany(_FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                batch.append(tuple(row))
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
            logger.debug("Oracle stream cursor close skipped", exc_info=True)


def copy_oracle_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Stream Oracle rows into SQL Server. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_sqlserver_copy_enabled():
        raise FastPathUnavailable("Oracle→SQL Server COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: SQL Server bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ora_schema_of(source_cfg, source_schema)
    source_ref = _ora_table_ref(src_schema, source_table)
    dst_schema = _ss_schema_of(dest_cfg, dest_schema)
    dest_ref = _ss_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ss_ident(c) for c in target_cols)
    placeholders = ", ".join(["%s"] * len(target_cols))
    insert_sql = (
        f"INSERT INTO {dest_ref} WITH (TABLOCK) ({col_sql}) "  # nosec B608
        f"VALUES ({placeholders})"
    )
    batch_size = pg_sqlserver_copy_batch()

    source_conn = _oracle_connect(source_cfg)
    dest_conn = _ss_connect(dest_cfg)
    created_here = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    src_cur = source_conn.cursor()
    dst_cur = dest_conn.cursor()
    _enable_fast_executemany(dst_cur)
    try:
        pk_cols, live = _ora_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not oracle_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not COPY-safe"
                )
        pk_map = mapped_single_pk(pk_cols, pairs)

        exists = _ss_table_exists(dst_cur, dst_schema, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(_ss_drop_sql(dest_ref))
            dest_conn.commit()
            exists = False
        if exists:
            dest_occupied = _ss_count(dst_cur, dest_ref) > 0
            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty SQL Server dest stays on the row path"
                )
        else:
            pk_dest = [
                rename
                for src_pk in pk_cols
                for src_col, rename in pairs
                if src_col.lower() == src_pk.lower()
            ]
            dst_cur.execute(
                _ss_create_sql(
                    dest_ref, dest_table, pairs, sqlserver_ddls, pk_dest
                )
            )
            dest_conn.commit()
            created_here = True

        src_cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _ora_count(src_cur, source_ref)
        workers = pg_mysql_copy_workers(source_count)
        n_parts = pg_mysql_copy_partitions(source_count, workers)
        partitions: list[dict[str, Any]] = []
        shard_mode = "serial"
        copy_split = "serial"
        to_copy: list[dict[str, Any]] = [{"predicate": "", "params": []}]

        if pk_map is not None:
            src_pk, dest_pk = pk_map
            src_ident = _ora_ident(src_pk)
            shard_mode = "pk"
            preserve_dest_on_failure = True
            pk_declared = live_l.get(src_pk.lower()) or ""
            partitions = _plan_pk_partitions(
                src_cur, source_ref, src_ident, pk_declared, n_parts, source_count
            )
            if dest_occupied:
                copy_split = "pk"
                dest_ident = _ss_ident(dest_pk)
                dest_conn.commit()
                to_copy = []
                for part in partitions:
                    already = _ss_range_count(
                        dst_cur, dest_ref, dest_ident, part
                    )
                    expected = int(part["source_count"])
                    if already == expected:
                        part["action"] = "skip"
                        part["dest_count"] = already
                    elif already == 0:
                        part["action"] = "load"
                        to_copy.append(part)
                    else:
                        _ss_delete_range(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        part["action"] = "reload"
                        to_copy.append(part)
                dest_conn.commit()
            else:
                to_copy = [{"predicate": "", "params": []}]

        src_cur.close()
        src_cur = None  # type: ignore[assignment]
        identity = _has_identity(dst_cur, dst_schema, dest_table)
        if identity:
            dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
        try:
            for item in to_copy:
                clause = str(item.get("predicate") or "")
                params = list(item.get("params") or [])
                _select_into_sqlserver(
                    source_conn,
                    dst_cur,
                    select_sql=_select_sql(source_ref, source_cols, clause),
                    params=params,
                    insert_sql=insert_sql,
                    batch_size=batch_size,
                )
                dest_conn.commit()
        finally:
            if identity:
                try:
                    dst_cur.execute(f"SET IDENTITY_INSERT {dest_ref} OFF")  # nosec B608
                except Exception:
                    logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)

        dest_count = _ss_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "Oracle→SQL Server COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        if shard_mode == "pk" and pk_map is not None:
            dest_ident = _ss_ident(pk_map[1])
            dest_conn.commit()
            for part in partitions:
                dest_part = _ss_range_count(dst_cur, dest_ref, dest_ident, part)
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
            logger.debug("Oracle source commit skipped", exc_info=True)
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
                "oracle_lock": "share",
                "copy_workers": 1,
                "copy_split": copy_split,
                "copy_partitions": max(len(partitions), 1),
                "partitions_skipped": skipped,
                "partitions_loaded": len(to_copy),
                "shard_mode": shard_mode,
                "varchar2_empty_stored_as_null": True,
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
                dst_cur.execute(_ss_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            if src_cur is not None:
                src_cur.close()
        except Exception:
            logger.debug("Oracle source cursor close skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("SQL Server dest cursor close skipped", exc_info=True)
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("Oracle source rollback skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("Oracle source close skipped", exc_info=True)
        try:
            _close_ss(dest_conn)
        except Exception:
            logger.debug("SQL Server dest close skipped", exc_info=True)
