"""SQL Server SELECT → Oracle executemany (cross-engine bulk).

SQL Server has no ``COPY TO STDOUT`` and this host has no client ``bcp``.
One HOLDLOCK (or SNAPSHOT) transaction streams ``SELECT``; each row is
bound with ``oracledb.executemany``. Not ``sqlldr`` / Data Pump / BCP.
Dest ``COUNT(*)`` must equal the source snapshot.

Oracle VARCHAR2 cannot store empty string: ``''`` IS NULL. That is
engine law, not a silent row drop. Empty-string cells from NVARCHAR
are bound as NULL and counted in ``empty_string_as_null_cells``. Rows
still land.

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips complete ranges and DELETE+reloads partial ones. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
varbinary/xml/geography, public proxy, occupied dest without a mapped
single PK.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_oracle import python_bind_for_ora_ddl
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
from services.copy_pg_oracle import pg_oracle_copy_batch
from services.copy_sqlserver_pg import (
    _FETCH_BATCH,
    _close_ss,
    _select_sql,
    sqlserver_type_is_copy_safe,
)
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _ident as _ss_ident,
    _plan_pk_partitions,
    _prepare_source_read,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _ss_table_pk_and_types,
    _table_ref as _ss_table_ref,
)

logger = logging.getLogger(__name__)


def sqlserver_oracle_copy_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_ORACLE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _select_into_oracle(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    params: list[Any],
    insert_sql: str,
    converters: list[Callable[[Any], Any]],
    batch_size: int,
) -> int:
    copied = 0
    cur = source_conn.cursor()
    try:
        try:
            cur.arraysize = 8192
        except Exception:
            logger.debug("SQL Server arraysize skipped", exc_info=True)
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
            logger.debug("SQL Server stream cursor close skipped", exc_info=True)


def copy_sqlserver_to_oracle(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    oracle_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Stream SQL Server rows into Oracle. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_oracle_copy_enabled():
        raise FastPathUnavailable("SQL Server→Oracle COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: Oracle bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ss_schema_of(source_cfg, source_schema)
    source_ref = _ss_table_ref(src_schema, source_table)
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

    source_conn = _ss_connect(source_cfg)
    dest_conn = _oracle_connect(dest_cfg)
    created_here = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    src_cur = source_conn.cursor()
    dst_cur = dest_conn.cursor()
    try:
        pk_cols, live = _ss_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not sqlserver_type_is_copy_safe(declared):
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

        isolation = _prepare_source_read(src_cur, source_conn)
        source_hint = "WITH (HOLDLOCK, TABLOCK)" if isolation == "holdlock" else ""
        source_count = _ss_count(src_cur, source_ref, source_hint)
        workers = pg_mysql_copy_workers(source_count)
        n_parts = pg_mysql_copy_partitions(source_count, workers)
        partitions: list[dict[str, Any]] = []
        shard_mode = "serial"
        copy_split = "serial"
        to_copy: list[dict[str, Any]] = [{"predicate": "", "params": []}]

        if pk_map is not None:
            src_pk, dest_pk = pk_map
            src_ident = _ss_ident(src_pk)
            shard_mode = "pk"
            preserve_dest_on_failure = True
            pk_declared = live_l.get(src_pk.lower()) or ""
            partitions = _plan_pk_partitions(
                src_cur, source_ref, src_ident, pk_declared, n_parts, source_count
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
                        to_copy.append(part)
                    else:
                        _ora_delete_range(
                            dst_cur, dest_ref, dest_ident, part
                        )
                        part["action"] = "reload"
                        to_copy.append(part)
                dest_conn.commit()
            else:
                to_copy = [{"predicate": "", "params": []}]

        src_cur.close()
        src_cur = None  # type: ignore[assignment]
        for item in to_copy:
            clause = str(item.get("predicate") or "")
            params = list(item.get("params") or [])
            _select_into_oracle(
                source_conn,
                dst_cur,
                select_sql=_select_sql(
                    source_ref, source_cols, clause, source_hint
                ),
                params=params,
                insert_sql=insert_sql,
                converters=converters,
                batch_size=batch_size,
            )
            dest_conn.commit()

        dest_count = _ora_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "SQL Server→Oracle COPY refused: dest COUNT(*) "
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
            logger.debug("SQL Server source commit skipped", exc_info=True)
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
                "sqlserver_isolation": isolation,
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
            if src_cur is not None:
                src_cur.close()
        except Exception:
            logger.debug("SQL Server source cursor close skipped", exc_info=True)
        try:
            dst_cur.close()
        except Exception:
            logger.debug("Oracle dest cursor close skipped", exc_info=True)
        try:
            _close_ss(source_conn)
        except Exception:
            logger.debug("SQL Server source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("Oracle dest close skipped", exc_info=True)
