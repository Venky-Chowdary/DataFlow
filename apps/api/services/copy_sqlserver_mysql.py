"""SQL Server SELECT → MySQL STRICT LOAD DATA (cross-engine bulk).

The reverse of ``copy_mysql_sqlserver``. HOLDLOCK ``SELECT`` is encoded
as LOAD DATA TSV on one thread into a tempfile, then STRICT
``LOAD DATA LOCAL INFILE``. A FIFO + pyodbc pump is **not** used: that
deadlock class was measured on SQL Server→PostgreSQL.

This is **not** BCP. Dest ``COUNT(*)`` must equal the source snapshot.
Empty string stays empty string (MySQL VARCHAR and SQL Server NVARCHAR).

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips complete ranges and DELETE+reloads partial ones. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
varbinary/xml/geography, public proxy, occupied dest without a mapped
single PK, LOAD DATA ineligible sessions.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_mysql import fast_load_data_text_value
from services.copy_mysql_pg import _FETCH_BATCH, _mysql_connect, _mysql_ident
from services.copy_pg_mysql import (
    _delete_mysql_range,
    _jsonable_bound,
    _mysql_create_sql,
    _mysql_range_count,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)
from services.copy_sqlserver_pg import (
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


def sqlserver_mysql_copy_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_MYSQL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _mysql_table_exists(cur: Any, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return int(cur.fetchone()[0]) > 0


def _select_into_mysql_load_data(
    source_conn: Any,
    dest_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    params: list[Any],
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

    fd, path = tempfile.mkstemp(prefix="df_ss_mysql_", suffix=".tsv")
    os.close(fd)
    cur = source_conn.cursor()
    try:
        if params:
            cur.execute(select_sql, params)
        else:
            cur.execute(select_sql)
        encode = fast_load_data_text_value
        join = "\t".join
        with open(path, "wb", buffering=1 << 20) as writer:
            while True:
                batch = cur.fetchmany(_FETCH_BATCH)
                if not batch:
                    break
                payload = "\n".join(join(encode(v) for v in row) for row in batch)
                writer.write((payload + "\n").encode("utf-8"))
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
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("SQL Server stream cursor close skipped", exc_info=True)
        try:
            os.unlink(path)
        except OSError:
            logger.debug("tempfile unlink skipped", exc_info=True)


def copy_sqlserver_to_mysql(
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
    """Stream SQL Server rows into MySQL LOAD DATA. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_mysql_copy_enabled():
        raise FastPathUnavailable("SQL Server→MySQL COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: LOAD DATA not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ss_schema_of(source_cfg, source_schema)
    source_ref = _ss_table_ref(src_schema, source_table)
    dest_q = _mysql_ident(dest_table)

    source_conn = _ss_connect(source_cfg)
    dest_conn = _mysql_connect(dest_cfg)
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

        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_occupied = int(dst_cur.fetchone()[0]) > 0
            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty MySQL dest stays on the row path"
                )
        else:
            pk_dest = [
                rename
                for src_pk in pk_cols
                for src_col, rename in pairs
                if src_col.lower() == src_pk.lower()
            ]
            dst_cur.execute(
                _mysql_create_sql(dest_table, pairs, mysql_ddls, pk_dest)
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
                dest_ident = _mysql_ident(dest_pk)
                dest_conn.commit()
                to_copy = []
                for part in partitions:
                    already = _mysql_range_count(dst_cur, dest_q, dest_ident, part)
                    expected = int(part["source_count"])
                    if already == expected:
                        part["action"] = "skip"
                        part["dest_count"] = already
                    elif already == 0:
                        part["action"] = "load"
                        to_copy.append(part)
                    else:
                        _delete_mysql_range(dst_cur, dest_q, dest_ident, part)
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
            _select_into_mysql_load_data(
                source_conn,
                dest_conn,
                dst_cur,
                select_sql=_select_sql(
                    source_ref, source_cols, clause, source_hint
                ),
                params=params,
                table_q=dest_q,
                columns=target_cols,
            )
            dest_conn.commit()

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "SQL Server→MySQL COPY refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        if shard_mode == "pk" and pk_map is not None:
            dest_ident = _mysql_ident(pk_map[1])
            dest_conn.commit()
            for part in partitions:
                dest_part = _mysql_range_count(dst_cur, dest_q, dest_ident, part)
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
                "load_data": "tempfile",
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
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
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
            logger.debug("MySQL dest cursor close skipped", exc_info=True)
        try:
            _close_ss(source_conn)
        except Exception:
            logger.debug("SQL Server source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("MySQL dest close skipped", exc_info=True)
