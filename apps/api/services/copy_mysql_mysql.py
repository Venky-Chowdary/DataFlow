"""MySQL → MySQL identity bulk (same-engine).

Same-instance: ``INSERT INTO dest SELECT … FROM src`` on the connection that
holds ``START TRANSACTION WITH CONSISTENT SNAPSHOT``. MySQL executes the copy;
Python never formats a row. Dest CREATE is a separate connection so DDL does
not implicit-commit the snapshot.

Cross-host (or ``DATAFLOW_MYSQL_MYSQL_INSERT_SELECT=0``): unbuffered SELECT →
FIFO TSV (LOAD DATA encoder) → STRICT ``LOAD DATA LOCAL INFILE``. Same dest
``COUNT(*)`` proof. Parallel source reads are not used (InnoDB has no snapshot
id to share).

A mapped single PK still proves dest ``COUNT(*)`` per key range. Non-empty dest
skips complete ranges and DELETE+reloads partial ones.

Declines (row path keeps quarantine): transforms that change values, public
proxy, cross-host when LOAD DATA is off or types are not LOAD-DATA-safe,
copy onto the same table.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_pg import (
    _FETCH_BATCH,
    _PIPE_CHUNK,
    _mysql_connect,
    _mysql_ident,
    _mysql_table_pk_and_types,
    _plan_pk_partitions,
    _select_sql,
    mysql_type_is_copy_safe,
)
from services.copy_pg_mysql import (
    _delete_mysql_range,
    _jsonable_bound,
    _mysql_create_sql,
    _mysql_range_count,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)

logger = logging.getLogger(__name__)


def mysql_mysql_insert_select_enabled() -> bool:
    raw = (getenv_brand("MYSQL_MYSQL_INSERT_SELECT", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _norm_mysql_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return "127.0.0.1"
    return h


def mysql_same_instance(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    """True only when host+port are present and equal. Fail closed on blanks."""
    src_host = _norm_mysql_host(str(src_cfg.get("host") or ""))
    dest_host = _norm_mysql_host(str(dest_cfg.get("host") or ""))
    if not src_host or not dest_host:
        return False
    src_port = int(src_cfg.get("port") or 3306)
    dest_port = int(dest_cfg.get("port") or 3306)
    return src_host == dest_host and src_port == dest_port


def _qual(database: str, table: str) -> str:
    db = (database or "").strip()
    if db:
        return f"{_mysql_ident(db)}.{_mysql_ident(table)}"
    return _mysql_ident(table)


def _insert_select_sql(
    dest_ref: str,
    source_ref: str,
    pairs: list[tuple[str, str]],
    predicate: str,
) -> str:
    dest_cols = ", ".join(_mysql_ident(t) for _s, t in pairs)
    src_cols = ", ".join(_mysql_ident(s) for s, _t in pairs)
    where = f" WHERE {predicate}" if predicate else ""
    return (
        f"INSERT INTO {dest_ref} ({dest_cols}) "  # nosec B608
        f"SELECT {src_cols} FROM {source_ref}{where}"
    )


def fast_load_data_text_value(value: object) -> str:
    """LOAD DATA text for MySQL identity hot types; else canonical encoder."""
    if value is None:
        return "\\N"
    t = type(value)
    if t is int:
        return str(value)
    if t is str:
        if "\\" not in value and "\t" not in value and "\n" not in value and "\r" not in value:
            return value
        return (
            value.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
    import datetime

    if t is datetime.date:
        return value.isoformat()
    from connectors.mysql_load_data import load_data_text_value

    return load_data_text_value(value)


def _fifo_mysql_into_mysql(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    table_q: str,
    columns: list[str],
) -> None:
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        quote_load_data_path,
    )

    tmp = tempfile.mkdtemp(prefix="df_mysql_mysql_")
    path = os.path.join(tmp, "stream.tsv")
    os.mkfifo(path, 0o600)
    load_sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
    )
    failure: list[BaseException] = []

    def _pump() -> None:
        try:
            from pymysql.cursors import SSCursor

            encode = fast_load_data_text_value
            join = "\t".join
            with source_conn.cursor(SSCursor) as stream:
                stream.execute(select_sql)
                with open(path, "wb", buffering=_PIPE_CHUNK) as writer:
                    while True:
                        batch = stream.fetchmany(_FETCH_BATCH)
                        if not batch:
                            break
                        payload = "\n".join(
                            join(encode(v) for v in row) for row in batch
                        )
                        writer.write((payload + "\n").encode("utf-8"))
        except BaseException as exc:  # noqa: BLE001 — re-raised on the caller
            failure.append(exc)

    pump = threading.Thread(target=_pump, name="mysql-mysql-copy-fifo", daemon=True)
    pump.start()
    try:
        dst_cur.execute(load_sql)
        dst_cur.execute("SHOW WARNINGS")
        blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
        if blocked:
            raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")
    except BaseException:
        pump.join(timeout=30)
        raise
    finally:
        pump.join(timeout=120)
        try:
            os.unlink(path)
        except OSError:
            logger.debug("fifo unlink skipped", exc_info=True)
        try:
            os.rmdir(tmp)
        except OSError:
            logger.debug("fifo dir rmdir skipped", exc_info=True)
    if failure:
        raise failure[0]


def copy_mysql_to_mysql(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    replace_destination: bool,
    source_where: str = "",
) -> FastPathResult:
    """Identity MySQL→MySQL. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: MySQL bulk copy not assumed")

    src_db = str(source_cfg.get("database") or "")
    dest_db = str(dest_cfg.get("database") or "")
    if (
        mysql_same_instance(source_cfg, dest_cfg)
        and src_db.lower() == dest_db.lower()
        and source_table.lower() == dest_table.lower()
    ):
        raise FastPathUnavailable("refusing copy onto the same MySQL table")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _qual(src_db, source_table)
    dest_ref = _qual(dest_db, dest_table)
    dest_local = _mysql_ident(dest_table)

    same_instance = mysql_same_instance(source_cfg, dest_cfg)
    use_insert_select = same_instance and mysql_mysql_insert_select_enabled()

    source_conn = _mysql_connect(source_cfg)
    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    existed_before = False
    pk_map: tuple[str, str] | None = None
    try:
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
            pk_cols, live = _mysql_table_pk_and_types(src_cur, source_table, source_cols)
            live_l = {k.lower(): v for k, v in live.items()}
            create_ddls: list[str] = []
            for i, col in enumerate(source_cols):
                declared = live_l.get(col.lower()) or mysql_ddls[i]
                if use_insert_select:
                    create_ddls.append(declared)
                else:
                    if not mysql_type_is_copy_safe(declared):
                        raise FastPathUnavailable(
                            f"source column {col!r} type {declared} is not LOAD DATA safe"
                        )
                    create_ddls.append(declared)
            pk_map = mapped_single_pk(pk_cols, pairs)

            if not use_insert_select:
                from connectors.mysql_load_data import mysql_load_data_session_ready

                ready, why = mysql_load_data_session_ready(dst_cur, dest_conn)
                if not ready:
                    raise FastPathUnavailable(why)

            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (dest_table,),
            )
            exists = dst_cur.fetchone() is not None
            existed_before = bool(exists)
            dest_occupied = False
            if replace_destination and exists:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_local}")  # nosec B608
                dest_conn.commit()
                exists = False
            if exists:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_local}")  # nosec B608
                dest_occupied = int(dst_cur.fetchone()[0]) > 0
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty MySQL dest stays on the row path"
                    )
            else:
                pk = [
                    rename
                    for src_pk in pk_cols
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                dst_cur.execute(
                    _mysql_create_sql(dest_table, pairs, create_ddls, pk)
                )
                created_here = True
            dest_conn.commit()

            src_cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            src_cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            cursor_where = (source_where or "").strip()
            where_sql = f" WHERE {cursor_where}" if cursor_where else ""
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}{where_sql}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            predicates: list[str] = [cursor_where] if cursor_where else [""]

            if cursor_where:
                if dest_occupied:
                    raise FastPathUnavailable(
                        "filtered COPY into occupied dest stays on the incremental staging path"
                    )
                shard_mode = "cursor"
                partitions = [{
                    "lo": None,
                    "hi": None,
                    "null_shard": False,
                    "source_count": source_count,
                    "predicate": cursor_where,
                    "action": "load",
                }]
            elif pk_map is not None:
                src_pk, dest_pk = pk_map
                src_ident = _mysql_ident(src_pk)
                shard_mode = "pk"
                pk_declared = live_l.get(src_pk.lower()) or ""
                partitions = _plan_pk_partitions(
                    src_cur, source_ref, src_ident, pk_declared, n_parts, source_count
                )
                if dest_occupied:
                    dest_ident = _mysql_ident(dest_pk)
                    dest_conn.commit()
                    to_copy: list[str] = []
                    for part in partitions:
                        already = _mysql_range_count(
                            dst_cur, dest_local, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                            to_copy.append(str(part.get("predicate") or ""))
                        else:
                            _delete_mysql_range(
                                dst_cur, dest_local, dest_ident, part
                            )
                            part["action"] = "reload"
                            to_copy.append(str(part.get("predicate") or ""))
                    dest_conn.commit()
                    predicates = to_copy
                else:
                    predicates = [""]

            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty MySQL dest stays on the row path"
                )

            copy_split = "insert_select" if use_insert_select else "load_data_fifo"
            if predicates:
                if use_insert_select:
                    for pred in predicates:
                        src_cur.execute(
                            _insert_select_sql(dest_ref, source_ref, pairs, pred)
                        )
                    source_conn.commit()
                    dest_conn.commit()
                else:
                    src_cur.close()
                    load_pred = ""
                    if len(predicates) == 1:
                        load_pred = predicates[0]
                    elif len(predicates) > 1:
                        load_pred = " OR ".join(
                            f"({p})" for p in predicates if p
                        )
                    select_sql = _select_sql(source_ref, source_cols, load_pred)
                    with dest_conn.cursor() as load_cur:
                        _fifo_mysql_into_mysql(
                            source_conn,
                            load_cur,
                            select_sql=select_sql,
                            table_q=dest_local,
                            columns=target_cols,
                        )
                    dest_conn.commit()

            with dest_conn.cursor() as count_cur:
                count_cur.execute(f"SELECT COUNT(*) FROM {dest_local}")  # nosec B608
                dest_count = int(count_cur.fetchone()[0])
            if dest_count != source_count:
                raise ValueError(
                    "MySQL→MySQL copy refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _mysql_ident(pk_map[1])
                dest_conn.commit()
                with dest_conn.cursor() as count_cur:
                    for part in partitions:
                        dest_part = _mysql_range_count(
                            count_cur, dest_local, dest_ident, part
                        )
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
                logger.debug("source commit after dest COUNT skipped", exc_info=True)
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
            return FastPathResult(
                rows_copied=dest_count,
                source_rows=source_count,
                source_checksum=proof,
                target_rows=dest_count,
                target_checksum=proof,
                source_snapshot={
                    "mysql_consistent_snapshot": True,
                    "same_instance": same_instance,
                    "copy_workers": 1,
                    "copy_split": copy_split,
                    "copy_partitions": len(partitions) or 1,
                    "partitions_skipped": sum(
                        1 for p in partitions if p.get("action") == "skip"
                    ),
                    "shard_mode": shard_mode if partitions else "serial",
                    "partition_proof": partition_proof,
                    "source_where": bool(cursor_where),
                },
                proof_scope=(
                    "partition_dest_count_equals_source_snapshot"
                    if partition_proof
                    else "dest_count_equals_source_snapshot_count"
                ),
            )
    except Exception:
        if created_here:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {dest_local}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and pk_map is None:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {dest_local}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("mysql source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql dest close skipped", exc_info=True)
