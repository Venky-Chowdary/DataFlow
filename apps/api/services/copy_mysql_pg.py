"""MySQL → PostgreSQL COPY FROM STDIN (cross-engine bulk).

The reverse of ``copy_pg_mysql``. MySQL has no ``COPY TO STDOUT``, so one
REPEATABLE READ transaction streams an unbuffered SELECT; each cell is encoded
into PostgreSQL COPY text on a FIFO that ``COPY … FROM STDIN`` reads. The hot
path uses a tight encoder equivalent to ``_copy_text_value`` for None / int /
str / date / datetime / float; every other Python type falls through to the
canonical encoder. Python never runs transform / quarantine / fingerprint.
Dest ``COUNT(*)`` must equal the source snapshot count.

A mapped single PK still proves dest ``COUNT(*)`` per key range after the
load. Parallel MySQL reads are not used: InnoDB cannot export a snapshot id
the way ``pg_export_snapshot()`` does, so extra connections would not share
the coordinator's consistent read.

Declines (row path keeps quarantine): transforms that change values, blob,
json, geometry, bit, binary, TIMESTAMP (session TZ), non-empty append without
a mapped PK.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    _quote,
    _table_ref,
    stream_between_cursors,
)
from services.copy_pg_mysql import (
    _INTEGER_PK_BASES,
    _jsonable_bound,
    _pg_quoted_literal,
    integer_pk_cuts,
    key_ranges_from_cuts,
    mapping_is_plain_carry,
    mapped_single_pk,
    mysql_pk_range_clause,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
    pk_range_predicate,
)

logger = logging.getLogger(__name__)

_PIPE_CHUNK = 1 << 20
_FETCH_BATCH = 8192

_UNSAFE_MYSQL_BASES = frozenset({
    "BLOB",
    "TINYBLOB",
    "MEDIUMBLOB",
    "LONGBLOB",
    "BINARY",
    "VARBINARY",
    "BIT",
    "GEOMETRY",
    "POINT",
    "JSON",
    "TIMESTAMP",
    "VECTOR",
})


def _mysql_base(declared: str) -> str:
    raw = (declared or "").strip().upper()
    return raw.split("(")[0].strip()


def mysql_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper()
    if not raw:
        return False
    base = _mysql_base(declared)
    if base in _UNSAFE_MYSQL_BASES:
        return False
    return True


def _mysql_ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    return quote_sql_identifier(name, "`")


def _pg_ident(name: str) -> str:
    return _quote(name)


def _pg_create_sql(
    schema: str,
    table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    primary_key: list[str],
) -> str:
    dest_ref = _table_ref(schema, table)
    cols: list[str] = []
    targets = [t for _s, t in pairs]
    for (_source, target), ddl in zip(pairs, pg_ddls):
        cols.append(f"{_pg_ident(target)} {ddl}")
    pk = [c for c in primary_key if c in targets]
    if pk:
        pk_sql = ", ".join(_pg_ident(c) for c in pk)
        cols.append(f"PRIMARY KEY ({pk_sql})")
    return f"CREATE TABLE {dest_ref} ({', '.join(cols)})"


def _mysql_table_pk_and_types(
    cur: Any, table: str, columns: list[str]
) -> tuple[list[str], dict[str, str]]:
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        (table,),
    )
    types = {str(n): str(t or "") for n, t in cur.fetchall()}
    live_l = {k.lower(): v for k, v in types.items()}
    missing = [c for c in columns if c.lower() not in live_l]
    if missing:
        raise FastPathUnavailable(f"source column {missing[0]!r} absent")
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
        "AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
        (table,),
    )
    pk = [str(r[0]) for r in cur.fetchall()]
    return pk, types


def _select_sql(table_q: str, source_cols: list[str], predicate: str) -> str:
    cols = ", ".join(_mysql_ident(c) for c in source_cols)
    where = f" WHERE {predicate}" if predicate else ""
    return f"SELECT {cols} FROM {table_q}{where}"  # nosec B608


def _escape_copy_field(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def fast_copy_text_value(value: object) -> str:
    """COPY-text for the MySQL identity hot types; else canonical encoder.

    Equivalent to ``_copy_text_value`` for None, ``int``, ``str``, ``date``,
    ``datetime``, and ``float``. Avoids the per-cell import / isinstance chain
    and skips four ``str.replace`` calls when the string has no COPY metachars.
    """
    if value is None:
        return "\\N"
    t = type(value)
    if t is int:
        return str(value)
    if t is str:
        if "\\" not in value and "\t" not in value and "\n" not in value and "\r" not in value:
            return value
        return _escape_copy_field(value)
    if t is float:
        return str(value)
    if t is datetime.datetime or t is datetime.date:
        return str(value)
    from connectors.postgresql_writer import _copy_text_value

    return _copy_text_value(value)


def _fifo_mysql_into_pg(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    dest_ref: str,
    columns: list[str],
) -> None:
    col_list = ", ".join(_pg_ident(c) for c in columns)
    copy_sql = (
        f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
        "(FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    )

    def _produce(path: str) -> None:
        from pymysql.cursors import SSCursor

        encode = fast_copy_text_value
        join = "\t".join
        with source_conn.cursor(SSCursor) as stream:
            stream.execute(select_sql)
            with open(path, "wb", buffering=_PIPE_CHUNK) as writer:
                while True:
                    batch = stream.fetchmany(_FETCH_BATCH)
                    if not batch:
                        break
                    payload = "\n".join(join(encode(v) for v in row) for row in batch)
                    writer.write((payload + "\n").encode("utf-8"))

    def _consume(path: str) -> None:
        with open(path, "rb", buffering=_PIPE_CHUNK) as reader:
            dst_cur.copy_expert(copy_sql, reader)

    stream_between_cursors(
        prefix="df_mysql_pg_", producer=_produce, consumer=_consume
    )


def _mysql_connect(cfg: dict[str, Any]) -> Any:
    from connectors.mysql_conn import get_connection as mysql_connect

    conn = mysql_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 3306),
        database=cfg.get("database", ""),
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
        purpose="write",
    )
    conn.autocommit = False
    return conn


def _pg_connect(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection as pg_connect

    conn = pg_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database") or cfg.get("dbname") or "",
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
    conn.autocommit = False
    return conn


def fetch_mysql_pk_interior_cuts(
    cur: Any, table_q: str, pk_ident: str, workers: int
) -> list[Any]:
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    cur.execute(
        f"SELECT COUNT(*) FROM {table_q} WHERE {pk_ident} IS NOT NULL"  # nosec B608
    )
    total = int(cur.fetchone()[0])
    if total <= 1:
        return []
    cuts: list[Any] = []
    for i in range(1, n):
        off = max((i * total) // n, 1) - 1
        cur.execute(
            f"SELECT {pk_ident} FROM {table_q} WHERE {pk_ident} IS NOT NULL "  # nosec B608
            f"ORDER BY {pk_ident} LIMIT 1 OFFSET %s",
            (off,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            cuts.append(row[0])
    return cuts


def _plan_pk_partitions(
    src_cur: Any,
    table_q: str,
    src_ident: str,
    pk_declared: str,
    n_parts: int,
    source_count: int,
) -> list[dict[str, Any]]:
    if n_parts <= 1:
        key_ranges: list[tuple[Any | None, Any | None]] = [(None, None)]
    elif _mysql_base(pk_declared) in _INTEGER_PK_BASES:
        src_cur.execute(
            f"SELECT MIN({src_ident}), MAX({src_ident}) FROM {table_q} "  # nosec B608
            f"WHERE {src_ident} IS NOT NULL"
        )
        row = src_cur.fetchone()
        cuts = (
            integer_pk_cuts(int(row[0]), int(row[1]), n_parts)
            if row and row[0] is not None and row[1] is not None
            else []
        )
        key_ranges = key_ranges_from_cuts(cuts)
    else:
        cuts = fetch_mysql_pk_interior_cuts(src_cur, table_q, src_ident, n_parts)
        key_ranges = key_ranges_from_cuts(cuts)
    src_cur.execute(
        f"SELECT COUNT(*) FROM {table_q} WHERE {src_ident} IS NULL"  # nosec B608
    )
    nulls = int(src_cur.fetchone()[0])
    unbounded = len(key_ranges) == 1 and key_ranges[0] == (None, None)
    plan: list[tuple[str, Any, Any, bool]] = []
    if nulls and not unbounded:
        plan.append((f"{src_ident} IS NULL", None, None, True))
    for lo, hi in key_ranges:
        clause, params = mysql_pk_range_clause(src_ident, lo, hi)
        pred = src_cur.mogrify(clause, params)
        if isinstance(pred, bytes):
            pred = pred.decode()
        plan.append((str(pred), lo, hi, False))
    partitions: list[dict[str, Any]] = []
    for pred, lo, hi, is_null in plan:
        if pred and pred != "1=1":
            src_cur.execute(f"SELECT COUNT(*) FROM {table_q} WHERE {pred}")  # nosec B608
        else:
            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            pred = ""
        expected = int(src_cur.fetchone()[0])
        partitions.append({
            "lo": lo,
            "hi": hi,
            "null_shard": is_null,
            "source_count": expected,
            "predicate": pred,
            "action": "load",
        })
    accounted = sum(int(p["source_count"]) for p in partitions)
    if accounted != source_count:
        raise ValueError(
            f"PK range source COUNTs {accounted} != snapshot {source_count}"
        )
    return partitions


def _pg_range_count(
    cur: Any, dest_ref: str, dest_ident: str, part: dict[str, Any]
) -> int:
    if part.get("null_shard"):
        pred = f"{dest_ident} IS NULL"
        cur.execute(f"SELECT COUNT(*) FROM {dest_ref} WHERE {pred}")  # nosec B608
        return int(cur.fetchone()[0])
    lo_sql = (
        _pg_quoted_literal(cur, part["lo"]) if part.get("lo") is not None else None
    )
    hi_sql = (
        _pg_quoted_literal(cur, part["hi"]) if part.get("hi") is not None else None
    )
    pred = pk_range_predicate(dest_ident, lo_sql, hi_sql)
    if not pred:
        cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
    else:
        cur.execute(f"SELECT COUNT(*) FROM {dest_ref} WHERE {pred}")  # nosec B608
    return int(cur.fetchone()[0])


def copy_mysql_to_postgres(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    replace_destination: bool,
    source_where: str = "",
) -> FastPathResult:
    """Stream MySQL rows into PostgreSQL COPY. Dest COUNT is the proof.

    ``source_where`` is a pre-quoted SQL fragment (incremental cursor predicate).
    When set, COUNT and SELECT use that filter, dest-occupied PK skip is
    disabled, and the load is a single shard.
    """
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    table_q = _mysql_ident(source_table)
    dest_ref = _table_ref(dest_schema, dest_table)

    source_conn = _mysql_connect(source_cfg)
    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    existed_before = False
    pk_map: tuple[str, str] | None = None
    try:
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
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

            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s LIMIT 1",
                (dest_schema or "public", dest_table),
            )
            exists = dst_cur.fetchone() is not None
            existed_before = bool(exists)
            dest_occupied = False
            if replace_destination and exists:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                exists = False
            if exists:
                dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                dest_occupied = int(dst_cur.fetchone()[0]) > 0
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty PostgreSQL dest stays on the row path"
                    )
            else:
                pk = [
                    rename
                    for src_pk in pk_cols
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                dst_cur.execute(
                    _pg_create_sql(dest_schema, dest_table, pairs, pg_ddls, pk)
                )
                created_here = True
                dest_conn.commit()

            cursor_where = (source_where or "").strip()
            where_sql = f" WHERE {cursor_where}" if cursor_where else ""
            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}{where_sql}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            partitions: list[dict[str, Any]] = []
            shard_mode = "ctid"
            select_sql = _select_sql(table_q, source_cols, cursor_where)

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
                    src_cur, table_q, src_ident, pk_declared, n_parts, source_count
                )
                if dest_occupied:
                    dest_ident = _pg_ident(dest_pk)
                    dest_conn.commit()
                    to_copy: list[str] = []
                    for part in partitions:
                        already = _pg_range_count(dst_cur, dest_ref, dest_ident, part)
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                            to_copy.append(str(part.get("predicate") or ""))
                        else:
                            pred = pk_range_predicate(
                                dest_ident,
                                _pg_quoted_literal(dst_cur, part["lo"])
                                if part.get("lo") is not None
                                else None,
                                _pg_quoted_literal(dst_cur, part["hi"])
                                if part.get("hi") is not None
                                else None,
                                null_shard=bool(part.get("null_shard")),
                            )
                            if not pred:
                                raise FastPathUnavailable("refusing unbounded dest DELETE on resume")
                            dst_cur.execute(
                                f"DELETE FROM {dest_ref} WHERE {pred}"  # nosec B608
                            )
                            part["action"] = "reload"
                            to_copy.append(str(part.get("predicate") or ""))
                    dest_conn.commit()
                    if not to_copy:
                        select_sql = ""
                    elif len(to_copy) == 1:
                        select_sql = _select_sql(table_q, source_cols, to_copy[0])
                    else:
                        combined = " OR ".join(f"({p})" for p in to_copy if p)
                        select_sql = _select_sql(table_q, source_cols, combined)

            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty PostgreSQL dest stays on the row path"
                )

            src_cur.close()
            if select_sql:
                _fifo_mysql_into_pg(
                    source_conn,
                    dst_cur,
                    select_sql=select_sql,
                    dest_ref=dest_ref,
                    columns=target_cols,
                )
                dest_conn.commit()

            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            if dest_count != source_count:
                raise ValueError(
                    "MySQL→PG COPY refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _pg_ident(pk_map[1])
                dest_conn.commit()
                for part in partitions:
                    dest_part = _pg_range_count(dst_cur, dest_ref, dest_ident, part)
                    part["dest_count"] = dest_part
                    if dest_part != int(part["source_count"]):
                        raise ValueError(
                            "PK range dest COUNT "
                            f"{dest_part} != source {part['source_count']} "
                            f"(lo={part['lo']!r} hi={part['hi']!r})"
                        )
            dest_conn.commit()
            source_conn.commit()
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
                    "copy_workers": 1,
                    "copy_split": "cursor" if cursor_where else "serial",
                    "copy_partitions": len(partitions) or 1,
                    "partitions_skipped": sum(
                        1 for p in partitions if p.get("action") == "skip"
                    ),
                    "shard_mode": shard_mode if partitions else "serial",
                    "tsv_encoder": "fast_copy_text",
                    "source_where": bool(cursor_where),
                    "partition_proof": partition_proof,
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
                    cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and pk_map is None:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
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
            logger.debug("pg dest close skipped", exc_info=True)
