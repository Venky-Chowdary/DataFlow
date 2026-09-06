"""PostgreSQL COPY text → MySQL STRICT LOAD DATA (cross-engine bulk).

Same-engine PG→PG already uses binary COPY in ``copy_fast_path``. Cross-engine
cannot use that wire. This path streams ``COPY (SELECT …) TO STDOUT`` text
(tab / ``\\N``) into ``LOAD DATA LOCAL INFILE`` under STRICT sql_mode.

Python never materializes a row. Dest ``COUNT(*)`` in the same operator
proof must equal the source snapshot count. Warning/Error from LOAD DATA
rolls the destination back and raises — never silent coerce.

Large tables overlap COPY and LOAD DATA on a FIFO (no full tempfile).
A mapped single PK is the dest-COUNT proof (integer: Spark-style min/max
cuts; else ``percentile_disc``). An **empty** dest COPYs by ``ctid`` heap
page ranges (sequential I/O). A **non-empty** dest resumes by PK range
(skip complete, DELETE+reload partial). No mapped single PK: ctid COPY
and total dest COUNT only. Workers share ``pg_export_snapshot()``. A
missed PK range fails dest COUNT.

PK partitions are a restartable job: a range whose dest COUNT already
equals the source snapshot is skipped; a partial range is deleted and
reloaded; a disjoint range may LOAD into a dest that already holds other
keys. ctid shards still refuse non-empty append (cannot COUNT dest by
ctid). Failure in PK mode leaves dest for resume — it does not TRUNCATE
completed ranges.

Declines (row path keeps quarantine): transforms that change values, jsonb,
bytea, timestamptz, arrays, non-empty ctid append, missing local_infile.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    _quote,
    _table_ref,
    require_fifo_streaming,
)
from services.engine_checksum import _NO_OP_TYPE_TRANSFORMS

logger = logging.getLogger(__name__)

_SAFE_PG_BASES = frozenset({
    "SMALLINT",
    "INT2",
    "INTEGER",
    "INT",
    "INT4",
    "BIGINT",
    "INT8",
    "NUMERIC",
    "DECIMAL",
    "REAL",
    "FLOAT4",
    "DOUBLE",
    "FLOAT8",
    "FLOAT",
    "VARCHAR",
    "CHARACTER VARYING",
    "CHAR",
    "CHARACTER",
    "BPCHAR",
    "TEXT",
    "CITEXT",
    "DATE",
    "TIMESTAMP",
    "DATETIME",
    "BOOLEAN",
    "BOOL",
})

_PIPE_CHUNK = 1 << 22
_MAX_WORKERS = 32
_MAX_PARTITIONS = 32
_AUTO_PARALLEL_ROWS = 50_000
_PARALLEL_8_ROWS = 1_000_000
_TARGET_ROWS_PER_PARTITION = 1_000_000
_INTEGER_PK_BASES = frozenset({
    "SMALLINT",
    "INT2",
    "INTEGER",
    "INT",
    "INT4",
    "BIGINT",
    "INT8",
})


def _pg_base(declared: str) -> str:
    from connectors.sql_temporal import sql_base_type

    return sql_base_type(declared)


def mapping_is_plain_carry(mappings: list[dict]) -> tuple[bool, str]:
    if not mappings:
        return False, "no mappings"
    for item in mappings:
        transform = str(item.get("transform") or "none").strip().lower()
        if transform not in _NO_OP_TYPE_TRANSFORMS:
            return False, f"transform {transform!r} changes values"
        source = str(item.get("source") or "").strip()
        target = str(item.get("target") or "").strip()
        if not source or not target:
            return False, "mapping missing source/target"
    return True, ""


def pg_type_is_load_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper()
    if not raw or "[" in raw or raw.endswith("[]"):
        return False
    base = _pg_base(declared)
    if base in {"BYTEA", "JSON", "JSONB", "TIMESTAMPTZ", "VECTOR", "UUID", "XML"}:
        return False
    if "TIME ZONE" in raw and "WITHOUT" not in raw:
        return False
    return base in _SAFE_PG_BASES or raw.split("(")[0].strip() in _SAFE_PG_BASES


def heap_page_ranges(relpages: int, workers: int) -> list[tuple[int, int | None]]:
    """Disjoint ``[lo, hi)`` heap page ranges. Last shard is unbounded (``hi=None``)."""
    pages = max(int(relpages or 0), 0)
    n = max(1, min(int(workers or 1), _MAX_WORKERS))
    if pages <= 1:
        return [(0, None)]
    n = min(n, pages)
    size = max(pages // n, 1)
    ranges: list[tuple[int, int | None]] = []
    lo = 0
    for i in range(n):
        if i == n - 1:
            ranges.append((lo, None))
            break
        hi = lo + size
        ranges.append((lo, hi))
        lo = hi
    return ranges


def ctid_predicate(lo_page: int, hi_page: int | None) -> str:
    """Heap-page filter. Empty string means the whole table (one worker)."""
    if lo_page <= 0 and hi_page is None:
        return ""
    if hi_page is None:
        return f"ctid >= '({int(lo_page)},1)'::tid"
    if lo_page <= 0:
        return f"ctid < '({int(hi_page)},1)'::tid"
    return (
        f"ctid >= '({int(lo_page)},1)'::tid AND ctid < '({int(hi_page)},1)'::tid"
    )


def key_ranges_from_cuts(cuts: list[Any]) -> list[tuple[Any | None, Any | None]]:
    """Equal-height ranges from interior cuts: ``[None, c0), [c0, c1), … [cN, None)``."""
    uniq: list[Any] = []
    for cut in cuts:
        if cut is None:
            continue
        if not uniq or uniq[-1] != cut:
            uniq.append(cut)
    if not uniq:
        return [(None, None)]
    ranges: list[tuple[Any | None, Any | None]] = [(None, uniq[0])]
    for i in range(len(uniq) - 1):
        ranges.append((uniq[i], uniq[i + 1]))
    ranges.append((uniq[-1], None))
    return ranges


def mapped_single_pk(
    source_pk_columns: list[str],
    pairs: list[tuple[str, str]],
) -> tuple[str, str] | None:
    """``(source_pk, dest_pk)`` when exactly one source PK column is mapped."""
    pks = [str(c) for c in (source_pk_columns or []) if str(c).strip()]
    if len(pks) != 1:
        return None
    want = pks[0].lower()
    for source_col, dest_col in pairs:
        if source_col.lower() == want:
            return source_col, dest_col
    return None


def pg_mysql_copy_workers(source_count: int) -> int:
    """Operator cap. ``auto`` uses 4 workers at ≥50k and 8 at ≥1M."""
    raw = (getenv_brand("PG_MYSQL_COPY_WORKERS", "auto") or "auto").strip().lower()
    cpus = os.cpu_count() or 4
    if raw in {"auto", ""}:
        n = int(source_count or 0)
        if n >= _PARALLEL_8_ROWS:
            return min(8, cpus)
        if n >= _AUTO_PARALLEL_ROWS:
            return min(4, cpus)
        return 1
    try:
        return max(1, min(int(raw), _MAX_WORKERS))
    except ValueError:
        return 1


def pg_mysql_copy_partitions(source_count: int, workers: int) -> int:
    """How many PK ranges to plan. At ≥1M, ~1M rows each, capped at 32.

    Waves of ``workers`` run those ranges. More partitions than CPUs is how a
    200M table becomes a resume-granular job on a 4-core box.
    """
    w = max(1, int(workers or 1))
    n = int(source_count or 0)
    if w <= 1:
        return 1
    if n >= _TARGET_ROWS_PER_PARTITION:
        aimed = max(w, n // _TARGET_ROWS_PER_PARTITION)
        return max(1, min(_MAX_PARTITIONS, aimed))
    return min(w, _MAX_PARTITIONS)


def integer_pk_cuts(lo: int, hi: int, workers: int) -> list[int]:
    """Interior cuts on a closed integer interval ``[lo, hi]`` (Spark JDBC style)."""
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    width = int(hi) - int(lo) + 1
    if width <= 1:
        return []
    n = min(n, width)
    cuts: list[int] = []
    for i in range(1, n):
        cut = int(lo) + (i * width) // n
        if cut <= int(lo) or cut > int(hi):
            continue
        if not cuts or cut != cuts[-1]:
            cuts.append(cut)
    return cuts


def fetch_integer_pk_cuts(
    cur: Any, source_ref: str, pk_ident: str, workers: int
) -> list[int]:
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    cur.execute(
        f"SELECT min({pk_ident}), max({pk_ident}) "  # nosec B608
        f"FROM {source_ref} WHERE {pk_ident} IS NOT NULL"
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        return []
    return integer_pk_cuts(int(row[0]), int(row[1]), n)


def _pg_quoted_literal(cur: Any, value: Any) -> str:
    quoted = cur.mogrify("%s", (value,))
    if isinstance(quoted, bytes):
        return quoted.decode()
    return str(quoted)


def pk_range_predicate(ident: str, lo: Any, hi: Any, *, null_shard: bool = False) -> str:
    if null_shard:
        return f"{ident} IS NULL"
    parts: list[str] = []
    if lo is not None:
        parts.append(f"{ident} >= {lo}")
    if hi is not None:
        parts.append(f"{ident} < {hi}")
    return " AND ".join(parts)


def mysql_pk_range_clause(
    ident: str, lo: Any, hi: Any, *, null_shard: bool = False
) -> tuple[str, list[Any]]:
    if null_shard:
        return f"{ident} IS NULL", []
    parts: list[str] = []
    params: list[Any] = []
    if lo is not None:
        parts.append(f"{ident} >= %s")
        params.append(lo)
    if hi is not None:
        parts.append(f"{ident} < %s")
        params.append(hi)
    if not parts:
        return "1=1", []
    return " AND ".join(parts), params


def _mysql_range_count(
    cur: Any,
    table_q: str,
    dest_ident: str,
    part: dict[str, Any],
) -> int:
    clause, params = mysql_pk_range_clause(
        dest_ident,
        part.get("lo"),
        part.get("hi"),
        null_shard=bool(part.get("null_shard")),
    )
    cur.execute(
        f"SELECT COUNT(*) FROM {table_q} WHERE {clause}",  # nosec B608
        params,
    )
    return int(cur.fetchone()[0])


def _delete_mysql_range(
    cur: Any,
    table_q: str,
    dest_ident: str,
    part: dict[str, Any],
) -> None:
    clause, params = mysql_pk_range_clause(
        dest_ident,
        part.get("lo"),
        part.get("hi"),
        null_shard=bool(part.get("null_shard")),
    )
    cur.execute(
        f"DELETE FROM {table_q} WHERE {clause}",  # nosec B608
        params,
    )


def fetch_pk_interior_cuts(
    cur: Any, source_ref: str, pk_ident: str, workers: int
) -> list[Any]:
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    # Fractions are computed from worker count, not user input.
    frac_sql = ",".join(f"{i / n:.10g}" for i in range(1, n))
    cur.execute(
        f"SELECT percentile_disc(ARRAY[{frac_sql}]::double precision[]) "  # nosec B608
        f"WITHIN GROUP (ORDER BY {pk_ident}) FROM {source_ref}"  # nosec B608
    )
    row = cur.fetchone()
    raw = row[0] if row else None
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    text = str(raw).strip()
    if text.startswith("{") and text.endswith("}"):
        inner = text[1:-1]
        if not inner:
            return []
        return [part.strip() for part in inner.split(",") if part.strip()]
    return [raw]


def _jsonable_bound(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _pg_copy_select_expr(column: str, declared: str) -> str:
    """Project a PG column as MySQL LOAD DATA text (bool → 0/1)."""
    ident = _quote(column)
    base = _pg_base(declared)
    if base in {"BOOLEAN", "BOOL"}:
        return f"CASE WHEN {ident} THEN 1 WHEN NOT {ident} THEN 0 ELSE NULL END"
    return ident


def _mysql_ident(name: str) -> str:
    from connectors.writer_common import quote_sql_identifier

    return quote_sql_identifier(name, "`")


def _mysql_create_sql(
    table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    primary_key: list[str],
) -> str:
    cols: list[str] = []
    targets = [t for _s, t in pairs]
    for (_source, target), ddl in zip(pairs, mysql_ddls):
        cols.append(f"{_mysql_ident(target)} {ddl}")
    pk = [c for c in primary_key if c in targets]
    if pk:
        pk_sql = ", ".join(_mysql_ident(c) for c in pk)
        cols.append(f"PRIMARY KEY ({pk_sql})")
    return f"CREATE TABLE {_mysql_ident(table)} ({', '.join(cols)})"


def _copy_select_sql(select_list: str, source_ref: str, predicate: str) -> str:
    where = f" WHERE {predicate}" if predicate else ""
    return (
        f"COPY (SELECT {select_list} FROM {source_ref}{where}) "  # nosec B608
        "TO STDOUT WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    )


def _fifo_copy_into_mysql(
    src_cur: Any,
    dst_cur: Any,
    *,
    copy_sql: str,
    table_q: str,
    columns: list[str],
) -> None:
    """Overlap PG COPY writes with MySQL LOCAL INFILE reads. No full tempfile."""
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        quote_load_data_path,
    )

    tmp = tempfile.mkdtemp(prefix="df_pg_mysql_")
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
            with open(path, "wb", buffering=_PIPE_CHUNK) as writer:
                src_cur.copy_expert(copy_sql, writer)
        except BaseException as exc:  # noqa: BLE001 — re-raised on the caller
            failure.append(exc)

    pump = threading.Thread(target=_pump, name="pg-mysql-copy-fifo", daemon=True)
    pump.start()
    try:
        from connectors.mysql_load_data import (
            apply_mysql_bulk_load_session,
            restore_mysql_bulk_load_session,
        )

        apply_mysql_bulk_load_session(dst_cur)
        try:
            dst_cur.execute(load_sql)
            dst_cur.execute("SHOW WARNINGS")
        finally:
            restore_mysql_bulk_load_session(dst_cur)
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


def _pg_connect(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection as pg_connect

    return pg_connect(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database") or cfg.get("dbname") or "",
        username=cfg.get("username") or cfg.get("user") or "",
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
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


def _heap_relpages(cur: Any, schema: str, table: str) -> int:
    cur.execute(
        """
        SELECT c.relpages
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema or "public", table),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _run_shard(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    snapshot_id: str,
    copy_sql: str,
    table_q: str,
    columns: list[str],
) -> None:
    from connectors.mysql_load_data import mysql_load_data_session_ready

    src = _pg_connect(source_cfg)
    dst = _mysql_connect(dest_cfg)
    try:
        src.autocommit = False
        with src.cursor() as src_cur, dst.cursor() as dst_cur:
            src_cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            src_cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_id,))
            ready, why = mysql_load_data_session_ready(dst_cur, dst)
            if not ready:
                raise FastPathUnavailable(why)
            _fifo_copy_into_mysql(
                src_cur,
                dst_cur,
                copy_sql=copy_sql,
                table_q=table_q,
                columns=columns,
            )
            dst.commit()
            src.commit()
    except Exception:
        try:
            dst.rollback()
        except Exception:
            logger.debug("shard mysql rollback skipped", exc_info=True)
        raise
    finally:
        try:
            src.close()
        except Exception:
            logger.debug("shard pg close skipped", exc_info=True)
        try:
            dst.close()
        except Exception:
            logger.debug("shard mysql close skipped", exc_info=True)


def _launch_copy_shards(
    *,
    copy_sqls: list[str],
    src_cur: Any,
    dst_cur: Any,
    dest_conn: Any,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    snapshot_id: str,
    table_q: str,
    target_cols: list[str],
    max_parallel: int = 1,
) -> None:
    if not copy_sqls:
        return
    if len(copy_sqls) == 1:
        _fifo_copy_into_mysql(
            src_cur,
            dst_cur,
            copy_sql=copy_sqls[0],
            table_q=table_q,
            columns=target_cols,
        )
        dest_conn.commit()
        return
    parallel = max(1, min(int(max_parallel or len(copy_sqls)), _MAX_WORKERS))
    errors: list[BaseException] = []
    for offset in range(0, len(copy_sqls), parallel):
        batch = copy_sqls[offset:offset + parallel]
        threads: list[threading.Thread] = []
        for sql in batch:
            t = threading.Thread(
                target=_shard_thread,
                kwargs={
                    "source_cfg": source_cfg,
                    "dest_cfg": dest_cfg,
                    "snapshot_id": snapshot_id,
                    "copy_sql": sql,
                    "table_q": table_q,
                    "columns": target_cols,
                    "errors": errors,
                },
                daemon=True,
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]


def copy_postgres_to_mysql(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    replace_destination: bool,
    source_where: str = "",
) -> FastPathResult:
    """COPY text from PostgreSQL into MySQL LOAD DATA. Dest COUNT is the proof.

    ``source_where`` is a pre-quoted SQL fragment (incremental cursor predicate).
    When set, COUNT and COPY use that filter, dest-occupied PK skip is disabled,
    and the load is a single shard — PK-ranging a filtered subset would miss
    rows whose keys sit outside the planned ranges.
    """
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    require_fifo_streaming("PG→MySQL")

    from connectors.mysql_load_data import mysql_load_data_session_ready
    from connectors.write_resilience import is_public_proxy_host
    from services.copy_fast_path import source_column_types, source_table_shape

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: LOCAL INFILE not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _table_ref(source_schema, source_table)
    table_q = _mysql_ident(dest_table)

    source_conn = _pg_connect(source_cfg)
    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    existed_before = False
    preserve_dest_on_failure = False
    try:
        source_conn.autocommit = False
        with source_conn.cursor() as src_cur, dest_conn.cursor() as dst_cur:
            src_cur.execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            live = source_column_types(
                src_cur, source_schema, source_table, source_cols
            )
            live_l = {k.lower(): v for k, v in live.items()}
            for col in source_cols:
                declared = live_l.get(col.lower())
                if not declared:
                    raise FastPathUnavailable(f"source column {col!r} absent")
                if not pg_type_is_load_safe(declared):
                    raise FastPathUnavailable(
                        f"source column {col!r} type {declared} is not LOAD DATA safe"
                    )
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)
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
                dst_cur.execute(f"DROP TABLE IF EXISTS {table_q}")  # nosec B608
                exists = False
            if exists:
                dst_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
                dest_occupied = int(dst_cur.fetchone()[0]) > 0
                if dest_occupied and pk_map is None:
                    raise FastPathUnavailable(
                        "append into non-empty MySQL dest stays on the row path "
                        "(ctid shards cannot prove dest COUNT per range)"
                    )
            else:
                pk = [
                    rename
                    for src_pk in shape.primary_key
                    for src_col, rename in pairs
                    if src_col.lower() == src_pk.lower()
                ]
                create_sql = _mysql_create_sql(dest_table, pairs, mysql_ddls, pk)
                dst_cur.execute(create_sql)  # nosec B608
                created_here = True
                dest_conn.commit()

            cursor_where = (source_where or "").strip()
            where_sql = f" WHERE {cursor_where}" if cursor_where else ""
            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}{where_sql}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            src_cur.execute("SELECT pg_export_snapshot()")
            snapshot_id = str(src_cur.fetchone()[0])
            workers = 1 if cursor_where else pg_mysql_copy_workers(source_count)
            n_parts = 1 if cursor_where else pg_mysql_copy_partitions(source_count, workers)
            select_list = ", ".join(
                _pg_copy_select_expr(col, live_l[col.lower()]) for col in source_cols
            )
            copy_sqls: list[str] = []
            partitions: list[dict[str, Any]] = []
            shard_mode = "ctid"
            copy_split = "ctid"

            if cursor_where:
                if dest_occupied:
                    raise FastPathUnavailable(
                        "filtered COPY into occupied dest stays on the incremental staging path"
                    )
                shard_mode = "cursor"
                copy_split = "cursor"
                copy_sqls = [
                    _copy_select_sql(select_list, source_ref, cursor_where)
                ]
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
                src_ident = _quote(src_pk)
                shard_mode = "pk"
                preserve_dest_on_failure = True
                if n_parts <= 1:
                    key_ranges: list[tuple[Any | None, Any | None]] = [(None, None)]
                else:
                    pk_declared = live_l.get(src_pk.lower()) or ""
                    if _pg_base(pk_declared) in _INTEGER_PK_BASES:
                        cuts = fetch_integer_pk_cuts(
                            src_cur, source_ref, src_ident, n_parts
                        )
                    else:
                        cuts = fetch_pk_interior_cuts(
                            src_cur, source_ref, src_ident, n_parts
                        )
                    key_ranges = key_ranges_from_cuts(cuts)
                src_cur.execute(
                    f"SELECT COUNT(*) FROM {source_ref} WHERE {src_ident} IS NULL"  # nosec B608
                )
                nulls = int(src_cur.fetchone()[0])
                unbounded = (
                    len(key_ranges) == 1 and key_ranges[0] == (None, None)
                )
                plan: list[tuple[str, Any, Any, bool]] = []
                if nulls and not unbounded:
                    plan.append((f"{src_ident} IS NULL", None, None, True))
                for lo, hi in key_ranges:
                    lo_sql = (
                        _pg_quoted_literal(src_cur, lo) if lo is not None else None
                    )
                    hi_sql = (
                        _pg_quoted_literal(src_cur, hi) if hi is not None else None
                    )
                    pred = pk_range_predicate(src_ident, lo_sql, hi_sql)
                    plan.append((pred, lo, hi, False))
                for pred, lo, hi, is_null in plan:
                    if pred:
                        src_cur.execute(
                            f"SELECT COUNT(*) FROM {source_ref} WHERE {pred}"  # nosec B608
                        )
                    else:
                        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
                    expected = int(src_cur.fetchone()[0])
                    partitions.append({
                        "lo": lo,
                        "hi": hi,
                        "null_shard": is_null,
                        "source_count": expected,
                        "dest_pk": dest_pk,
                        "predicate": pred,
                        "action": "load",
                    })
                accounted = sum(int(p["source_count"]) for p in partitions)
                if accounted != source_count:
                    raise ValueError(
                        "PK range source COUNTs "
                        f"{accounted} != snapshot {source_count}"
                    )
                dest_ident = _mysql_ident(dest_pk)
                if dest_occupied:
                    copy_split = "pk"
                    dest_conn.commit()
                    for part in partitions:
                        already = _mysql_range_count(
                            dst_cur, table_q, dest_ident, part
                        )
                        expected = int(part["source_count"])
                        if already == expected:
                            part["action"] = "skip"
                            part["dest_count"] = already
                        elif already == 0:
                            part["action"] = "load"
                        else:
                            _delete_mysql_range(
                                dst_cur, table_q, dest_ident, part
                            )
                            part["action"] = "reload"
                    dest_conn.commit()
                    copy_sqls = [
                        _copy_select_sql(
                            select_list, source_ref, str(p.get("predicate") or "")
                        )
                        for p in partitions
                        if p.get("action") in {"load", "reload"}
                    ]
                else:
                    # Sequential heap COPY; PK ranges are dest-COUNT proof only.
                    copy_split = "ctid"
                    relpages = _heap_relpages(src_cur, source_schema, source_table)
                    page_ranges = heap_page_ranges(relpages, workers)
                    copy_sqls = [
                        _copy_select_sql(
                            select_list, source_ref, ctid_predicate(*lo_hi)
                        )
                        for lo_hi in page_ranges
                    ]
            else:
                if dest_occupied:
                    raise FastPathUnavailable(
                        "append into non-empty MySQL dest stays on the row path"
                    )
                relpages = _heap_relpages(src_cur, source_schema, source_table)
                page_ranges = heap_page_ranges(relpages, workers)
                copy_sqls = [
                    _copy_select_sql(
                        select_list, source_ref, ctid_predicate(*lo_hi)
                    )
                    for lo_hi in page_ranges
                ]

            _launch_copy_shards(
                copy_sqls=copy_sqls,
                src_cur=src_cur,
                dst_cur=dst_cur,
                dest_conn=dest_conn,
                source_cfg=source_cfg,
                dest_cfg=dest_cfg,
                snapshot_id=snapshot_id,
                table_q=table_q,
                target_cols=target_cols,
                max_parallel=workers,
            )

            dst_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            if dest_count != source_count:
                raise ValueError(
                    "PG→MySQL COPY refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _mysql_ident(pk_map[1])
                dest_conn.commit()
                for part in partitions:
                    dest_part = _mysql_range_count(
                        dst_cur, table_q, dest_ident, part
                    )
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
            skipped = sum(1 for p in partitions if p.get("action") == "skip")
            proof_scope = (
                "filtered_dest_count_equals_source_snapshot"
                if cursor_where
                else (
                    "partition_dest_count_equals_source_snapshot"
                    if partition_proof
                    else "dest_count_equals_source_snapshot_count"
                )
            )
            return FastPathResult(
                rows_copied=dest_count,
                source_rows=source_count,
                source_checksum=proof,
                target_rows=dest_count,
                target_checksum=proof,
                source_snapshot={
                    "pg_snapshot": snapshot_id,
                    "copy_workers": workers,
                    "copy_partitions": max(len(partitions), len(copy_sqls)),
                    "partitions_skipped": skipped,
                    "partitions_loaded": len(copy_sqls),
                    "shard_mode": shard_mode,
                    "copy_split": copy_split,
                    "partition_proof": partition_proof,
                    "source_where": bool(cursor_where),
                },
                proof_scope=proof_scope,
            )
    except Exception:
        if preserve_dest_on_failure:
            raise
        if created_here:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {table_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql dest close skipped", exc_info=True)


def _shard_thread(
    *,
    source_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    snapshot_id: str,
    copy_sql: str,
    table_q: str,
    columns: list[str],
    errors: list[BaseException],
) -> None:
    try:
        _run_shard(
            source_cfg=source_cfg,
            dest_cfg=dest_cfg,
            snapshot_id=snapshot_id,
            copy_sql=copy_sql,
            table_q=table_q,
            columns=columns,
        )
    except BaseException as exc:  # noqa: BLE001 — collected; coordinator fails closed
        errors.append(exc)
