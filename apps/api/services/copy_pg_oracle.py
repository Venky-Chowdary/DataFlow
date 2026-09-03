"""PostgreSQL COPY text → Oracle executemany (cross-engine bulk).

Same-engine Oracle already uses INSERT SELECT. Cross-engine cannot.
This path streams ``COPY (SELECT …) TO STDOUT`` text (tab / ``\\N``),
decodes each field, and binds batches with ``oracledb.executemany``.
No client ``sqlldr`` / Data Pump on this host.

Oracle VARCHAR2 cannot store empty string: ``''`` IS NULL. That is
engine law, not a silent row drop. Empty-string cells are bound as
NULL and counted in ``empty_string_as_null_cells``. Rows still land;
dest ``COUNT(*)`` must equal the source snapshot.

Empty dest COPYs the table once (serial INSERT into a PK dest).
Occupied dest + mapped PK: skip complete ranges, DELETE+reload partial.
No mapped single PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
jsonb/bytea/timestamptz/arrays, public proxy, occupied dest without a
mapped single PK.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    _quote,
    _table_ref as _pg_table_ref,
    source_column_types,
    source_table_shape,
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
    _INTEGER_PK_BASES,
    _copy_select_sql,
    _jsonable_bound,
    _pg_base,
    _pg_connect,
    _pg_copy_select_expr,
    _pg_quoted_literal,
    fetch_integer_pk_cuts,
    fetch_pk_interior_cuts,
    key_ranges_from_cuts,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
    pg_type_is_load_safe,
    pk_range_predicate,
)
from services.copy_pg_sqlserver import (
    _CopyExecutemanySink,
    _as_date,
    _as_datetime,
    _as_float,
    _as_int,
    _identity,
    converter_for_ddl,
)

logger = logging.getLogger(__name__)

_VARCHAR2_BASES = frozenset({
    "VARCHAR2",
    "NVARCHAR2",
    "CHAR",
    "NCHAR",
    "CLOB",
    "NCLOB",
    "VARCHAR",
})


def pg_oracle_copy_enabled() -> bool:
    raw = (getenv_brand("PG_ORACLE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pg_oracle_copy_batch() -> int:
    raw = (getenv_brand("PG_ORACLE_COPY_BATCH", "50000") or "50000").strip()
    try:
        return max(1, min(int(raw), 100_000))
    except ValueError:
        return 50_000


def converter_for_ora_ddl(
    ddl: str, coerced: list[int]
) -> Callable[[str | None], Any]:
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in {"NUMBER", "INTEGER", "INT", "SMALLINT", "INT4", "INT8", "BIGINT"}:
        if "(" in (ddl or ""):
            inside = (ddl or "")[(ddl or "").find("(") + 1 : (ddl or "").find(")")]
            parts = [p.strip() for p in inside.split(",")]
            if len(parts) == 2 and parts[1] not in {"0", ""}:
                return _as_float
        return _as_int
    if base in {"FLOAT", "BINARY_FLOAT", "BINARYFLOAT", "BINARY_DOUBLE", "BINARYDOUBLE"}:
        return _as_float
    if base == "DATE":
        return _as_date
    if base.startswith("TIMESTAMP"):
        return _as_datetime
    inner = converter_for_ddl(ddl) if base not in _VARCHAR2_BASES else _identity
    if base not in _VARCHAR2_BASES:
        return inner

    def _varchar2(value: str | None) -> str | None:
        if value is None:
            return None
        if value == "":
            coerced[0] += 1
            return None
        return value

    return _varchar2


def _copy_into_oracle(
    src_cur: Any,
    dst_cur: Any,
    *,
    copy_sql: str,
    insert_sql: str,
    converters: list[Callable[[str | None], Any]],
) -> int:
    sink = _CopyExecutemanySink(
        dst_cur,
        insert_sql,
        converters,
        pg_oracle_copy_batch(),
        len(converters),
    )
    try:
        src_cur.copy_expert(copy_sql, sink)
    finally:
        sink.close()
    return sink.rows


def copy_postgres_to_oracle(
    *,
    source_cfg: dict[str, Any],
    source_schema: str,
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    oracle_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """COPY text from PostgreSQL into Oracle. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_oracle_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→Oracle COPY disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: Oracle bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _pg_table_ref(source_schema, source_table)
    dst_schema = _ora_schema_of(dest_cfg, dest_schema)
    dest_ref = _ora_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ora_ident(c) for c in target_cols)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(target_cols)))
    insert_sql = (
        f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    )
    coerced = [0]
    converters = [converter_for_ora_ddl(ddl, coerced) for ddl in oracle_ddls]

    source_conn = _pg_connect(source_cfg)
    dest_conn = _oracle_connect(dest_cfg)
    created_here = False
    existed_before = False
    pk_map: tuple[str, str] | None = None
    preserve_dest_on_failure = False
    try:
        source_conn.autocommit = False
        src_cur = source_conn.cursor()
        dst_cur = dest_conn.cursor()
        try:
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
                        f"source column {col!r} type {declared} is not COPY-text safe"
                    )
            shape = source_table_shape(
                src_cur, source_schema, source_table, source_cols
            )
            pk_map = mapped_single_pk(list(shape.primary_key or []), pairs)

            exists = _ora_table_exists(dst_cur, dst_schema, dest_table)
            existed_before = bool(exists)
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
                    for src_pk in shape.primary_key
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

            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])
            src_cur.execute("SELECT pg_export_snapshot()")
            snapshot_id = str(src_cur.fetchone()[0])
            workers = pg_mysql_copy_workers(source_count)
            n_parts = pg_mysql_copy_partitions(source_count, workers)
            select_list = ", ".join(
                _pg_copy_select_expr(col, live_l[col.lower()]) for col in source_cols
            )
            copy_sqls: list[str] = []
            partitions: list[dict[str, Any]] = []
            shard_mode = "serial"
            copy_split = "serial"

            if pk_map is not None:
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
                dest_ident = _ora_ident(dest_pk)
                if dest_occupied:
                    copy_split = "pk"
                    dest_conn.commit()
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
                        else:
                            _ora_delete_range(
                                dst_cur, dest_ref, dest_ident, part
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
                    copy_split = "serial"
                    copy_sqls = [_copy_select_sql(select_list, source_ref, "")]
            else:
                if dest_occupied:
                    raise FastPathUnavailable(
                        "append into non-empty Oracle dest stays on the row path"
                    )
                copy_sqls = [_copy_select_sql(select_list, source_ref, "")]

            for sql in copy_sqls:
                _copy_into_oracle(
                    src_cur,
                    dst_cur,
                    copy_sql=sql,
                    insert_sql=insert_sql,
                    converters=converters,
                )
                dest_conn.commit()

            dest_count = _ora_count(dst_cur, dest_ref)
            if dest_count != source_count:
                raise ValueError(
                    "PG→Oracle COPY refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            if shard_mode == "pk" and pk_map is not None:
                dest_ident = _ora_ident(pk_map[1])
                dest_conn.commit()
                for part in partitions:
                    dest_part = _ora_range_count(
                        dst_cur, dest_ref, dest_ident, part
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
            return FastPathResult(
                rows_copied=dest_count,
                source_rows=source_count,
                source_checksum=proof,
                target_rows=dest_count,
                target_checksum=proof,
                source_snapshot={
                    "pg_snapshot": snapshot_id,
                    "copy_workers": 1,
                    "copy_partitions": max(len(partitions), len(copy_sqls) or 1),
                    "partitions_skipped": skipped,
                    "partitions_loaded": len(copy_sqls),
                    "shard_mode": shard_mode,
                    "copy_split": copy_split,
                    "empty_string_as_null_cells": coerced[0],
                    "partition_proof": partition_proof,
                },
                proof_scope=(
                    "partition_dest_count_equals_source_snapshot"
                    if partition_proof
                    else "dest_count_equals_source_snapshot_count"
                ),
            )
        finally:
            try:
                src_cur.close()
            except Exception:
                logger.debug("pg source cursor close skipped", exc_info=True)
            try:
                dst_cur.close()
            except Exception:
                logger.debug("Oracle dest cursor close skipped", exc_info=True)
    except Exception:
        if preserve_dest_on_failure:
            raise
        if created_here:
            try:
                with dest_conn.cursor() as cur:
                    cur.execute(_ora_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("Oracle dest close skipped", exc_info=True)
