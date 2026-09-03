"""SQL Server SELECT → PostgreSQL COPY FROM STDIN (cross-engine bulk).

The reverse of ``copy_pg_sqlserver``. SQL Server has no ``COPY TO STDOUT``
and this host has no client ``bcp``. One HOLDLOCK (or SNAPSHOT, when the
database already allows it) transaction streams ``SELECT``; each cell is
encoded into PostgreSQL COPY text that ``COPY … FROM STDIN`` reads on
the same thread (``read()`` fetches the next SQL Server batch). Dest
``COUNT(*)`` must equal the source snapshot count.

Empty dest SELECTs the table once. Occupied dest with a mapped single PK
skips complete ranges and DELETE+reloads partial ones. No mapped single
PK on an occupied dest: decline.

Declines (row path keeps quarantine): transforms that change values,
varbinary/xml/geography/rowversion, public proxy, occupied dest without
a mapped single PK.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_mysql_pg import (
    _pg_connect,
    _pg_create_sql,
    _pg_range_count,
    fast_copy_text_value,
)
from services.copy_pg_mysql import (
    _jsonable_bound,
    _pg_quoted_literal,
    mapped_single_pk,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
    pk_range_predicate,
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

_FETCH_BATCH = 8192
_READ_CHUNK = 1 << 20

_UNSAFE_SS_BASES = frozenset({
    "IMAGE",
    "BINARY",
    "VARBINARY",
    "TIMESTAMP",
    "ROWVERSION",
    "XML",
    "GEOGRAPHY",
    "GEOMETRY",
    "HIERARCHYID",
    "SQL_VARIANT",
    "UNIQUEIDENTIFIER",
    "DATETIMEOFFSET",
})


def sqlserver_pg_copy_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlserver_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().upper()
    if not raw:
        return False
    base = raw.split("(")[0].strip()
    return base not in _UNSAFE_SS_BASES


def _pg_ident(name: str) -> str:
    return _quote(name)


def _close_ss(conn: Any) -> None:
    """Commit leftover T-SQL trans, then disconnect so TABLOCK is not pooled."""
    inner = getattr(conn, "_conn", conn)
    try:
        cur = inner.cursor()
        try:
            for _ in range(8):
                cur.execute("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")
                cur.execute("SELECT @@TRANCOUNT")
                row = cur.fetchone()
                if not row or int(row[0] or 0) == 0:
                    break
        finally:
            try:
                cur.close()
            except Exception:
                logger.debug("SQL Server drain cursor close skipped", exc_info=True)
    except Exception:
        logger.debug("SQL Server TRANCOUNT drain skipped", exc_info=True)
    try:
        inner.rollback()
    except Exception:
        logger.debug("SQL Server rollback skipped", exc_info=True)
    for method in ("invalidate", "detach", "close"):
        fn = getattr(inner, method, None)
        if callable(fn):
            try:
                fn()
                break
            except Exception:
                logger.debug("SQL Server %s skipped", method, exc_info=True)
    try:
        conn.close()
    except Exception:
        logger.debug("SQL Server wrapper close skipped", exc_info=True)


def _select_sql(
    table_ref: str,
    source_cols: list[str],
    clause: str,
    source_hint: str,
) -> str:
    cols = ", ".join(_ss_ident(c) for c in source_cols)
    hint = f" {source_hint}" if source_hint else ""
    where = f" WHERE {clause}" if clause and clause != "1=1" else ""
    return f"SELECT {cols} FROM {table_ref}{hint}{where}"  # nosec B608


class _SelectCopyReader:
    """Single-thread file-like: fetch SQL Server rows as COPY text on read()."""

    def __init__(self, cur: Any) -> None:
        self._cur = cur
        self._buf = b""
        self._done = False
        self._encode = fast_copy_text_value

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        want = _READ_CHUNK if size is None or size < 0 else max(int(size), 1)
        join = "\t".join
        encode = self._encode
        while not self._done and len(self._buf) < want:
            batch = self._cur.fetchmany(_FETCH_BATCH)
            if not batch:
                self._done = True
                break
            payload = "\n".join(join(encode(v) for v in row) for row in batch)
            self._buf += (payload + "\n").encode("utf-8")
        out = self._buf[:want]
        self._buf = self._buf[want:]
        return out


def _select_into_pg(
    source_conn: Any,
    dst_cur: Any,
    *,
    select_sql: str,
    params: list[Any],
    dest_ref: str,
    columns: list[str],
) -> None:
    col_list = ", ".join(_pg_ident(c) for c in columns)
    copy_sql = (
        f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
        "(FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    )
    cur = source_conn.cursor()
    try:
        if params:
            cur.execute(select_sql, params)
        else:
            cur.execute(select_sql)
        dst_cur.copy_expert(copy_sql, _SelectCopyReader(cur))
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("SQL Server stream cursor close skipped", exc_info=True)


def copy_sqlserver_to_postgres(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Stream SQL Server rows into PostgreSQL COPY. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_pg_copy_enabled():
        raise FastPathUnavailable("SQL Server→PostgreSQL COPY disabled")

    from connectors.write_resilience import is_public_proxy_host
    from services.copy_fast_path import _table_ref as _pg_table_ref

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = _ss_schema_of(source_cfg, source_schema)
    source_ref = _ss_table_ref(src_schema, source_table)
    dest_ref = _pg_table_ref(dest_schema, dest_table)

    source_conn = _ss_connect(source_cfg)
    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    existed_before = False
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
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_occupied = int(dst_cur.fetchone()[0]) > 0
            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty PostgreSQL dest stays on the row path"
                )
        else:
            pk_dest = [
                rename
                for src_pk in pk_cols
                for src_col, rename in pairs
                if src_col.lower() == src_pk.lower()
            ]
            dst_cur.execute(
                _pg_create_sql(dest_schema, dest_table, pairs, pg_ddls, pk_dest)
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
                dest_ident = _pg_ident(dest_pk)
                dest_conn.commit()
                to_copy = []
                for part in partitions:
                    already = _pg_range_count(dst_cur, dest_ref, dest_ident, part)
                    expected = int(part["source_count"])
                    if already == expected:
                        part["action"] = "skip"
                        part["dest_count"] = already
                    elif already == 0:
                        part["action"] = "load"
                        to_copy.append(part)
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
                            raise FastPathUnavailable(
                                "refusing unbounded dest DELETE on resume"
                            )
                        dst_cur.execute(
                            f"DELETE FROM {dest_ref} WHERE {pred}"  # nosec B608
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
            _select_into_pg(
                source_conn,
                dst_cur,
                select_sql=_select_sql(
                    source_ref, source_cols, clause, source_hint
                ),
                params=params,
                dest_ref=dest_ref,
                columns=target_cols,
            )
            dest_conn.commit()

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "SQL Server→PG COPY refused: dest COUNT(*) "
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
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and pk_map is None:
            try:
                dst_cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
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
            logger.debug("pg dest cursor close skipped", exc_info=True)
        try:
            _close_ss(source_conn)
        except Exception:
            logger.debug("SQL Server source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("pg dest close skipped", exc_info=True)
