"""PostgreSQL COPY text → MySQL STRICT LOAD DATA (cross-engine bulk).

Same-engine PG→PG already uses binary COPY in ``copy_fast_path``. Cross-engine
cannot use that wire. This path streams ``COPY (SELECT …) TO STDOUT`` text
(tab / ``\\N``) into ``LOAD DATA LOCAL INFILE`` under STRICT sql_mode.

Python never materializes a row. Dest ``COUNT(*)`` in the same operator
proof must equal the source snapshot count. Warning/Error from LOAD DATA
rolls the destination back (truncate) and raises — never silent coerce.

Declines (row path keeps quarantine): transforms that change values, jsonb,
bytea, timestamptz, arrays, non-empty append, missing local_infile.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote, _table_ref
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
) -> FastPathResult:
    """COPY text from PostgreSQL into MySQL LOAD DATA. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")

    from connectors.mysql_conn import get_connection as mysql_connect
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        mysql_load_data_session_ready,
        quote_load_data_path,
    )
    from connectors.postgresql_conn import get_connection as pg_connect
    from connectors.write_resilience import is_public_proxy_host
    from services.copy_fast_path import source_column_types, source_table_shape

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: LOCAL INFILE not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    source_ref = _table_ref(source_schema, source_table)

    source_conn = pg_connect(
        host=source_cfg.get("host", ""),
        port=int(source_cfg.get("port") or 5432),
        database=source_cfg.get("database") or source_cfg.get("dbname") or "",
        username=source_cfg.get("username") or source_cfg.get("user") or "",
        password=source_cfg.get("password", ""),
        connection_string=source_cfg.get("connection_string", ""),
        ssl=bool(source_cfg.get("ssl", False)),
    )
    dest_conn = mysql_connect(
        host=dest_cfg.get("host", ""),
        port=int(dest_cfg.get("port") or 3306),
        database=dest_cfg.get("database", ""),
        username=dest_cfg.get("username") or dest_cfg.get("user") or "",
        password=dest_cfg.get("password", ""),
        connection_string=dest_cfg.get("connection_string", ""),
        ssl=bool(dest_cfg.get("ssl", False)),
        purpose="write",
    )
    dest_conn.autocommit = False
    fd, path = tempfile.mkstemp(prefix="df_pg_mysql_", suffix=".tsv")
    os.close(fd)
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
            ready, why = mysql_load_data_session_ready(dst_cur, dest_conn)
            if not ready:
                raise FastPathUnavailable(why)

            table_q = _mysql_ident(dest_table)
            dst_cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                (dest_table,),
            )
            exists = dst_cur.fetchone() is not None
            if replace_destination and exists:
                dst_cur.execute(f"DROP TABLE IF EXISTS {table_q}")  # nosec B608
                exists = False
            if exists:
                dst_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
                if int(dst_cur.fetchone()[0]) > 0:
                    raise FastPathUnavailable(
                        "append into non-empty MySQL dest stays on the row path"
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

            src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])

            select_list = ", ".join(
                _pg_copy_select_expr(col, live_l[col.lower()]) for col in source_cols
            )
            copy_sql = (
                f"COPY (SELECT {select_list} FROM {source_ref}) "  # nosec B608
                "TO STDOUT WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
            )
            with open(path, "wb") as handle:
                src_cur.copy_expert(copy_sql, handle)

            load_sql = build_load_data_sql(
                table_q=table_q,
                columns=target_cols,
                infile_sql=quote_load_data_path(path),
            )
            try:
                dst_cur.execute(load_sql)
            except Exception as exc:
                dest_conn.rollback()
                raise FastPathUnavailable(f"LOAD DATA failed: {exc}") from exc
            dst_cur.execute("SHOW WARNINGS")
            blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
            if blocked:
                dest_conn.rollback()
                raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")

            dst_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            dest_count = int(dst_cur.fetchone()[0])
            if dest_count != source_count:
                dest_conn.rollback()
                raise ValueError(
                    "PG→MySQL COPY refused: dest COUNT(*) "
                    f"{dest_count} != source snapshot {source_count}"
                )
            dest_conn.commit()
            source_conn.commit()
            proof = f"dest_count:{dest_count}"
            return FastPathResult(
                rows_copied=dest_count,
                source_rows=source_count,
                source_checksum=proof,
                target_rows=dest_count,
                target_checksum=proof,
                proof_scope="dest_count_equals_source_snapshot_count",
            )
    finally:
        try:
            os.unlink(path)
        except OSError:
            logger.debug("pg→mysql tempfile unlink skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("pg source close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("mysql dest close skipped", exc_info=True)
