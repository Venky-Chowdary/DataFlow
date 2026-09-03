"""MySQL consistent-snapshot SELECT → MongoDB insert_many (cross-engine bulk).

MySQL has no ``COPY TO STDOUT``. One ``START TRANSACTION WITH CONSISTENT
SNAPSHOT`` streams ``SELECT`` (SSCursor); Python values become BSON
documents and ``insert_many`` (unordered) loads them. Dest COUNT is
``count_documents({})`` — never ``estimatedDocumentCount``. Empty dest
is insert, **not** upsert / ``ReplaceOne``. Occupied dest whose COUNT
already equals the source snapshot is skip-complete. Occupied dest with
a different COUNT declines (identity COPY would duplicate).

``_id`` is not invented from row bytes. SQL NULL is BSON null (field
present). Empty string stays empty string. DATE is BSON Date at UTC
midnight — Mongo has no date-only type. DATETIME / TIME / TIMESTAMP
decline (BSON Date would invent UTC or a calendar day).

Declines (row path keeps quarantine): transforms that change values,
blob/json/geometry/bit/timestamp/datetime/time, public proxy, occupied
dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_sink import (
    abort_created_mongo,
    insert_many_documents,
    mongo_copy_batch,
    prepare_mongo_dest,
    prove_mongo_dest,
    sql_value_to_bson,
)
from services.copy_mysql_pg import (
    _FETCH_BATCH,
    _mysql_base,
    _mysql_connect,
    _mysql_ident,
    _mysql_table_pk_and_types,
    _select_sql,
    mysql_type_is_copy_safe,
)
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)


def mysql_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("MYSQL_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mysql_mongo_copy_batch() -> int:
    return mongo_copy_batch("MYSQL_MONGO_COPY_BATCH")


def mysql_mongo_type_is_copy_safe(declared: str) -> bool:
    """LOAD-DATA-safe MySQL types minus DATETIME/TIME (BSON Date invents UTC)."""
    if not mysql_type_is_copy_safe(declared):
        return False
    return _mysql_base(declared) not in {"DATETIME", "TIME"}


def mysql_value_to_bson(value: Any) -> Any:
    return sql_value_to_bson(value)


def _select_insert_many(
    source_conn: Any,
    coll: Any,
    *,
    select_sql: str,
    target_cols: list[str],
    batch_size: int,
) -> int:
    from pymysql.cursors import SSCursor

    inserted = 0
    batch: list[dict[str, Any]] = []
    cur = source_conn.cursor(SSCursor)
    try:
        cur.execute(select_sql)
        while True:
            rows = cur.fetchmany(_FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                batch.append(
                    {
                        name: mysql_value_to_bson(val)
                        for name, val in zip(target_cols, row, strict=True)
                    }
                )
                if len(batch) >= batch_size:
                    inserted += insert_many_documents(coll, batch)
                    batch.clear()
        if batch:
            inserted += insert_many_documents(coll, batch)
        return inserted
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("MySQL stream cursor close skipped", exc_info=True)


def copy_mysql_to_mongo(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mongo_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """SELECT MySQL into Mongo insert_many. Dest count_documents is the proof."""
    del source_schema  # MySQL tables are schema = database().
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mysql_mongo_copy_enabled():
        raise FastPathUnavailable("MySQL→MongoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    table_q = _mysql_ident(source_table)
    select_sql = _select_sql(table_q, source_cols, "")

    source_conn = _mysql_connect(source_cfg)
    created_here = False
    coll = None
    try:
        with source_conn.cursor() as src_cur:
            src_cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            src_cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            _pk_cols, live = _mysql_table_pk_and_types(src_cur, source_table, source_cols)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in source_cols:
                declared = live_l.get(col.lower()) or ""
                if not mysql_mongo_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"source column {col!r} type {declared} is not Mongo COPY-safe"
                    )
            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            source_count = int(src_cur.fetchone()[0])

        prepared = prepare_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            replace_destination=replace_destination,
        )
        if isinstance(prepared, FastPathResult):
            return prepared
        coll, created_here, mongo_write = prepared
        inserted = _select_insert_many(
            source_conn,
            coll,
            select_sql=select_sql,
            target_cols=target_cols,
            batch_size=mysql_mongo_copy_batch(),
        )
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
        )
    except Exception:
        abort_created_mongo(coll, created_here)
        raise
    finally:
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("MySQL source rollback skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("MySQL source close skipped", exc_info=True)
