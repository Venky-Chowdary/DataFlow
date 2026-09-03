"""SQL Server HOLDLOCK SELECT → MongoDB insert_many (cross-engine bulk).

SQL Server has no ``COPY TO STDOUT`` and this host has no client ``bcp``.
One HOLDLOCK (or SNAPSHOT) transaction streams ``SELECT``; Python values
become BSON documents and ``insert_many`` (unordered) loads them. Dest
COUNT is ``count_documents({})`` — never ``estimatedDocumentCount``.
Empty dest is insert, **not** upsert / ``ReplaceOne``. Occupied dest
whose COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines.

``_id`` is not invented from row bytes. SQL NULL is BSON null. Empty
string stays empty string. DATE is BSON Date at UTC midnight. DATETIME /
DATETIME2 / TIME decline (BSON Date would invent UTC). This is **not**
BCP / ``mongoimport``.

Declines (row path keeps quarantine): transforms that change values,
varbinary/xml/geography/rowversion/datetime, public proxy, occupied dest
with dest COUNT ≠ source.
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
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlserver_pg import (
    _FETCH_BATCH,
    _close_ss,
    _select_sql,
    sqlserver_type_is_copy_safe,
)
from services.copy_sqlserver_sqlserver import (
    _count as _ss_count,
    _prepare_source_read,
    _schema_of as _ss_schema_of,
    _ss_connect,
    _ss_table_pk_and_types,
    _table_ref as _ss_table_ref,
)

logger = logging.getLogger(__name__)

_UNSAFE_SS_MONGO = frozenset({
    "DATETIME",
    "DATETIME2",
    "SMALLDATETIME",
    "TIME",
    "DATETIMEOFFSET",
})


def sqlserver_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlserver_mongo_type_is_copy_safe(declared: str) -> bool:
    if not sqlserver_type_is_copy_safe(declared):
        return False
    base = (declared or "").strip().upper().split("(")[0].strip()
    return base not in _UNSAFE_SS_MONGO


def _select_insert_many(
    source_conn: Any,
    coll: Any,
    *,
    select_sql: str,
    target_cols: list[str],
    batch_size: int,
) -> int:
    inserted = 0
    batch: list[dict[str, Any]] = []
    cur = source_conn.cursor()
    try:
        try:
            cur.arraysize = _FETCH_BATCH
        except Exception:
            logger.debug("SQL Server arraysize skipped", exc_info=True)
        cur.execute(select_sql)
        while True:
            rows = cur.fetchmany(_FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                batch.append(
                    {
                        name: sql_value_to_bson(val)
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
            logger.debug("SQL Server stream cursor close skipped", exc_info=True)


def copy_sqlserver_to_mongo(
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
    """SELECT SQL Server into Mongo insert_many. Dest count_documents is the proof."""
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_mongo_copy_enabled():
        raise FastPathUnavailable("SQL Server→MongoDB COPY disabled")
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
    src_schema = _ss_schema_of(source_cfg, source_schema)
    source_ref = _ss_table_ref(src_schema, source_table)

    source_conn = _ss_connect(source_cfg)
    created_here = False
    coll = None
    src_cur = source_conn.cursor()
    try:
        pk_cols, live = _ss_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not sqlserver_mongo_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Mongo COPY-safe"
                )
        isolation = _prepare_source_read(src_cur, source_conn)
        source_hint = "WITH (HOLDLOCK, TABLOCK)" if isolation == "holdlock" else ""
        source_count = _ss_count(src_cur, source_ref, source_hint)
        select_sql = _select_sql(source_ref, source_cols, "", source_hint)
        src_cur.close()
        src_cur = None  # type: ignore[assignment]

        extra = {"sqlserver_isolation": isolation, "source_pk": list(pk_cols or [])}
        prepared = prepare_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            replace_destination=replace_destination,
            extra_snapshot=extra,
        )
        if isinstance(prepared, FastPathResult):
            return prepared
        coll, created_here, mongo_write = prepared
        inserted = _select_insert_many(
            source_conn,
            coll,
            select_sql=select_sql,
            target_cols=target_cols,
            batch_size=mongo_copy_batch("SQLSERVER_MONGO_COPY_BATCH"),
        )
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
            extra_snapshot=extra,
        )
    except Exception:
        abort_created_mongo(coll, created_here)
        raise
    finally:
        if src_cur is not None:
            try:
                src_cur.close()
            except Exception:
                logger.debug("SQL Server source cursor close skipped", exc_info=True)
        _close_ss(source_conn)
