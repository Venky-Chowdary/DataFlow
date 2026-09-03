"""Oracle SHARE-lock SELECT → MongoDB insert_many (cross-engine bulk).

This host has no client ``sqlldr`` / Data Pump. One
``LOCK TABLE src IN SHARE MODE`` transaction streams ``SELECT``; Python
values become BSON documents and ``insert_many`` (unordered) loads them.
Dest COUNT is ``count_documents({})`` — never ``estimatedDocumentCount``.
Empty dest is insert, **not** upsert / ``ReplaceOne``. Occupied dest
whose COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines.

``_id`` is not invented from row bytes. SQL NULL is BSON null. Oracle
``VARCHAR2`` stores ``''`` as ``NULL`` (engine law) — source empty
strings therefore arrive as BSON null, not a row drop. DATE at midnight
is BSON Date at UTC midnight. TIMESTAMP / TIMESTAMP WITH TIME ZONE
decline (BSON Date would invent UTC). This is **not** ``mongoimport``.

Declines (row path keeps quarantine): transforms that change values,
BLOB/RAW/XMLTYPE/SDO_GEOMETRY/timestamp, public proxy, occupied dest
with dest COUNT ≠ source, DATE with a time component.
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
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _ora_table_pk_and_types,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_ref as _ora_table_ref,
)
from services.copy_oracle_pg import _select_sql, _tune_fetch, oracle_type_is_copy_safe
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192
_UNSAFE_ORA_MONGO = frozenset({
    "TIMESTAMP",
    "TIMESTAMPWITHTIMEZONE",
    "TIMESTAMPWITHLOCALTIMEZONE",
})


def oracle_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("ORACLE_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def oracle_mongo_type_is_copy_safe(declared: str) -> bool:
    if not oracle_type_is_copy_safe(declared):
        return False
    raw = (declared or "").strip().upper()
    if "WITH TIME ZONE" in raw or "WITH LOCAL TIME ZONE" in raw:
        return False
    base = raw.split("(")[0].strip().replace(" ", "")
    return base not in _UNSAFE_ORA_MONGO


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
    _tune_fetch(cur)
    try:
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
            logger.debug("Oracle stream cursor close skipped", exc_info=True)


def copy_oracle_to_mongo(
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
    """SELECT Oracle into Mongo insert_many. Dest count_documents is the proof."""
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_mongo_copy_enabled():
        raise FastPathUnavailable("Oracle→MongoDB COPY disabled")
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
    src_schema = _ora_schema_of(source_cfg, source_schema)
    source_ref = _ora_table_ref(src_schema, source_table)

    source_conn = _oracle_connect(source_cfg)
    created_here = False
    coll = None
    src_cur = source_conn.cursor()
    try:
        pk_cols, live = _ora_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not oracle_mongo_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Mongo COPY-safe"
                )
        src_cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _ora_count(src_cur, source_ref)
        select_sql = _select_sql(source_ref, source_cols, "")
        src_cur.close()
        src_cur = None  # type: ignore[assignment]

        extra = {"oracle_lock": "share", "source_pk": list(pk_cols or [])}
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
            batch_size=mongo_copy_batch("ORACLE_MONGO_COPY_BATCH"),
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
                logger.debug("Oracle source cursor close skipped", exc_info=True)
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("Oracle source rollback skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("Oracle source close skipped", exc_info=True)
