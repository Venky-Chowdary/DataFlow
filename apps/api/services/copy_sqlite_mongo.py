"""SQLite SELECT → MongoDB insert_many (cross-engine bulk).

One ``BEGIN`` on the source file streams ``SELECT``; Python values become
BSON documents and ``insert_many`` (unordered) loads them. Dest COUNT is
``count_documents({})`` — never ``estimatedDocumentCount``. Empty dest
is insert, **not** upsert / ``mongoimport`` / ``.dump``. Occupied dest
whose COUNT already equals the source COUNT is skip-complete. Occupied
dest with a different COUNT declines. ``:memory:`` / BLOB decline.
DATE ISO text or a calendar day becomes BSON Date at UTC midnight.
DATETIME / TIMESTAMP decline (BSON Date would invent UTC).

Declines (row path keeps quarantine): transforms that change values,
BLOB/DATETIME, public proxy, occupied dest with dest COUNT ≠ source,
``:memory:``.
"""

from __future__ import annotations

import logging
from datetime import date
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
from services.copy_sqlite_common import (
    sqlite_connect,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192
_UNSAFE_SQLITE_MONGO_BASES = frozenset({
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
})


def sqlite_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlite_mongo_copy_batch() -> int:
    return mongo_copy_batch("SQLITE_MONGO_COPY_BATCH")


def sqlite_mongo_type_is_copy_safe(declared: str) -> bool:
    if not sqlite_type_is_copy_safe(declared):
        return False
    base = (declared or "").strip().upper().replace(" ", "").split("(", 1)[0]
    return base not in _UNSAFE_SQLITE_MONGO_BASES


def sqlite_value_to_bson(value: Any, ddl: str) -> Any:
    """SQLite cell → BSON. DATE ISO text is UTC midnight; DATETIME declines."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("BLOB values are not Mongo COPY-safe")
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in _UNSAFE_SQLITE_MONGO_BASES:
        raise FastPathUnavailable(
            f"{base} SQLite value is not Mongo COPY-safe (BSON Date would invent UTC)"
        )
    if base == "DATE":
        if value is None:
            return None
        if isinstance(value, str):
            try:
                value = date.fromisoformat(value[:10])
            except ValueError as exc:
                raise FastPathUnavailable(
                    f"DATE cell {value!r} is not ISO calendar-day COPY-safe"
                ) from exc
        return sql_value_to_bson(value)
    return sql_value_to_bson(value)


def copy_sqlite_to_mongo(
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
    """SELECT SQLite into Mongo insert_many. Dest count_documents is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_mongo_copy_enabled():
        raise FastPathUnavailable("SQLite→MongoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    sqlite_resolved_path(source_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_ref = sqlite_ident(source_table)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}"  # nosec B608
    batch_size = sqlite_mongo_copy_batch()

    source_conn = sqlite_connect(source_cfg)
    created_here = False
    coll = None
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col, ddl in zip(source_cols, mongo_ddls, strict=True):
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_mongo_type_is_copy_safe(declared) or (
                ddl and not sqlite_mongo_type_is_copy_safe(ddl)
            ):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Mongo COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}").fetchone()[0]  # nosec B608
        )

        prepared = prepare_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            replace_destination=replace_destination,
            extra_snapshot={"sqlite_read": "skip"},
        )
        if isinstance(prepared, FastPathResult):
            return prepared
        coll, created_here, mongo_write = prepared

        src_cur = source_conn.cursor()
        src_cur.execute(select_sql)
        inserted = 0
        batch: list[dict[str, Any]] = []
        while True:
            rows = src_cur.fetchmany(_FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                batch.append(
                    {
                        name: sqlite_value_to_bson(val, ddl)
                        for name, val, ddl in zip(
                            target_cols, row, mongo_ddls, strict=True
                        )
                    }
                )
                if len(batch) >= batch_size:
                    inserted += insert_many_documents(coll, batch)
                    batch.clear()
        if batch:
            inserted += insert_many_documents(coll, batch)
        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
            extra_snapshot={"sqlite_read": "select"},
        )
    except Exception:
        abort_created_mongo(coll, created_here)
        raise
    finally:
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
