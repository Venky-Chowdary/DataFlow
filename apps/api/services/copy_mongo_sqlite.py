"""MongoDB snapshot find → SQLite executemany (cross-engine bulk).

Source COUNT is ``count_documents({})`` inside a replica-set snapshot
transaction — never ``estimatedDocumentCount``. Payload is ``find()`` in
that same snapshot bound with ``executemany`` INSERT. Dest ``COUNT(*)``
must equal that snapshot COUNT **before commit**. Empty dest is insert,
**not** upsert / sqlite3 ``.import`` / ``mongoexport``. Occupied dest
whose COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines. Nested documents /
binary decline. DATE is SQLite TEXT (ISO calendar day) — SQLite has no
DATE affinity (engine law). ``:memory:`` / BLOB dest DDL decline.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, ``:memory:``, snapshot read concern
unavailable.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session, mongo_type_is_copy_safe
from services.copy_mongo_sink import bson_to_python
from services.copy_pg_mongo import mongo_collection
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_table_exists,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def mongo_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mongo_sqlite_copy_batch() -> int:
    raw = (getenv_brand("MONGO_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def python_to_sqlite(value: Any) -> Any:
    """Bind a BSON-decoded Python value. DATE lands as ISO TEXT."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.hour or value.minute or value.second or value.microsecond:
            raise FastPathUnavailable(
                "datetime with a time component is not SQLite DATE COPY-safe"
            )
        return date(value.year, value.month, value.day).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Mongo field is not SQLite COPY-safe")
    return value


def copy_mongo_to_sqlite(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """INSERT Mongo snapshot documents into SQLite. Dest COUNT(*) before commit is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_sqlite_copy_enabled():
        raise FastPathUnavailable("MongoDB→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        source_cfg.get("host") or source_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    sqlite_resolved_path(dest_cfg)
    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    batch_size = mongo_sqlite_copy_batch()

    client, coll = mongo_collection(source_cfg, source_table)
    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    session = None
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        dest_conn.execute("BEGIN IMMEDIATE")
        exists = sqlite_table_exists(dest_conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                dest_conn.rollback()
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"mongo_read": "skip", "sqlite_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if exists:
            live = sqlite_pragma_types(dest_conn, dest_table)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in target_cols:
                declared = live_l.get(col.lower())
                if declared is None:
                    raise FastPathUnavailable(f"dest column {col!r} absent")
                if not sqlite_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"dest column {col!r} type {declared} is not SQLite COPY-safe"
                    )
        else:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        pending: list[tuple[Any, ...]] = []
        inserted = 0
        while True:
            batch = list(islice(cursor, _FIND_BATCH))
            if not batch:
                break
            for doc in batch:
                pending.append(
                    tuple(
                        python_to_sqlite(bson_to_python(doc.get(col), ddl))
                        for col, ddl in zip(source_cols, sqlite_ddls, strict=True)
                    )
                )
                if len(pending) >= batch_size:
                    dest_conn.executemany(insert_sql, pending)
                    inserted += len(pending)
                    pending.clear()
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "Mongo→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source snapshot {source_count}"
            )
        dest_conn.commit()
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "mongo_read": "snapshot_find",
                "sqlite_write": sqlite_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("SQLite dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        if session is not None:
            try:
                session.abort_transaction()
            except Exception:
                logger.debug("Mongo snapshot abort skipped", exc_info=True)
            try:
                session.end_session()
            except Exception:
                logger.debug("Mongo snapshot session close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
