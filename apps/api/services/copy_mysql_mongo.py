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
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mysql_pg import (
    _FETCH_BATCH,
    _mysql_base,
    _mysql_connect,
    _mysql_ident,
    _mysql_table_pk_and_types,
    _select_sql,
    mysql_type_is_copy_safe,
)
from services.copy_pg_mongo import mongo_collection, mongo_dest_count
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)


def mysql_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("MYSQL_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mysql_mongo_copy_batch() -> int:
    raw = (getenv_brand("MYSQL_MONGO_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def mysql_mongo_type_is_copy_safe(declared: str) -> bool:
    """LOAD-DATA-safe MySQL types minus DATETIME/TIME (BSON Date invents UTC)."""
    if not mysql_type_is_copy_safe(declared):
        return False
    return _mysql_base(declared) not in {"DATETIME", "TIME"}


def mysql_value_to_bson(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.hour or value.minute or value.second or value.microsecond:
            raise FastPathUnavailable(
                "MySQL DATETIME/TIMESTAMP is not Mongo COPY-safe"
            )
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, Decimal):
        from bson.decimal128 import Decimal128

        return Decimal128(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary MySQL field is not Mongo COPY-safe")
    return value


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
                    result = coll.insert_many(batch, ordered=False)
                    got = len(result.inserted_ids)
                    if got != len(batch):
                        raise ValueError(
                            "MySQL→Mongo COPY refused: insert_many "
                            f"{got} != batch {len(batch)}"
                        )
                    inserted += got
                    batch.clear()
        if batch:
            result = coll.insert_many(batch, ordered=False)
            got = len(result.inserted_ids)
            if got != len(batch):
                raise ValueError(
                    "MySQL→Mongo COPY refused: insert_many "
                    f"{got} != batch {len(batch)}"
                )
            inserted += got
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

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    table_q = _mysql_ident(source_table)
    select_sql = _select_sql(table_q, source_cols, "")

    _client, coll = mongo_collection(dest_cfg, dest_table)
    source_conn = _mysql_connect(source_cfg)
    created_here = False
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

        dest_count_before = mongo_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                proof = f"dest_count:{dest_count_before}"
                return FastPathResult(
                    rows_copied=source_count,
                    source_rows=source_count,
                    source_checksum=proof,
                    target_rows=dest_count_before,
                    target_checksum=proof,
                    source_snapshot={
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "mongo_write": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            raise FastPathUnavailable(
                "append into occupied Mongo dest stays on the row path "
                "(identity COPY would duplicate)"
            )

        mongo_write = "overwrite" if replace_destination and dest_occupied else "insert"
        if replace_destination and dest_occupied:
            coll.drop()
            dest_count_before = 0
        created_here = dest_count_before == 0

        inserted = _select_insert_many(
            source_conn,
            coll,
            select_sql=select_sql,
            target_cols=target_cols,
            batch_size=mysql_mongo_copy_batch(),
        )
        if inserted != source_count:
            raise ValueError(
                "MySQL→Mongo COPY refused: inserted "
                f"{inserted} != source snapshot {source_count}"
            )

        dest_count = mongo_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "MySQL→Mongo COPY refused: dest count_documents "
                f"{dest_count} != source snapshot {source_count}"
            )
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
                "mongo_write": mongo_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                coll.drop()
            except Exception:
                logger.debug("Mongo dest drop after copy failure skipped", exc_info=True)
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
