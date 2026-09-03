"""Canonical Mongo dest writer for identity COPY.

Dest COUNT is ``count_documents({})`` — never ``estimatedDocumentCount``.
Empty dest is ``insert_many`` (unordered), **not** upsert / ``ReplaceOne``.
Occupied dest whose COUNT already equals the source snapshot is
skip-complete. Occupied dest with a different COUNT declines.
``_id`` is not invented from row bytes.

SQL NULL is BSON null (field present). Empty string stays empty string.
DATE-like Python values at midnight become BSON Date at UTC midnight —
Mongo has no date-only type. A datetime with a time component declines
(BSON Date would invent UTC). Nested documents are not converted here;
pair engines that allow BSON identity (Mongo→Mongo) pass values through.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mongo import mongo_collection, mongo_dest_count

logger = logging.getLogger(__name__)


def mongo_copy_batch(env_key: str, default: str = "5000") -> int:
    raw = (getenv_brand(env_key, default) or default).strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def sql_value_to_bson(value: Any) -> Any:
    """Python cell from a SQL driver → BSON. Midnight datetime is DATE."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.hour or value.minute or value.second or value.microsecond:
            raise FastPathUnavailable(
                "SQL datetime with a time component is not Mongo COPY-safe "
                "(BSON Date would invent UTC)"
            )
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, Decimal):
        from bson.decimal128 import Decimal128

        return Decimal128(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary SQL field is not Mongo COPY-safe")
    return value


def bson_to_python(value: Any, ddl: str) -> Any:
    """BSON cell → Python for SQL dest bind. Nested/binary decline."""
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        raise FastPathUnavailable("nested Mongo document is not COPY-safe")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Mongo field is not COPY-safe")
    from bson import Decimal128, Int64, ObjectId

    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, datetime):
        base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
        if base == "DATE":
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc).date()
            return value.date()
        if value.tzinfo is not None:
            raise FastPathUnavailable("timestamptz Mongo Date is not COPY-safe")
        return value
    return value


def insert_many_documents(coll: Any, docs: list[dict[str, Any]]) -> int:
    if not docs:
        return 0
    result = coll.insert_many(docs, ordered=False)
    inserted = len(result.inserted_ids)
    if inserted != len(docs):
        raise ValueError(
            "Mongo COPY refused: insert_many "
            f"{inserted} != batch {len(docs)}"
        )
    return inserted


def skip_complete_mongo(
    *,
    source_count: int,
    dest_count: int,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "skip",
        "copy_partitions": 1,
        "partitions_skipped": 1,
        "partitions_loaded": 0,
        "shard_mode": "table",
        "mongo_write": "skip",
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=source_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def prepare_mongo_dest(
    *,
    dest_cfg: dict[str, Any],
    dest_table: str,
    source_count: int,
    replace_destination: bool,
    extra_snapshot: dict[str, Any] | None = None,
) -> tuple[Any, bool, str] | FastPathResult:
    """Return ``(coll, created_here, mongo_write)`` or a skip-complete result."""
    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    _client, coll = mongo_collection(dest_cfg, dest_table)
    dest_count_before = mongo_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        if dest_count_before == source_count:
            return skip_complete_mongo(
                source_count=source_count,
                dest_count=dest_count_before,
                extra_snapshot=extra_snapshot,
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
    return coll, created_here, mongo_write


def prove_mongo_dest(
    *,
    dest_cfg: dict[str, Any],
    dest_table: str,
    source_count: int,
    inserted: int,
    mongo_write: str,
    extra_snapshot: dict[str, Any] | None = None,
) -> FastPathResult:
    if inserted != source_count:
        raise ValueError(
            "Mongo COPY refused: inserted "
            f"{inserted} != source snapshot {source_count}"
        )
    dest_count = mongo_dest_count(dest_cfg, dest_table)
    if dest_count != source_count:
        raise ValueError(
            "Mongo COPY refused: dest count_documents "
            f"{dest_count} != source snapshot {source_count}"
        )
    proof = f"dest_count:{dest_count}"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "serial",
        "copy_partitions": 1,
        "partitions_skipped": 0,
        "partitions_loaded": 1,
        "shard_mode": "table",
        "mongo_write": mongo_write,
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=dest_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def abort_created_mongo(coll: Any, created_here: bool) -> None:
    if not created_here:
        return
    try:
        coll.drop()
    except Exception:
        logger.debug("Mongo dest drop after copy failure skipped", exc_info=True)
