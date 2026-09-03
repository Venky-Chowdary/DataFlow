"""PostgreSQL COPY text → MongoDB insert_many (cross-engine bulk).

This is **not** ``mongoimport``. One PostgreSQL ``REPEATABLE READ``
snapshot streams ``COPY (SELECT …) TO STDOUT`` text; decoded rows become
BSON documents and ``insert_many`` (unordered) loads them. Dest COUNT is
``count_documents({})`` — never ``estimatedDocumentCount``. Empty dest
is insert, **not** upsert / ``ReplaceOne``. Occupied dest whose COUNT
already equals the source snapshot is skip-complete. Occupied dest with
a different COUNT declines (identity COPY would duplicate).

``_id`` is not invented from row bytes. Map ``_id`` explicitly or let
Mongo assign ObjectIds. SQL NULL is BSON null (field present). Empty
string stays empty string. DATE is BSON Date at UTC midnight — Mongo
has no date-only type. TIMESTAMP / TIMESTAMPTZ decline (BSON Date would
invent UTC).

Declines (row path keeps quarantine): transforms that change values,
jsonb/bytea/timestamptz/arrays/timestamp, public proxy, occupied dest
with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
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
from services.copy_pg_mysql import (
    _copy_select_sql,
    _pg_base,
    _pg_connect,
    mapping_is_plain_carry,
    pg_type_is_load_safe,
)
from services.copy_pg_sqlserver import decode_copy_text_row

logger = logging.getLogger(__name__)


def mongo_family_name(name: str) -> str:
    n = (name or "").strip().lower()
    if n in {"mongodb", "mongo"}:
        return "mongodb"
    return n


def pg_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("PG_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pg_mongo_copy_batch() -> int:
    raw = (getenv_brand("PG_MONGO_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def pg_mongo_type_is_copy_safe(declared: str) -> bool:
    """LOAD-DATA-safe PG types minus TIMESTAMP (BSON Date invents UTC)."""
    if not pg_type_is_load_safe(declared):
        return False
    base = _pg_base(declared)
    return base not in {"TIMESTAMP", "DATETIME"}


def mongo_database_name(cfg: dict[str, Any]) -> str:
    return str(cfg.get("database") or "").strip()


def mongo_collection(
    cfg: dict[str, Any], collection: str
) -> tuple[Any, Any]:
    """Return ``(client, collection)`` from the pooled Mongo client."""
    from connectors.mongodb_common import _mongo_client
    from src.transfer.adapters import mongodb_connection_string

    database = mongo_database_name(cfg)
    if not database:
        raise FastPathUnavailable("MongoDB database name required")
    name = str(collection or cfg.get("collection") or cfg.get("table") or "").strip()
    if not name:
        raise FastPathUnavailable("MongoDB collection name required")
    client = _mongo_client(mongodb_connection_string(cfg))
    return client, client[database][name]


def mongo_dest_count(cfg: dict[str, Any], collection: str) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "mongodb",
        {**cfg, "database": mongo_database_name(cfg)},
        schema="",  # unused for Mongo; destination_row_count still requires it
        table_name=collection,
    )
    if n is None:
        raise ValueError("MongoDB dest COUNT unmeasured (count_documents)")
    return int(n)


def _as_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _as_float(value: str | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _as_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    low = value.strip().lower()
    if low in {"t", "true", "1"}:
        return True
    if low in {"f", "false", "0"}:
        return False
    raise ValueError(f"boolean COPY cell is not BSON-safe: {value!r}")


def _as_date_utc(value: str | None) -> datetime | None:
    """Calendar day → BSON Date at UTC midnight. Mongo has no date-only type."""
    if value is None:
        return None
    day = date.fromisoformat(value[:10])
    return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)


def _as_decimal128(value: str | None) -> Any:
    if value is None:
        return None
    from bson.decimal128 import Decimal128

    return Decimal128(Decimal(value))


def _identity(value: str | None) -> str | None:
    return value


def converter_for_mongo_ddl(ddl: str) -> Callable[[str | None], Any]:
    base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
    if base in {"BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT", "INT2", "INT4", "INT8"}:
        return _as_int
    if base in {"FLOAT", "REAL", "FLOAT4", "FLOAT8"} or base.startswith("DOUBLE"):
        return _as_float
    if base in {"BOOLEAN", "BOOL"}:
        return _as_bool
    if base == "DATE":
        return _as_date_utc
    if base in {"NUMERIC", "DECIMAL"}:
        return _as_decimal128
    return _identity


class _CopyInsertManySink:
    """File-like COPY TO STDOUT consumer that inserts decoded BSON batches."""

    def __init__(
        self,
        coll: Any,
        target_cols: list[str],
        converters: list[Callable[[str | None], Any]],
        batch_size: int,
    ) -> None:
        self._coll = coll
        self._target_cols = target_cols
        self._converters = converters
        self._batch_size = batch_size
        self._expected_cols = len(target_cols)
        self._raw = bytearray()
        self._batch: list[dict[str, Any]] = []
        self.rows = 0

    def write(self, data: Any) -> int:
        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, memoryview):
            data = data.tobytes()
        self._raw.extend(data)
        self._drain_lines(flush=False)
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self._drain_lines(flush=True)

    def _drain_lines(self, *, flush: bool) -> None:
        while True:
            nl = self._raw.find(b"\n")
            if nl < 0:
                break
            line = self._raw[:nl]
            del self._raw[: nl + 1]
            if line.endswith(b"\r"):
                line = line[:-1]
            if not line:
                continue
            self._add_line(line.decode("utf-8"))
        if flush and self._raw:
            line = bytes(self._raw)
            self._raw.clear()
            if line.endswith(b"\r"):
                line = line[:-1]
            if line:
                self._add_line(line.decode("utf-8"))
        if flush:
            self._flush_batch()

    def _add_line(self, line: str) -> None:
        fields = decode_copy_text_row(line)
        if len(fields) != self._expected_cols:
            raise ValueError(
                f"COPY row has {len(fields)} fields, expected {self._expected_cols}"
            )
        doc = {
            name: conv(value)
            for name, conv, value in zip(
                self._target_cols, self._converters, fields, strict=True
            )
        }
        self._batch.append(doc)
        if len(self._batch) >= self._batch_size:
            self._flush_batch()

    def _flush_batch(self) -> None:
        if not self._batch:
            return
        result = self._coll.insert_many(self._batch, ordered=False)
        inserted = len(result.inserted_ids)
        if inserted != len(self._batch):
            raise ValueError(
                "PostgreSQL→Mongo COPY refused: insert_many "
                f"{inserted} != batch {len(self._batch)}"
            )
        self.rows += inserted
        self._batch.clear()


def copy_postgres_to_mongo(
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
    """COPY PG text into Mongo insert_many. Dest count_documents is the proof."""
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_mongo_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→MongoDB COPY disabled")
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
    src_schema = source_schema or source_cfg.get("schema") or "public"
    source_ref = _pg_table_ref(src_schema, source_table)
    converters = [converter_for_mongo_ddl(ddl) for ddl in mongo_ddls]

    _client, coll = mongo_collection(dest_cfg, dest_table)
    source_conn = _pg_connect(source_cfg)
    created_here = False
    try:
        source_conn.autocommit = False
        src_cur = source_conn.cursor()
        src_cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        live = source_column_types(src_cur, src_schema, source_table, source_cols)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not declared:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not pg_mongo_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Mongo COPY-safe"
                )
        shape = source_table_shape(src_cur, src_schema, source_table, source_cols)
        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
        source_count = int(src_cur.fetchone()[0])
        src_cur.execute("SELECT pg_export_snapshot()")
        snapshot_id = str(src_cur.fetchone()[0])

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
                        "pg_snapshot": snapshot_id,
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "mongo_write": "skip",
                        "source_pk": list(shape.primary_key or []),
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

        select_list = ", ".join(_quote(c) for c in source_cols)
        copy_sql = _copy_select_sql(select_list, source_ref, "")
        sink = _CopyInsertManySink(
            coll, target_cols, converters, pg_mongo_copy_batch()
        )
        try:
            src_cur.copy_expert(copy_sql, sink)
        finally:
            sink.close()
        if sink.rows != source_count:
            raise ValueError(
                "PostgreSQL→Mongo COPY refused: inserted "
                f"{sink.rows} != source snapshot {source_count}"
            )

        dest_count = mongo_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "PostgreSQL→Mongo COPY refused: dest count_documents "
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
                "pg_snapshot": snapshot_id,
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "mongo_write": mongo_write,
                "source_pk": list(shape.primary_key or []),
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
            logger.debug("PostgreSQL source rollback skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("PostgreSQL source close skipped", exc_info=True)
