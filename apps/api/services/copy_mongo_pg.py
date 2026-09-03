"""MongoDB snapshot find → PostgreSQL COPY FROM STDIN (cross-engine bulk).

The reverse of ``copy_pg_mongo``. Source COUNT is ``count_documents({})``
inside a replica-set snapshot transaction — never
``estimatedDocumentCount``. Payload is ``find()`` in that same
snapshot (not a second non-snapshot scan). Each cell is encoded as
PostgreSQL COPY text into ``COPY … FROM STDIN``. Dest ``COUNT(*)`` must
equal that snapshot COUNT.

This is **not** ``mongoexport``. Empty dest COPYs the snapshot once.
Occupied dest whose ``COUNT(*)`` already equals the source snapshot
COUNT is skip-complete. Occupied dest with a different COUNT declines.
Standalone Mongo (no snapshot read concern) declines.

Nested documents / arrays / binary decline (row path keeps quarantine).
``_id`` is omitted unless mapped. BSON Date stored as UTC midnight
(from PostgreSQL DATE) round-trips as DATE.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, snapshot read concern unavailable.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_mysql_pg import _pg_connect, _pg_create_sql, fast_copy_text_value
from services.copy_pg_mongo import mongo_collection, pg_mongo_type_is_copy_safe
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_READ_CHUNK = 1 << 20
_FIND_BATCH = 4096


def mongo_pg_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mongo_type_is_copy_safe(declared: str) -> bool:
    if pg_mongo_type_is_copy_safe(declared):
        return True
    raw = (declared or "").strip().lower().replace(" ", "")
    if not raw:
        return False
    base = raw.split("(", 1)[0]
    if base in {
        "object",
        "array",
        "bindata",
        "binary",
        "javascript",
        "regex",
        "timestamp",
        "minkey",
        "maxkey",
        "dbref",
        "timestamptz",
    }:
        return False
    return base in {
        "string",
        "int",
        "long",
        "bool",
        "boolean",
        "date",
        "double",
        "decimal",
        "decimal128",
        "objectid",
        "varchar",
        "text",
        "bigint",
        "integer",
        "number",
    }


def _pg_ident(name: str) -> str:
    return _quote(name)


def _bson_to_copy_cell(value: Any, ddl: str) -> str:
    if value is None:
        return "\\N"
    if isinstance(value, dict) or isinstance(value, list):
        raise FastPathUnavailable("nested Mongo document is not COPY-safe")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Mongo field is not COPY-safe")
    from bson import Decimal128, Int64, ObjectId

    if isinstance(value, ObjectId):
        value = str(value)
    elif isinstance(value, Decimal128):
        value = value.to_decimal()
    elif isinstance(value, Int64):
        value = int(value)
    elif isinstance(value, datetime):
        base = (ddl or "").split("(")[0].strip().upper()
        if base == "DATE":
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).date()
            else:
                value = value.date()
        elif value.tzinfo is not None:
            raise FastPathUnavailable("timestamptz Mongo Date is not COPY-safe")
    elif isinstance(value, date) and not isinstance(value, datetime):
        pass
    return fast_copy_text_value(value)


class _MongoCopyReader:
    """Single-thread file-like: encode snapshot find() batches as COPY text."""

    def __init__(
        self,
        cursor: Any,
        source_cols: list[str],
        pg_ddls: list[str],
    ) -> None:
        self._cursor = cursor
        self._source_cols = source_cols
        self._pg_ddls = pg_ddls
        self._buf = b""
        self._done = False
        self.rows = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        want = _READ_CHUNK if size is None or size < 0 else max(int(size), 1)
        join = "\t".join
        while not self._done and len(self._buf) < want:
            batch = list(islice(self._cursor, _FIND_BATCH))
            if not batch:
                self._done = True
                break
            lines: list[str] = []
            for doc in batch:
                cells = [
                    _bson_to_copy_cell(doc.get(col), ddl)
                    for col, ddl in zip(self._source_cols, self._pg_ddls, strict=True)
                ]
                lines.append(join(cells))
                self.rows += 1
            if lines:
                self._buf += ("\n".join(lines) + "\n").encode("utf-8")
        out = self._buf[:want]
        self._buf = self._buf[want:]
        return out


def _start_snapshot_session(client: Any) -> Any:
    from pymongo import ReadPreference
    from pymongo.read_concern import ReadConcern

    try:
        session = client.start_session()
        session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            read_preference=ReadPreference.PRIMARY,
        )
        return session
    except Exception as exc:
        raise FastPathUnavailable(
            f"Mongo snapshot read concern unavailable (need replica set): {exc}"
        ) from exc


def copy_mongo_to_postgres(
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
    """COPY Mongo snapshot documents into PostgreSQL. Dest COUNT(*) is the proof."""
    del source_schema  # Mongo collections are not schema-qualified.
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_pg_copy_enabled():
        raise FastPathUnavailable("MongoDB→PostgreSQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host
    from services.copy_fast_path import _table_ref as _pg_table_ref

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        source_cfg.get("host") or source_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _pg_table_ref(dest_schema_n, dest_table)

    client, coll = mongo_collection(source_cfg, source_table)
    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    session = None
    try:
        dst_cur = dest_conn.cursor()
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        dst_cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (dest_schema_n, dest_table),
        )
        exists = dst_cur.fetchone() is not None
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count:
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
                        "mongo_read": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied PostgreSQL dest stays on the row path "
                    "(Mongo source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(
                _pg_create_sql(dest_schema_n, dest_table, pairs, pg_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        reader = _MongoCopyReader(cursor, source_cols, pg_ddls)
        col_list = ", ".join(_pg_ident(c) for c in target_cols)
        copy_sql = (
            f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
            "(FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        )
        dst_cur.copy_expert(copy_sql, reader)
        dest_conn.commit()
        if reader.rows != source_count:
            raise ValueError(
                "Mongo→PostgreSQL COPY refused: COPY rows "
                f"{reader.rows} != source snapshot {source_count}"
            )

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "Mongo→PostgreSQL COPY refused: dest COUNT(*) "
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
                "mongo_read": "snapshot_find",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("PostgreSQL dest drop after copy failure skipped", exc_info=True)
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
            logger.debug("PostgreSQL dest close skipped", exc_info=True)
