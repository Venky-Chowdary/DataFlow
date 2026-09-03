"""MongoDB snapshot find → MySQL STRICT LOAD DATA (cross-engine bulk).

The reverse of ``copy_mysql_mongo``. Source COUNT is ``count_documents({})``
inside a replica-set snapshot transaction — never
``estimatedDocumentCount``. Payload is ``find()`` in that same
snapshot. Each cell is encoded as LOAD DATA TSV into a tempfile, then
STRICT ``LOAD DATA LOCAL INFILE``. Dest ``COUNT(*)`` must equal that
snapshot COUNT.

This is **not** ``mongoexport``. Empty dest loads the snapshot once.
Occupied dest whose ``COUNT(*)`` already equals the source snapshot
COUNT is skip-complete. Occupied dest with a different COUNT declines.
Standalone Mongo (no snapshot read concern) declines.

Nested documents / arrays / binary decline (row path keeps quarantine).
``_id`` is omitted unless mapped. BSON Date stored as UTC midnight
(from MySQL DATE) round-trips as DATE.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, snapshot read concern unavailable, LOAD DATA
ineligible sessions.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session, mongo_type_is_copy_safe
from services.copy_mysql_mysql import fast_load_data_text_value
from services.copy_mysql_pg import _mysql_connect, _mysql_ident
from services.copy_pg_mongo import mongo_collection
from services.copy_pg_mysql import _mysql_create_sql, mapping_is_plain_carry

logger = logging.getLogger(__name__)


def mongo_mysql_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_MYSQL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _mysql_table_exists(cur: Any, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        (table,),
    )
    return int(cur.fetchone()[0]) > 0


def _load_tsv_into_mysql(
    dest_conn: Any,
    dst_cur: Any,
    *,
    path: str,
    table_q: str,
    columns: list[str],
) -> None:
    from connectors.mysql_load_data import (
        blocking_load_data_warnings,
        build_load_data_sql,
        mysql_load_data_session_ready,
        quote_load_data_path,
    )

    ready, why = mysql_load_data_session_ready(dst_cur, dest_conn)
    if not ready:
        raise FastPathUnavailable(why)
    load_sql = build_load_data_sql(
        table_q=table_q,
        columns=columns,
        infile_sql=quote_load_data_path(path),
    )
    dst_cur.execute(load_sql)
    dst_cur.execute("SHOW WARNINGS")
    blocked = blocking_load_data_warnings(list(dst_cur.fetchall() or []))
    if blocked:
        raise FastPathUnavailable(f"LOAD DATA warnings: {blocked[0]}")


def _bson_to_load_data_cell(value: Any, ddl: str) -> str:
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
    return fast_load_data_text_value(value)


def _snapshot_find_to_load_data_tsv(
    cursor: Any,
    source_cols: list[str],
    mysql_ddls: list[str],
    path: str,
) -> int:
    join = "\t".join
    written = 0
    with open(path, "wb", buffering=1 << 20) as writer:
        while True:
            batch = list(islice(cursor, _FIND_BATCH))
            if not batch:
                break
            lines: list[str] = []
            for doc in batch:
                cells = [
                    _bson_to_load_data_cell(doc.get(col), ddl)
                    for col, ddl in zip(source_cols, mysql_ddls, strict=True)
                ]
                lines.append(join(cells))
                written += 1
            if lines:
                writer.write(("\n".join(lines) + "\n").encode("utf-8"))
    return written


def copy_mongo_to_mysql(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """LOAD Mongo snapshot documents into MySQL. Dest COUNT(*) is the proof."""
    del source_schema  # Mongo collections are not schema-qualified.
    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_mysql_copy_enabled():
        raise FastPathUnavailable("MongoDB→MySQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

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
    dest_q = _mysql_ident(dest_table)

    client, coll = mongo_collection(source_cfg, source_table)
    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    session = None
    tmp_path = ""
    dst_cur = dest_conn.cursor()
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
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
                        "load_data": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied MySQL dest stays on the row path "
                    "(Mongo source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(_mysql_create_sql(dest_table, pairs, mysql_ddls, []))
            dest_conn.commit()
            created_here = True

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        handle, tmp_path = tempfile.mkstemp(prefix="df_mongo_mysql_", suffix=".tsv")
        os.close(handle)
        written = _snapshot_find_to_load_data_tsv(
            cursor, source_cols, mysql_ddls, tmp_path
        )
        if written != source_count:
            raise ValueError(
                "Mongo→MySQL COPY refused: TSV rows "
                f"{written} != source snapshot {source_count}"
            )
        _load_tsv_into_mysql(
            dest_conn,
            dst_cur,
            path=tmp_path,
            table_q=dest_q,
            columns=target_cols,
        )
        dest_conn.commit()

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "Mongo→MySQL COPY refused: dest COUNT(*) "
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
                "load_data": "tempfile",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("MySQL dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Mongo→MySQL TSV unlink skipped", exc_info=True)
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
            logger.debug("MySQL dest close skipped", exc_info=True)
