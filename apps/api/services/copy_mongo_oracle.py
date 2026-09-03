"""MongoDB snapshot find → Oracle executemany (cross-engine bulk).

The reverse of ``copy_oracle_mongo``. Source COUNT is
``count_documents({})`` inside a replica-set snapshot transaction —
never ``estimatedDocumentCount``. Payload is ``find()`` in that same
snapshot. Batches are bound with ``oracledb.executemany``. Dest
``COUNT(*)`` must equal that snapshot COUNT.

This is **not** ``sqlldr`` / Data Pump and **not** ``mongoexport``.
Oracle VARCHAR2 cannot store empty string: ``''`` IS NULL. That is
engine law, not a silent row drop. Empty-string cells from Mongo are
bound as NULL and counted in ``empty_string_as_null_cells``. Rows
still land.

Empty dest loads the snapshot once. Occupied dest whose ``COUNT(*)``
already equals the source snapshot COUNT is skip-complete. Occupied
dest with a different COUNT declines. Standalone Mongo declines.

Nested documents / arrays / binary decline. ``_id`` is omitted unless
mapped. BSON Date stored as UTC midnight (from Oracle DATE) round-trips
as DATE.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, snapshot read concern unavailable.
"""

from __future__ import annotations

import logging
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session
from services.copy_mongo_sink import bson_to_python
from services.copy_mysql_oracle import python_bind_for_ora_ddl
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _create_sql as _ora_create_sql,
    _drop_sql as _ora_drop_sql,
    _ident as _ora_ident,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_exists as _ora_table_exists,
    _table_ref as _ora_table_ref,
)
from services.copy_pg_mongo import mongo_collection
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_pg_oracle import pg_oracle_copy_batch

logger = logging.getLogger(__name__)


def mongo_oracle_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_ORACLE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_mongo_to_oracle(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    oracle_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """Bind Mongo snapshot documents into Oracle. Dest COUNT(*) is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_oracle_copy_enabled():
        raise FastPathUnavailable("MongoDB→Oracle COPY disabled")
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
        import oracledb  # noqa: F401
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"oracledb/pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dst_schema = _ora_schema_of(dest_cfg, dest_schema)
    dest_ref = _ora_table_ref(dst_schema, dest_table)
    col_sql = ", ".join(_ora_ident(c) for c in target_cols)
    placeholders = ", ".join(f":{i + 1}" for i in range(len(target_cols)))
    insert_sql = (
        f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    )
    coerced = [0]
    converters = [python_bind_for_ora_ddl(ddl, coerced) for ddl in oracle_ddls]
    batch_size = pg_oracle_copy_batch()

    client, coll = mongo_collection(source_cfg, source_table)
    dest_conn = _oracle_connect(dest_cfg)
    created_here = False
    session = None
    dst_cur = dest_conn.cursor()
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        exists = _ora_table_exists(dst_cur, dst_schema, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(_ora_drop_sql(dest_ref))
            dest_conn.commit()
            exists = False
        if exists:
            dest_count_before = _ora_count(dst_cur, dest_ref)
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
                        "empty_string_as_null_cells": 0,
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied Oracle dest stays on the row path "
                    "(Mongo source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(
                _ora_create_sql(dest_ref, dest_table, pairs, oracle_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        copied = 0
        batch: list[tuple[Any, ...]] = []
        while True:
            docs = list(islice(cursor, _FIND_BATCH))
            if not docs:
                break
            for doc in docs:
                python_row = [
                    bson_to_python(doc.get(col), ddl)
                    for col, ddl in zip(source_cols, oracle_ddls, strict=True)
                ]
                batch.append(
                    tuple(conv(val) for conv, val in zip(converters, python_row))
                )
                if len(batch) >= batch_size:
                    dst_cur.executemany(insert_sql, batch)
                    copied += len(batch)
                    batch.clear()
        if batch:
            dst_cur.executemany(insert_sql, batch)
            copied += len(batch)
        dest_conn.commit()
        if copied != source_count:
            raise ValueError(
                "Mongo→Oracle COPY refused: bound rows "
                f"{copied} != source snapshot {source_count}"
            )
        dest_count = _ora_count(dst_cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "Mongo→Oracle COPY refused: dest COUNT(*) "
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
                "empty_string_as_null_cells": coerced[0],
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(_ora_drop_sql(dest_ref))
                dest_conn.commit()
            except Exception:
                logger.debug("Oracle dest drop after copy failure skipped", exc_info=True)
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
            dst_cur.close()
        except Exception:
            logger.debug("Oracle dest cursor close skipped", exc_info=True)
        try:
            dest_conn.close()
        except Exception:
            logger.debug("Oracle dest close skipped", exc_info=True)
