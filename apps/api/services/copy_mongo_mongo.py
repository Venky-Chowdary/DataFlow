"""MongoDB snapshot find → MongoDB insert_many (identity bulk).

Source COUNT is ``count_documents({})`` inside a replica-set snapshot
transaction — never ``estimatedDocumentCount``. Payload is ``find()`` in
that same snapshot. Dest writes use a **separate** client session so
aborting the source snapshot cannot roll back landed documents. Dest
COUNT is ``count_documents({})``. Empty dest is insert, **not** upsert /
``ReplaceOne`` / ``$out`` (``$out`` is unsupported in a transaction).

Same database + collection declines (identity COPY onto itself is not a
transfer). Occupied dest whose COUNT already equals the source snapshot
is skip-complete. Occupied dest with a different COUNT declines.
Standalone Mongo declines.

Nested documents / arrays / binary **are** identity-safe here — BSON
passes through. ``_id`` is omitted unless mapped (Mongo assigns new
ObjectIds). This is **not** ``mongoexport`` / ``mongoimport``.

Declines (row path keeps quarantine): transforms that change values,
copy onto the same collection, public proxy, occupied dest with dest
COUNT ≠ source, snapshot read concern unavailable.
"""

from __future__ import annotations

import logging
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session, mongo_type_is_copy_safe
from services.copy_mongo_sink import (
    abort_created_mongo,
    insert_many_documents,
    mongo_copy_batch,
    prepare_mongo_dest,
    prove_mongo_dest,
)
from services.copy_pg_mongo import mongo_collection, mongo_database_name
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_MONGO_MONGO_EXTRA_SAFE = frozenset({
    "object",
    "array",
    "bindata",
    "binary",
    "timestamp",
    "timestamptz",
})


def mongo_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mongo_mongo_type_is_copy_safe(declared: str) -> bool:
    if mongo_type_is_copy_safe(declared):
        return True
    raw = (declared or "").strip().lower().replace(" ", "")
    if not raw:
        return True
    base = raw.split("(", 1)[0]
    if base in {"javascript", "regex", "minkey", "maxkey", "dbref"}:
        return False
    return base in _MONGO_MONGO_EXTRA_SAFE


def _mongo_endpoint_key(cfg: dict[str, Any], collection: str) -> tuple[str, int, str, str]:
    host = str(cfg.get("host") or "").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        host = "127.0.0.1"
    try:
        port = int(cfg.get("port") or 27017)
    except (TypeError, ValueError):
        port = 27017
    db = mongo_database_name(cfg).strip().lower()
    name = str(collection or "").strip()
    return (host, port, db, name)


def copy_mongo_to_mongo(
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
    """Snapshot find on src, insert_many on dest. Dest count_documents is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_mongo_copy_enabled():
        raise FastPathUnavailable("MongoDB→MongoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    if _mongo_endpoint_key(source_cfg, source_table) == _mongo_endpoint_key(
        dest_cfg, dest_table
    ):
        raise FastPathUnavailable("Mongo COPY onto the same collection stays on the row path")

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_client, src_coll = mongo_collection(source_cfg, source_table)
    created_here = False
    dest_coll = None
    session = None
    try:
        session = _start_snapshot_session(src_client)
        source_count = int(src_coll.count_documents({}, session=session))
        extra = {"mongo_read": "snapshot_find"}
        prepared = prepare_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            replace_destination=replace_destination,
            extra_snapshot=extra,
        )
        if isinstance(prepared, FastPathResult):
            return prepared
        dest_coll, created_here, mongo_write = prepared

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = src_coll.find({}, projection, session=session, no_cursor_timeout=False)
        batch_size = mongo_copy_batch("MONGO_MONGO_COPY_BATCH")
        inserted = 0
        batch: list[dict[str, Any]] = []
        while True:
            docs = list(islice(cursor, _FIND_BATCH))
            if not docs:
                break
            for doc in docs:
                batch.append(
                    {target: doc.get(source) for source, target in zip(source_cols, target_cols)}
                )
                if len(batch) >= batch_size:
                    inserted += insert_many_documents(dest_coll, batch)
                    batch.clear()
        if batch:
            inserted += insert_many_documents(dest_coll, batch)
        extra_write = {**extra, "mongo_write": mongo_write}
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
            extra_snapshot=extra_write,
        )
    except Exception:
        abort_created_mongo(dest_coll, created_here)
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
