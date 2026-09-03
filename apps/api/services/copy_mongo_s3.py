"""MongoDB snapshot find → S3 CSV PUT (cross-engine bulk).

Source COUNT is ``count_documents({})`` inside a replica-set snapshot
transaction — never ``estimatedDocumentCount``. Payload is ``find()`` in
that same snapshot encoded as CSV (HEADER, ``\\N`` = NULL, ``""`` =
empty string), then ``upload_file``. Dest COUNT is object-store artifact
COUNT of that CSV (header skipped) — never writer PUT ack, never
ListObjects length. Empty dest is PUT, **not** ``mongoexport`` /
``aws s3 cp``. Occupied dest whose COUNT already equals the source
snapshot is skip-complete. Occupied dest with a different COUNT
declines. Dest key must be ``.csv`` / ``.tsv``. Nested documents /
binary decline. DATE is ISO calendar day (BSON Date at UTC midnight).

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, non-CSV dest key, snapshot read concern
unavailable.
"""

from __future__ import annotations

import logging
import os
import tempfile
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session, mongo_type_is_copy_safe
from services.copy_mongo_sink import bson_to_python
from services.copy_pg_mongo import mongo_collection
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_delete_keys,
    s3_dest_count,
    s3_ensure_bucket,
    s3_ext,
    s3_list_keys,
    s3_write_delimited,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def mongo_s3_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mongo_s3_type_is_copy_safe(declared: str) -> bool:
    return mongo_type_is_copy_safe(declared)


def _snapshot_docs_as_rows(
    cursor: Any,
    source_cols: list[str],
    ddls: list[str],
):
    while True:
        batch = list(islice(cursor, _FIND_BATCH))
        if not batch:
            break
        for doc in batch:
            yield tuple(
                bson_to_python(doc.get(col), ddl)
                for col, ddl in zip(source_cols, ddls, strict=True)
            )


def copy_mongo_to_s3(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    s3_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """SELECT CSV from a Mongo snapshot into one S3 object. Dest COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_s3_copy_enabled():
        raise FastPathUnavailable("MongoDB→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        source_cfg.get("host") or source_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(
        dest_cfg.get("host")
        or dest_cfg.get("connection_string")
        or dest_cfg.get("endpoint_url")
        or ""
    ):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    ext = s3_ext(dest_table)
    if ext not in {"csv", "tsv"}:
        raise FastPathUnavailable("MongoDB→S3 COPY writes CSV/TSV")

    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    delim = "\t" if ext == "tsv" else ","

    client, coll = mongo_collection(source_cfg, source_table)
    created_here = False
    session = None
    tmp_path = ""
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        s3_ensure_bucket(dest_cfg)
        dest_count_before = s3_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "mongo_read": "skip",
                        "s3_write": "skip",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied S3 dest stays on the row path "
                "(identity COPY would duplicate)"
            )

        if replace_destination:
            s3_delete_keys(dest_cfg, s3_list_keys(dest_cfg, dest_table))
        created_here = dest_count_before == 0 or replace_destination

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)

        fd, tmp_path = tempfile.mkstemp(prefix="df-mongo-s3-", suffix=f".{ext}")
        os.close(fd)
        csv_rows = s3_write_delimited(
            tmp_path,
            target_cols,
            _snapshot_docs_as_rows(cursor, source_cols, s3_ddls),
            delim,
        )
        if csv_rows != source_count:
            raise ValueError(
                "MongoDB→S3 COPY refused: CSV rows "
                f"{csv_rows} != source snapshot {source_count}"
            )
        s3_client(dest_cfg).upload_file(tmp_path, s3_bucket(dest_cfg), dest_table)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "MongoDB→S3 COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        s3_write = "overwrite" if replace_destination and dest_occupied else "insert"
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "mongo_read": "snapshot_find",
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "object",
                "s3_write": s3_write,
                "s3_key": dest_table,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                s3_delete_keys(dest_cfg, [dest_table] + s3_list_keys(dest_cfg, dest_table))
            except Exception:
                logger.debug("S3 dest delete after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Mongo→S3 tempfile unlink skipped", exc_info=True)
        if session is not None:
            try:
                session.abort_transaction()
            except Exception:
                logger.debug("Mongo snapshot abort skipped", exc_info=True)
            try:
                session.end_session()
            except Exception:
                logger.debug("Mongo snapshot session close skipped", exc_info=True)
