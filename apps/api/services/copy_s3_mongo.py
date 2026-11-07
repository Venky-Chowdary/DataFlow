"""S3 CSV GET → MongoDB insert_many (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, then
``insert_many`` (unordered). Dest COUNT is ``count_documents({})`` —
never ``estimatedDocumentCount``. Empty dest is insert, **not** upsert /
``mongoimport`` / ``aws s3 cp``. Occupied dest whose COUNT already
equals the source COUNT is skip-complete. Occupied dest with a different
COUNT declines. JSON/JSONL/Parquet stay on the row path (CSV is the
COPY-native wire). Nested documents are not invented from CSV text.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import mongo_type_is_copy_safe
from services.copy_mongo_sink import (
    abort_created_mongo,
    insert_many_documents,
    mongo_copy_batch,
    prepare_mongo_dest,
    prove_mongo_dest,
)
from services.copy_pg_mongo import converter_for_mongo_ddl
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_iter_delimited_rows,
    s3_list_keys,
)

logger = logging.getLogger(__name__)


def s3_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("S3_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def s3_mongo_copy_batch() -> int:
    return mongo_copy_batch("S3_MONGO_COPY_BATCH")


def copy_s3_to_mongo(
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
    """GET S3 CSV into Mongo insert_many. Dest count_documents is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_mongo_copy_enabled():
        raise FastPathUnavailable("S3→MongoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in mongo_ddls:
        if ddl and not mongo_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not Mongo COPY-safe")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        source_cfg.get("host")
        or source_cfg.get("connection_string")
        or source_cfg.get("endpoint_url")
        or ""
    ) or is_public_proxy_host(
        dest_cfg.get("host") or dest_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Mongo bulk copy not assumed")

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→Mongo COPY wire)"
            )

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    converters = [converter_for_mongo_ddl(ddl) for ddl in mongo_ddls]
    batch_size = s3_mongo_copy_batch()

    prepared = prepare_mongo_dest(
        dest_cfg=dest_cfg,
        dest_table=dest_table,
        source_count=source_count,
        replace_destination=replace_destination,
        extra_snapshot={"s3_read": "skip"},
    )
    if isinstance(prepared, FastPathResult):
        return prepared
    coll, created_here, mongo_write = prepared

    tmp_paths: list[str] = []
    inserted = 0
    try:
        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        pending: list[dict[str, Any]] = []
        for src_key in src_keys:
            ext = s3_ext(src_key)
            fd, tmp_path = tempfile.mkstemp(prefix="df-s3-mongo-", suffix=f".{ext}")
            os.close(fd)
            tmp_paths.append(tmp_path)
            client.download_file(bucket, src_key, tmp_path)
            delim = "\t" if ext == "tsv" else ","
            for cells in s3_iter_delimited_rows(tmp_path, delim):
                if len(cells) != len(converters):
                    raise ValueError(
                        f"CSV width {len(cells)} != dest columns {len(converters)}"
                    )
                pending.append(
                    {
                        name: conv(cell)
                        for name, conv, cell in zip(
                            target_cols, converters, cells, strict=True
                        )
                    }
                )
                if len(pending) >= batch_size:
                    inserted += insert_many_documents(coll, pending)
                    pending.clear()
        if pending:
            inserted += insert_many_documents(coll, pending)
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
            extra_snapshot={"s3_read": "get_csv", "shard_mode": "object"},
        )
    except Exception:
        abort_created_mongo(coll, created_here)
        raise
    finally:
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3→Mongo tempfile unlink skipped", exc_info=True)
