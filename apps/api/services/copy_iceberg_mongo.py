"""Iceberg snapshot Parquet → MongoDB insert_many (cross-engine bulk).

Source COUNT is Iceberg file footers via ``destination_row_count`` /
``iceberg_mor`` — never ``scan().count()``. Payload is current-snapshot
data files read as Arrow (no ``scan().to_arrow()``). Python values
become BSON documents and ``insert_many`` (unordered) loads them. Dest
COUNT is ``count_documents({})`` — never ``estimatedDocumentCount``.
Empty dest is insert, **not** upsert / ``ReplaceOne`` / ``MERGE INTO``.
Occupied dest whose COUNT already equals the source footer COUNT is
skip-complete. Occupied dest with a different COUNT declines. Iceberg
MoR (delete files) declines. Filesystem CoW declines.

DATE is BSON Date at UTC midnight. TIMESTAMP / TIMESTAMPTZ decline
(BSON Date would invent UTC). list/map/struct stay on the row path
(same as Iceberg→SQL until nested identity is proven). This is **not**
``mongoimport``.

Reuses ``_arrow_from_iceberg_files`` and the canonical Mongo dest writer.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/timestamp/list/map/struct, MoR snapshots,
public proxy, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_iceberg_pg import (
    _ARROW_BATCH,
    _arrow_from_iceberg_files,
    _iceberg_source_count,
    iceberg_type_is_copy_safe,
)
from services.copy_mongo_sink import (
    abort_created_mongo,
    insert_many_documents,
    mongo_copy_batch,
    prepare_mongo_dest,
    prove_mongo_dest,
    sql_value_to_bson,
)
from services.copy_pg_iceberg import iceberg_copy_endpoint
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_UNSAFE_ICEBERG_MONGO = frozenset({
    "timestamp",
    "timestamp_ntz",
    "time",
})


def iceberg_mongo_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_MONGO_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def iceberg_mongo_type_is_copy_safe(declared: str) -> bool:
    if not iceberg_type_is_copy_safe(declared):
        return False
    raw = (declared or "").strip().lower().replace(" ", "")
    base = raw.split("(", 1)[0]
    return base not in _UNSAFE_ICEBERG_MONGO


def _arrow_insert_many(
    coll: Any,
    table: Any,
    target_cols: list[str],
    batch_size: int,
) -> int:
    inserted = 0
    batch: list[dict[str, Any]] = []
    for arrow_batch in table.to_batches(max_chunksize=_ARROW_BATCH):
        cols = [arrow_batch.column(i).to_pylist() for i in range(arrow_batch.num_columns)]
        if not cols:
            continue
        for row in zip(*cols):
            batch.append(
                {
                    name: sql_value_to_bson(val)
                    for name, val in zip(target_cols, row, strict=True)
                }
            )
            if len(batch) >= batch_size:
                inserted += insert_many_documents(coll, batch)
                batch.clear()
    if batch:
        inserted += insert_many_documents(coll, batch)
    return inserted


def copy_iceberg_to_mongo(
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
    """Bind Iceberg snapshot files into Mongo insert_many. Dest count_documents is the proof."""
    if not pairs or len(pairs) != len(mongo_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_mongo_copy_enabled():
        raise FastPathUnavailable("Iceberg→MongoDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or "") or is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(
            f"pyarrow/pyiceberg/pymongo required for Iceberg→Mongo COPY: {exc}"
        ) from exc

    endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    created_here = False
    coll = None
    try:
        source_count = _iceberg_source_count(endpoint)
        extra = {"iceberg_read": "snapshot_parquet"}
        prepared = prepare_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            replace_destination=replace_destination,
            extra_snapshot={**extra, "iceberg_read": "skip"},
        )
        if isinstance(prepared, FastPathResult):
            return prepared
        coll, created_here, mongo_write = prepared
        pa_table = _arrow_from_iceberg_files(
            endpoint,
            source_cols,
            type_is_copy_safe=iceberg_mongo_type_is_copy_safe,
        )
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→Mongo COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)
        inserted = _arrow_insert_many(
            coll,
            pa_table,
            target_cols,
            mongo_copy_batch("ICEBERG_MONGO_COPY_BATCH"),
        )
        return prove_mongo_dest(
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            source_count=source_count,
            inserted=inserted,
            mongo_write=mongo_write,
            extra_snapshot=extra,
        )
    except Exception:
        abort_created_mongo(coll, created_here)
        raise
