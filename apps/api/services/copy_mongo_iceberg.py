"""MongoDB snapshot find → Iceberg catalog snapshot (cross-engine bulk).

The reverse of ``copy_iceberg_mongo``. Source COUNT is
``count_documents({})`` inside a replica-set snapshot transaction —
never ``estimatedDocumentCount``. Payload is ``find()`` in that same
snapshot, encoded as CSV, then one Arrow table and one Iceberg snapshot
commit. Dest COUNT is Parquet file footers — never ``scan().count()``.
Empty dest is CoW snapshot append, **not** ``MERGE INTO`` / upsert /
``$out``. Occupied dest whose footer COUNT already equals the source
snapshot is skip-complete. Occupied dest with a different COUNT
declines. Filesystem CoW declines. Standalone Mongo declines.

Nested documents / arrays / binary decline. ``_id`` is omitted unless
mapped. BSON Date stored as UTC midnight round-trips as Iceberg date.
This is **not** ``mongoexport``.

Reuses the Iceberg dest helpers in ``copy_pg_iceberg``.

Declines (row path keeps quarantine): transforms that change values,
nested/object/array/binData/timestamptz, public proxy, occupied dest
with dest COUNT ≠ source, snapshot read concern unavailable.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from itertools import islice
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_mongo_pg import _FIND_BATCH, _start_snapshot_session, mongo_type_is_copy_safe
from services.copy_mongo_sink import bson_to_python
from services.copy_pg_iceberg import (
    _arrow_from_csv,
    _arrow_schema_for_iceberg,
    _iceberg_dest_count,
    _load_or_create_table,
    iceberg_copy_endpoint,
    iceberg_csv_cell,
)
from services.copy_pg_mongo import mongo_collection
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)


def mongo_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("MONGO_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_mongo_to_iceberg(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    iceberg_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
    source_schema: str | None = None,
) -> FastPathResult:
    """Snapshot find Mongo into one Iceberg snapshot. Dest footer COUNT is the proof."""
    del source_schema
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not mongo_iceberg_copy_enabled():
        raise FastPathUnavailable("MongoDB→Iceberg COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        dest_cfg.get("connection_string") or dest_cfg.get("host") or ""
    ) or is_public_proxy_host(
        source_cfg.get("host") or source_cfg.get("connection_string") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(
            f"pyarrow/pyiceberg/pymongo required for Mongo→Iceberg COPY: {exc}"
        ) from exc

    for col, declared in zip([p[0] for p in pairs], iceberg_ddls, strict=True):
        if declared and not mongo_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"source column {col!r} type {declared} is not Iceberg COPY-safe"
            )

    endpoint = iceberg_copy_endpoint(dest_cfg, dest_table, dest_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    arrow_schema = _arrow_schema_for_iceberg(target_cols, iceberg_ddls)
    client, coll = mongo_collection(source_cfg, source_table)
    created_here = False
    session = None
    tmp_path = ""
    try:
        session = _start_snapshot_session(client)
        source_count = int(coll.count_documents({}, session=session))

        tbl, existed = _load_or_create_table(endpoint, arrow_schema, create=True)
        created_here = not existed
        dest_count_before = 0 if created_here else _iceberg_dest_count(endpoint)
        dest_occupied = dest_count_before > 0

        if existed:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            if {n.lower() for n in live_names} != {c.lower() for c in target_cols}:
                raise FastPathUnavailable(
                    "Iceberg dest columns do not match mapped COPY columns"
                )

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
                        "mongo_read": "skip",
                        "iceberg_write": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        projection: dict[str, int] = {c: 1 for c in source_cols}
        if "_id" not in {c.lower() for c in source_cols}:
            projection["_id"] = 0
        cursor = coll.find({}, projection, session=session, no_cursor_timeout=False)
        handle, tmp_path = tempfile.mkstemp(prefix="df_mongo_iceberg_", suffix=".csv")
        os.close(handle)
        written = 0
        with open(tmp_path, "w", encoding="utf-8", newline="") as csv_handle:
            writer = csv.writer(csv_handle, lineterminator="\n")
            while True:
                docs = list(islice(cursor, _FIND_BATCH))
                if not docs:
                    break
                for doc in docs:
                    writer.writerow(
                        iceberg_csv_cell(bson_to_python(doc.get(col), ddl))
                        for col, ddl in zip(source_cols, iceberg_ddls, strict=True)
                    )
                    written += 1
        if written != source_count:
            raise ValueError(
                "Mongo→Iceberg COPY refused: CSV rows "
                f"{written} != source snapshot {source_count}"
            )
        pa_table = _arrow_from_csv(tmp_path, arrow_schema)
        if len(pa_table) != source_count:
            raise ValueError(
                "Mongo→Iceberg COPY refused: Arrow rows "
                f"{len(pa_table)} != source snapshot {source_count}"
            )
        if existed:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            pa_table = pa_table.select(live_names)

        iceberg_write = "overwrite" if replace_destination and existed else "append"
        if iceberg_write == "overwrite":
            tbl.overwrite(pa_table)
        else:
            tbl.append(pa_table)

        dest_count = _iceberg_dest_count(endpoint)
        if dest_count != source_count:
            raise ValueError(
                "Mongo→Iceberg COPY refused: dest COUNT "
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
                "iceberg_write": iceberg_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

                parsed = parse_iceberg_catalog_config(endpoint)
                catalog = load_catalog(endpoint)
                catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
            except Exception:
                logger.debug("Iceberg dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Mongo Iceberg COPY tempfile unlink skipped", exc_info=True)
        if session is not None:
            try:
                session.abort_transaction()
            except Exception:
                logger.debug("Mongo snapshot abort skipped", exc_info=True)
            try:
                session.end_session()
            except Exception:
                logger.debug("Mongo snapshot session close skipped", exc_info=True)
