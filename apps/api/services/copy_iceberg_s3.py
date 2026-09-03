"""Iceberg snapshot Parquet → S3 CSV PUT (cross-engine bulk).

The reverse of ``copy_s3_iceberg``. Source COUNT is Iceberg file footers
via ``destination_row_count`` / ``iceberg_mor`` — never
``scan().count()``. Payload is current-snapshot data files read as
Arrow (no ``scan().to_arrow()``), encoded as CSV (HEADER, ``\\N`` =
NULL), then ``upload_file``. Dest COUNT is object-store artifact COUNT
of that CSV (header skipped) — never writer PUT ack, never ListObjects
length. Empty dest is PUT, **not** ``MERGE INTO`` / ``aws s3 cp``.
Occupied dest whose COUNT already equals the source footer COUNT is
skip-complete. Occupied dest with a different COUNT declines.
Occupancy is counted **before** delete. Dest key must be ``.csv`` /
``.tsv``. Iceberg MoR (delete files) declines. Filesystem CoW declines.
JSON dest keys decline.

Reuses ``_arrow_from_iceberg_files`` from ``copy_iceberg_pg``.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
occupied dest with dest COUNT ≠ source, non-CSV dest key.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_iceberg_pg import (
    _ARROW_BATCH,
    _arrow_from_iceberg_files,
    _iceberg_source_count,
)
from services.copy_pg_iceberg import iceberg_copy_endpoint
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
from services.copy_s3_iceberg import _iceberg_proxy_fail_closed, _s3_proxy_fail_closed

logger = logging.getLogger(__name__)


def iceberg_s3_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_S3_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def iceberg_value_to_s3(value: Any) -> Any:
    """Bind an Iceberg/Arrow Python value for the S3 CSV wire."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "timestamptz Iceberg value is not S3 COPY-safe"
            )
        return value
    if isinstance(value, date):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Iceberg field is not S3 COPY-safe")
    if isinstance(value, UUID):
        raise FastPathUnavailable("uuid Iceberg field is not S3 COPY-safe")
    if isinstance(value, (dict, list, tuple)):
        raise FastPathUnavailable("nested Iceberg value is not S3 COPY-safe")
    return value


def _arrow_rows(table: Any):
    for batch in table.to_batches(max_chunksize=_ARROW_BATCH):
        cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
        if not cols:
            continue
        for row in zip(*cols):
            yield tuple(iceberg_value_to_s3(v) for v in row)


def copy_iceberg_to_s3(
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
    """Encode Iceberg snapshot files as one S3 CSV. Dest artifact COUNT is the proof."""
    if not pairs or len(pairs) != len(s3_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_s3_copy_enabled():
        raise FastPathUnavailable("Iceberg→S3 COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path

    if _iceberg_proxy_fail_closed(source_cfg) or _s3_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: S3 bulk copy not assumed")

    ext = s3_ext(dest_table)
    if ext not in {"csv", "tsv"}:
        raise FastPathUnavailable("Iceberg→S3 COPY writes CSV/TSV")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    delim = "\t" if ext == "tsv" else ","

    created_here = False
    tmp_path = ""
    try:
        source_count = _iceberg_source_count(endpoint)

        s3_ensure_bucket(dest_cfg)
        dest_count_before = s3_dest_count(dest_cfg, dest_table)
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "iceberg_read": "skip",
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

        pa_table = _arrow_from_iceberg_files(endpoint, source_cols)
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→S3 COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)

        fd, tmp_path = tempfile.mkstemp(prefix="df-iceberg-s3-", suffix=f".{ext}")
        os.close(fd)
        csv_rows = s3_write_delimited(
            tmp_path, target_cols, _arrow_rows(pa_table), delim
        )
        if csv_rows != source_count:
            raise ValueError(
                "Iceberg→S3 COPY refused: CSV rows "
                f"{csv_rows} != source footer COUNT {source_count}"
            )
        client = s3_client(dest_cfg)
        client.upload_file(tmp_path, s3_bucket(dest_cfg), dest_table)
        dest_count = s3_dest_count(dest_cfg, dest_table)
        if dest_count != source_count:
            raise ValueError(
                "Iceberg→S3 COPY refused: dest COUNT "
                f"{dest_count} != source footer COUNT {source_count}"
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
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "object",
                "iceberg_read": "snapshot_parquet",
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
                logger.debug("Iceberg→S3 tempfile unlink skipped", exc_info=True)
