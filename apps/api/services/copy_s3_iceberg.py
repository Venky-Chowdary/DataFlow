"""S3 CSV GET → Iceberg catalog snapshot (cross-engine bulk).

Source COUNT is object-store artifact COUNT of the CSV (header skipped),
never ListObjects length. Payload is one GET into a tempfile, re-encoded
as Iceberg CSV (``\\N`` = NULL), then one Arrow table and one catalog
snapshot. Dest COUNT is Parquet file footers via
``destination_row_count`` / ``iceberg_mor`` — never ``scan().count()``.
Empty dest is CoW snapshot append, **not** ``MERGE INTO``. Occupied dest
whose footer COUNT already equals the source COUNT is skip-complete.
Occupied dest with a different COUNT declines. Occupancy is counted
**before** overwrite. JSON/JSONL/Parquet stay on the row path (CSV is
the COPY-native wire). Filesystem CoW declines. This is **not**
``aws s3 cp`` / GET+PUT as an S3 identity copy.

Reuses Iceberg dest helpers in ``copy_pg_iceberg`` and S3 GET helpers
in ``copy_s3_common``.

Declines (row path keeps quarantine): transforms that change values,
non-CSV source, public proxy, occupied dest with dest COUNT ≠ source,
filesystem CoW, MoR is dest-side N/A (CoW snapshot write).
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_iceberg import (
    _arrow_from_csv,
    _arrow_schema_for_iceberg,
    _iceberg_dest_count,
    _load_or_create_table,
    iceberg_copy_endpoint,
    iceberg_csv_cell,
)
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_s3_common import (
    s3_bucket,
    s3_client,
    s3_dest_count,
    s3_ext,
    s3_iter_delimited_rows,
    s3_list_keys,
    skip_complete_s3,
)

logger = logging.getLogger(__name__)


def s3_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("S3_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _s3_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "endpoint_url", "dsn")
    )


def _iceberg_proxy_fail_closed(cfg: dict[str, Any]) -> bool:
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def copy_s3_to_iceberg(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    iceberg_ddls: list[str],
    replace_destination: bool,
    dest_schema: str | None = None,
) -> FastPathResult:
    """GET S3 CSV into one Iceberg snapshot. Dest footer COUNT is the proof."""
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not s3_iceberg_copy_enabled():
        raise FastPathUnavailable("S3→Iceberg COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path

    if _s3_proxy_fail_closed(source_cfg) or _iceberg_proxy_fail_closed(dest_cfg):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    src_keys = s3_list_keys(source_cfg, source_table)
    if not src_keys:
        raise FastPathUnavailable("S3 source object missing")
    for key in src_keys:
        if s3_ext(key) not in {"csv", "tsv"}:
            raise FastPathUnavailable(
                f"source object {key!r} is not CSV/TSV (S3→Iceberg COPY wire)"
            )

    endpoint = iceberg_copy_endpoint(dest_cfg, dest_table, dest_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_count = s3_dest_count(source_cfg, source_table)
    target_cols = [p[1] for p in pairs]
    arrow_schema = _arrow_schema_for_iceberg(target_cols, iceberg_ddls)

    created_here = False
    tmp_paths: list[str] = []
    ice_csv = ""
    try:
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
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={
                        "s3_read": "skip",
                        "iceberg_write": "skip",
                        "shard_mode": "table",
                    },
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        client = s3_client(source_cfg)
        bucket = s3_bucket(source_cfg)
        handle, ice_csv = tempfile.mkstemp(prefix="df_s3_iceberg_", suffix=".csv")
        os.close(handle)
        csv_rows = 0
        with open(ice_csv, "w", encoding="utf-8", newline="") as combined:
            writer = csv.writer(combined, lineterminator="\n")
            for src_key in src_keys:
                ext = s3_ext(src_key)
                fd, tmp_path = tempfile.mkstemp(prefix="df-s3-iceberg-", suffix=f".{ext}")
                os.close(fd)
                tmp_paths.append(tmp_path)
                client.download_file(bucket, src_key, tmp_path)
                delim = "\t" if ext == "tsv" else ","
                for cells in s3_iter_delimited_rows(tmp_path, delim):
                    if len(cells) != len(target_cols):
                        raise ValueError(
                            f"CSV width {len(cells)} != dest columns {len(target_cols)}"
                        )
                    writer.writerow(iceberg_csv_cell(v) for v in cells)
                    csv_rows += 1
        if csv_rows != source_count:
            raise ValueError(
                "S3→Iceberg COPY refused: CSV rows "
                f"{csv_rows} != source artifact COUNT {source_count}"
            )
        pa_table = _arrow_from_csv(ice_csv, arrow_schema)
        if len(pa_table) != source_count:
            raise ValueError(
                "S3→Iceberg COPY refused: Arrow rows "
                f"{len(pa_table)} != source artifact COUNT {source_count}"
            )
        if existed:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            pa_table = pa_table.select(live_names)

        iceberg_write = "overwrite" if replace_destination and dest_occupied else "append"
        if iceberg_write == "overwrite":
            tbl.overwrite(pa_table)
        else:
            tbl.append(pa_table)

        dest_count = _iceberg_dest_count(endpoint)
        if dest_count != source_count:
            raise ValueError(
                "S3→Iceberg COPY refused: dest COUNT "
                f"{dest_count} != source artifact COUNT {source_count}"
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
                "s3_read": "get_csv",
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
        for path in tmp_paths:
            try:
                os.unlink(path)
            except OSError:
                logger.debug("S3 Iceberg COPY tempfile unlink skipped", exc_info=True)
        if ice_csv:
            try:
                os.unlink(ice_csv)
            except OSError:
                logger.debug("S3 Iceberg COPY csv unlink skipped", exc_info=True)
