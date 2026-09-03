"""Iceberg snapshot Parquet → Iceberg catalog snapshot (identity bulk).

Same-catalog clone of the current snapshot: source COUNT is file footers
via ``destination_row_count`` / ``iceberg_mor`` — never ``scan().count()``.
Payload is current-snapshot data files read as Arrow (no
``scan().to_arrow()``), then one CoW dest snapshot (``append`` on empty,
``overwrite`` on replace). Dest COUNT is file footers. That is **not**
``MERGE INTO``. Dest writes **new** data files under the dest table
location — source files are not shared, so dropping dest cannot delete
source.

Same catalog + namespace + table declines (identity COPY onto itself is
not a transfer). Occupied dest whose footer COUNT already equals the
source footer COUNT is skip-complete. Occupied dest with a different
COUNT declines. Iceberg MoR (delete files) declines. Filesystem CoW
declines. Nested list/map/struct stay on the row path until nested
identity is proven (same gate as Iceberg→SQL).

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
copy onto the same table, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_iceberg_pg import (
    _arrow_from_iceberg_files,
    _iceberg_source_count,
    iceberg_type_is_copy_safe,
)
from services.copy_pg_iceberg import (
    _iceberg_dest_count,
    _load_or_create_table,
    iceberg_copy_endpoint,
)
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)


def iceberg_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _norm_uri(value: str) -> str:
    raw = (value or "").strip().lower().rstrip("/")
    return raw.replace("://localhost", "://127.0.0.1").replace("localhost", "127.0.0.1")


def iceberg_table_key(endpoint: dict[str, Any]) -> tuple[str, str, tuple[str, ...], str]:
    """Catalog URI + warehouse + namespace + table. Same key is not a transfer."""
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    parsed = parse_iceberg_catalog_config(endpoint)
    uri = _norm_uri(
        str(parsed.get("connection_string") or endpoint.get("connection_string") or "")
    )
    warehouse = _norm_uri(str(parsed.get("warehouse") or endpoint.get("warehouse") or ""))
    namespace = tuple(str(part).strip().lower() for part in parsed["namespace"])
    table = str(parsed.get("table_name") or endpoint.get("table") or "").strip().lower()
    return (uri, warehouse, namespace, table)


def copy_iceberg_to_iceberg(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    iceberg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Bind Iceberg snapshot files into a dest CoW snapshot. Dest footer COUNT is the proof."""
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_iceberg_copy_enabled():
        raise FastPathUnavailable("Iceberg→Iceberg COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ) or is_public_proxy_host(
        dest_cfg.get("connection_string") or dest_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    for col, declared in zip([p[0] for p in pairs], iceberg_ddls, strict=True):
        if declared and not iceberg_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"source column {col!r} type {declared} is not Iceberg COPY-safe"
            )

    src_endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    dst_endpoint = iceberg_copy_endpoint(dest_cfg, dest_table, dest_schema)
    if iceberg_table_key(src_endpoint) == iceberg_table_key(dst_endpoint):
        raise FastPathUnavailable(
            "Iceberg COPY onto the same table stays on the row path"
        )

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(
            f"pyarrow/pyiceberg required for Iceberg→Iceberg COPY: {exc}"
        ) from exc

    try:
        src_path = resolve_iceberg_write_path(src_endpoint)
        dst_path = resolve_iceberg_write_path(dst_endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if src_path != "catalog" or dst_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    created_here = False
    try:
        source_count = _iceberg_source_count(src_endpoint)
        existed = False
        tbl = None
        try:
            tbl, existed = _load_or_create_table(dst_endpoint, None, create=False)
        except FastPathUnavailable:
            tbl = None
            existed = False

        dest_count_before = 0 if not existed else _iceberg_dest_count(dst_endpoint)
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
                        "iceberg_read": "skip",
                        "iceberg_write": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        pa_table = _arrow_from_iceberg_files(src_endpoint, source_cols)
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→Iceberg COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)

        if not existed:
            tbl, existed_now = _load_or_create_table(
                dst_endpoint, pa_table.schema, create=True
            )
            created_here = not existed_now
        else:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            pa_table = pa_table.select(live_names)

        iceberg_write = "overwrite" if replace_destination and existed else "append"
        if iceberg_write == "overwrite":
            tbl.overwrite(pa_table)
        else:
            tbl.append(pa_table)

        dest_count = _iceberg_dest_count(dst_endpoint)
        if dest_count != source_count:
            raise ValueError(
                "Iceberg→Iceberg COPY refused: dest COUNT "
                f"{dest_count} != source footer COUNT {source_count}"
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
                "iceberg_read": "snapshot_parquet",
                "iceberg_write": iceberg_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

                parsed = parse_iceberg_catalog_config(dst_endpoint)
                catalog = load_catalog(dst_endpoint)
                catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
            except Exception:
                logger.debug("Iceberg dest drop after copy failure skipped", exc_info=True)
        raise
