"""SQLite SELECT → Iceberg catalog snapshot (cross-engine bulk).

One ``BEGIN`` on the source file streams ``SELECT`` into a CSV tempfile;
Arrow reads that CSV; one Iceberg snapshot commit follows. Dest COUNT is
Parquet file footers via ``destination_row_count`` / ``iceberg_mor`` —
never ``scan().count()``. Empty dest is CoW snapshot append, **not**
``MERGE INTO``. Occupied dest whose footer COUNT already equals the
source COUNT is skip-complete. Occupied dest with a different COUNT
declines. Filesystem CoW declines. ``:memory:`` / BLOB / JSON /
DATETIME / TIMESTAMP decline. DATE ISO text or a calendar day is
COPY-safe (Iceberg date, or string when the mapping is TEXT).

Reuses the Iceberg dest helpers in ``copy_pg_iceberg``.

Declines (row path keeps quarantine): transforms that change values,
BLOB/JSON/DATETIME, public proxy, occupied dest with dest COUNT ≠
source, ``:memory:``, filesystem CoW.
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
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192
_UNSAFE_SQLITE_ICEBERG_BASES = frozenset({
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "JSON",
    "JSONB",
})


def sqlite_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("SQLITE_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sqlite_iceberg_type_is_copy_safe(declared: str) -> bool:
    if not sqlite_type_is_copy_safe(declared):
        return False
    base = (declared or "").strip().upper().replace(" ", "").split("(", 1)[0]
    return base not in _UNSAFE_SQLITE_ICEBERG_BASES


def _select_to_csv(source_conn: Any, select_sql: str, path: str) -> int:
    written = 0
    cur = source_conn.cursor()
    try:
        cur.execute(select_sql)
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            while True:
                rows = cur.fetchmany(_FETCH_BATCH)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(iceberg_csv_cell(v) for v in row)
                    written += 1
        return written
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("SQLite stream cursor close skipped", exc_info=True)


def copy_sqlite_to_iceberg(
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
    """SELECT SQLite into one Iceberg snapshot. Dest footer COUNT is the proof."""
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlite_iceberg_copy_enabled():
        raise FastPathUnavailable("SQLite→Iceberg COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or dest_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    sqlite_resolved_path(source_cfg)
    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(dest_cfg, dest_table, dest_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_ref = sqlite_ident(source_table)
    src_col_sql = ", ".join(sqlite_ident(c) for c in source_cols)
    select_sql = f"SELECT {src_col_sql} FROM {src_ref}"  # nosec B608
    arrow_schema = _arrow_schema_for_iceberg(target_cols, iceberg_ddls)

    source_conn = sqlite_connect(source_cfg)
    created_here = False
    tmp_path = ""
    try:
        source_conn.execute("BEGIN")
        live = sqlite_pragma_types(source_conn, source_table)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower())
            if declared is None:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not sqlite_iceberg_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Iceberg COPY-safe"
                )
        source_count = int(
            source_conn.execute(f"SELECT COUNT(*) FROM {src_ref}").fetchone()[0]  # nosec B608
        )

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
                try:
                    source_conn.rollback()
                except Exception:
                    logger.debug("SQLite source rollback on skip skipped", exc_info=True)
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"sqlite_read": "skip", "iceberg_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        handle, tmp_path = tempfile.mkstemp(prefix="df_sqlite_iceberg_", suffix=".csv")
        os.close(handle)
        csv_rows = _select_to_csv(source_conn, select_sql, tmp_path)
        if csv_rows != source_count:
            raise ValueError(
                "SQLite→Iceberg COPY refused: CSV rows "
                f"{csv_rows} != source COUNT {source_count}"
            )
        pa_table = _arrow_from_csv(tmp_path, arrow_schema)
        if len(pa_table) != source_count:
            raise ValueError(
                "SQLite→Iceberg COPY refused: Arrow rows "
                f"{len(pa_table)} != source COUNT {source_count}"
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
                "SQLite→Iceberg COPY refused: dest COUNT "
                f"{dest_count} != source COUNT {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("SQLite source commit skipped", exc_info=True)
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
                "sqlite_read": "select",
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
                logger.debug("SQLite Iceberg COPY tempfile unlink skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("SQLite source close skipped", exc_info=True)
