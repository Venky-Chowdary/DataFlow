"""Oracle SELECT → Iceberg catalog snapshot (cross-engine bulk).

This host has no client ``sqlldr`` / Data Pump. One
``LOCK TABLE src IN SHARE MODE`` transaction streams ``SELECT`` into a
CSV tempfile; Arrow reads that CSV; one Iceberg snapshot commit
follows. Dest COUNT is Parquet file footers via
``destination_row_count`` / ``iceberg_mor`` — never ``scan().count()``.
Empty dest is CoW snapshot append, **not** ``MERGE INTO``. Occupied dest
whose footer COUNT already equals the source snapshot is skip-complete.
Occupied dest with a different COUNT declines. Filesystem CoW declines.
Iceberg catalog commits are snapshot-isolated, so this COPY is serial.

Oracle ``VARCHAR2`` stores ``''`` as ``NULL`` (engine law). Source empty
strings therefore arrive as ``None`` and CSV as ``\\N``. That is not a
row drop.

Reuses the Iceberg dest helpers in ``copy_pg_iceberg``.

Declines (row path keeps quarantine): transforms that change values,
BLOB/RAW/XMLTYPE/SDO_GEOMETRY, public proxy, occupied dest with dest
COUNT ≠ source.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_oracle_oracle import (
    _count as _ora_count,
    _ora_table_pk_and_types,
    _oracle_connect,
    _schema_of as _ora_schema_of,
    _table_ref as _ora_table_ref,
)
from services.copy_oracle_pg import _select_sql, _tune_fetch, oracle_type_is_copy_safe
from services.copy_pg_iceberg import (
    _arrow_from_csv,
    _arrow_schema_for_iceberg,
    _iceberg_dest_count,
    _load_or_create_table,
    iceberg_copy_endpoint,
    iceberg_csv_cell,
)
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_FETCH_BATCH = 8192


def oracle_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("ORACLE_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _select_to_csv(
    source_conn: Any,
    select_sql: str,
    path: str,
    params: list[Any] | None = None,
) -> int:
    written = 0
    cur = source_conn.cursor()
    _tune_fetch(cur)
    try:
        if params:
            cur.execute(select_sql, params)
        else:
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
            logger.debug("Oracle stream cursor close skipped", exc_info=True)


def copy_oracle_to_iceberg(
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
    """SELECT Oracle into one Iceberg snapshot. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_iceberg_copy_enabled():
        raise FastPathUnavailable("Oracle→Iceberg COPY disabled")
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
    src_schema = _ora_schema_of(source_cfg, source_schema)
    source_ref = _ora_table_ref(src_schema, source_table)
    arrow_schema = _arrow_schema_for_iceberg(target_cols, iceberg_ddls)

    source_conn = _oracle_connect(source_cfg)
    created_here = False
    tmp_path = ""
    src_cur = source_conn.cursor()
    try:
        pk_cols, live = _ora_table_pk_and_types(
            src_cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not oracle_type_is_copy_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Iceberg COPY-safe"
                )
        src_cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _ora_count(src_cur, source_ref)
        select_sql = _select_sql(source_ref, source_cols, "")
        src_cur.close()
        src_cur = None  # type: ignore[assignment]

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
                        "oracle_lock": "share",
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "iceberg_write": "skip",
                        "source_pk": list(pk_cols or []),
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        handle, tmp_path = tempfile.mkstemp(prefix="df_ora_iceberg_", suffix=".csv")
        os.close(handle)
        csv_rows = _select_to_csv(source_conn, select_sql, tmp_path)
        if csv_rows != source_count:
            raise ValueError(
                "Oracle→Iceberg COPY refused: CSV rows "
                f"{csv_rows} != source snapshot {source_count}"
            )
        pa_table = _arrow_from_csv(tmp_path, arrow_schema)
        if len(pa_table) != source_count:
            raise ValueError(
                "Oracle→Iceberg COPY refused: Arrow rows "
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
                "Oracle→Iceberg COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("Oracle source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "oracle_lock": "share",
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "iceberg_write": iceberg_write,
                "source_pk": list(pk_cols or []),
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
                logger.debug("Oracle Iceberg COPY tempfile unlink skipped", exc_info=True)
        try:
            if src_cur is not None:
                src_cur.close()
        except Exception:
            logger.debug("Oracle source cursor close skipped", exc_info=True)
        try:
            source_conn.rollback()
        except Exception:
            logger.debug("Oracle source rollback skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("Oracle source close skipped", exc_info=True)
