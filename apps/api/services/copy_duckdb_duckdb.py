"""DuckDB → DuckDB ATTACH + INSERT SELECT (identity bulk).

Dest COUNT is ``SELECT COUNT(*)`` via ``destination_row_count``, never
``duckdb_tables()`` metadata, never a writer ack. Empty dest is
``INSERT INTO dest SELECT … FROM srcdb.main.src`` after
``ATTACH … (READ_ONLY)``. Python never formats a row. Occupied dest whose
COUNT already equals the source COUNT is skip-complete. Occupied dest with
a different COUNT declines. Same file + same table declines.
``:memory:`` declines. MotherDuck declines. This is **not**
``EXPORT DATABASE`` / ``IMPORT DATABASE``, not ``read_parquet`` staging,
and not a pandas / Arrow round trip.

The READ_ONLY attach is also the snapshot argument: DuckDB's file lock
lets many readers or one writer, so while this path holds the source open
for reading no other process can open it for writing. The source
population cannot move under the copy.

Structure travels with the values. Dest DDL is rebuilt from the **source
catalog** (exact type text, ``NOT NULL``, ``DEFAULT``, ``PRIMARY KEY``,
``UNIQUE``), so this path does not widen ``INTEGER`` to ``BIGINT`` or hand
back a table that enforces fewer rules than its source.

Declines (row path keeps quarantine): transforms that change values,
column renames, ``CHECK`` / ``FOREIGN KEY`` sources, a key over an
unmapped column, occupied dest with dest COUNT ≠ source, copy onto the
same table, ``:memory:``, MotherDuck.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_duckdb_common import (
    duckdb_attach_sql,
    duckdb_create_sql_from_source,
    duckdb_dest_count,
    duckdb_engine,
    duckdb_ident,
    duckdb_resolved_path,
    duckdb_same_file,
    duckdb_schema_name,
    duckdb_source_columns,
    duckdb_source_constraints,
    duckdb_table_exists,
    duckdb_table_ref,
    duckdb_type_is_copy_safe,
    skip_complete_duckdb,
)
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_SRC_ALIAS = "dataflow_src"


def duckdb_duckdb_copy_enabled() -> bool:
    raw = (getenv_brand("DUCKDB_DUCKDB_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def copy_duckdb_to_duckdb(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    duckdb_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """ATTACH source and INSERT SELECT into dest. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(duckdb_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not duckdb_duckdb_copy_enabled():
        raise FastPathUnavailable("DuckDB→DuckDB COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for src_col, tgt_col in pairs:
        if src_col != tgt_col:
            raise FastPathUnavailable("DuckDB identity COPY cannot rename columns")
    for declared in duckdb_ddls:
        if declared and not duckdb_type_is_copy_safe(declared):
            raise FastPathUnavailable(
                f"declared type {declared!r} is not DuckDB COPY-safe"
            )

    src_path = duckdb_resolved_path(source_cfg)
    duckdb_resolved_path(dest_cfg)
    src_schema = (source_schema or "").strip() or duckdb_schema_name(source_cfg)
    dest_schema = duckdb_schema_name(dest_cfg)
    same_file = duckdb_same_file(source_cfg, dest_cfg)
    if (
        same_file
        and src_schema.lower() == dest_schema.lower()
        and source_table.strip().lower() == dest_table.strip().lower()
    ):
        raise FastPathUnavailable(
            "DuckDB COPY onto the same table stays on the row path"
        )

    source_cols = [p[0] for p in pairs]
    dest_cols = [p[1] for p in pairs]
    src_col_sql = ", ".join(duckdb_ident(c) for c in source_cols)
    dest_col_sql = ", ".join(duckdb_ident(c) for c in dest_cols)
    dest_ref = duckdb_table_ref(catalog=None, schema=dest_schema, table=dest_table)

    dest_count_before = duckdb_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0

    engine = duckdb_engine(dest_cfg)
    created_here = False
    attached = False
    conn = engine.connect()
    try:
        if same_file:
            src_ref = duckdb_table_ref(
                catalog=None, schema=src_schema, table=source_table
            )
            src_catalog = _current_catalog(conn)
        else:
            # Both failure modes here are pre-mutation, so they decline to the
            # row path instead of failing the job: another process holding the
            # source refuses our reader lock, and another connection in this
            # process already owns the file handle under its own alias (DuckDB
            # shares one instance per path). Either way the source is not
            # provably frozen, which is the only thing this path claims.
            try:
                conn.exec_driver_sql(duckdb_attach_sql(src_path, _SRC_ALIAS))
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    logger.debug("DuckDB rollback after ATTACH skipped", exc_info=True)
                raise FastPathUnavailable(
                    "DuckDB source is held by another connection "
                    f"(READ_ONLY attach refused): {exc}"
                ) from exc
            attached = True
            src_ref = duckdb_table_ref(
                catalog=_SRC_ALIAS, schema=src_schema, table=source_table
            )
            src_catalog = _SRC_ALIAS

        if not duckdb_table_exists(
            conn, catalog=src_catalog, schema=src_schema, table=source_table
        ):
            raise FastPathUnavailable("DuckDB source table missing")
        live = duckdb_source_columns(
            conn, catalog=src_catalog, schema=src_schema, table=source_table
        )
        constraints = duckdb_source_constraints(
            conn, catalog=src_catalog, schema=src_schema, table=source_table
        )

        try:
            source_count = int(
                conn.exec_driver_sql(f"SELECT COUNT(*) FROM {src_ref}").scalar() or 0  # nosec B608
            )
            dest_catalog = _current_catalog(conn)
            exists = duckdb_table_exists(
                conn, catalog=dest_catalog, schema=dest_schema, table=dest_table
            )
            if dest_occupied and not replace_destination:
                if dest_count_before == source_count:
                    conn.rollback()
                    return skip_complete_duckdb(
                        source_count=source_count,
                        dest_count=dest_count_before,
                        extra_snapshot={
                            "duckdb_write": "skip",
                            "duckdb_read": "skip",
                        },
                    )
                raise FastPathUnavailable(
                    "append into occupied DuckDB dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
            if replace_destination and exists:
                conn.exec_driver_sql(f"DROP TABLE {dest_ref}")  # nosec B608
                exists = False
            if not exists:
                conn.exec_driver_sql(
                    duckdb_create_sql_from_source(
                        dest_ref=dest_ref,
                        pairs=pairs,
                        live=live,
                        constraints=constraints,
                    )
                )
                created_here = True
            conn.exec_driver_sql(
                f"INSERT INTO {dest_ref} ({dest_col_sql}) "  # nosec B608
                f"SELECT {src_col_sql} FROM {src_ref}"
            )
            in_tx_count = int(
                conn.exec_driver_sql(f"SELECT COUNT(*) FROM {dest_ref}").scalar() or 0  # nosec B608
            )
            if in_tx_count != source_count:
                raise ValueError(
                    "DuckDB→DuckDB COPY refused: dest COUNT(*) "
                    f"{in_tx_count} != source COUNT {source_count}"
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                logger.debug("DuckDB dest rollback skipped", exc_info=True)
            if created_here:
                try:
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                    conn.commit()
                except Exception:
                    logger.debug(
                        "DuckDB dest drop after copy failure skipped", exc_info=True
                    )
            raise
    finally:
        if attached:
            try:
                conn.exec_driver_sql(f"DETACH {duckdb_ident(_SRC_ALIAS)}")
                conn.commit()
            except Exception:
                logger.debug("DuckDB DETACH skipped", exc_info=True)
        try:
            conn.close()
        except Exception:
            logger.debug("DuckDB dest close skipped", exc_info=True)

    dest_count = duckdb_dest_count(dest_cfg, dest_table)
    if dest_count != source_count:
        raise ValueError(
            "DuckDB→DuckDB COPY refused: dest COUNT(*) "
            f"{dest_count} != source COUNT {source_count}"
        )

    duckdb_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
            "duckdb_read": "same_file_select" if same_file else "attach_select",
            "duckdb_write": duckdb_write,
            "duckdb_table": dest_table,
        },
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _current_catalog(conn: Any) -> str:
    row = conn.exec_driver_sql("SELECT current_database()").fetchone()
    return str(row[0]) if row else "memory"
