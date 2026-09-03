"""Iceberg snapshot Parquet → SQLite executemany (cross-engine bulk).

The reverse of ``copy_sqlite_iceberg``. Source COUNT is Iceberg file
footers via ``destination_row_count`` / ``iceberg_mor`` — never
``scan().count()``. Payload is current-snapshot data files read as
Arrow (no ``scan().to_arrow()``). Python values bind with
``executemany`` INSERT. Dest ``COUNT(*)`` must equal that footer COUNT
**before commit**. Empty dest is INSERT, **not** upsert / sqlite3
``.import`` / ``MERGE INTO``. Occupied dest whose ``COUNT(*)`` already
equals the source footer COUNT is skip-complete. Occupied dest with a
different COUNT declines. Iceberg MoR (delete files) declines.
Filesystem CoW declines. ``:memory:`` / BLOB dest DDL decline. DATE /
DATETIME-NTZ land as SQLite TEXT (ISO — SQLite has no DATE affinity).

Reuses ``_arrow_from_iceberg_files`` from ``copy_iceberg_pg``.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
occupied dest with dest COUNT ≠ source, ``:memory:``.
"""

from __future__ import annotations

import logging
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
    iceberg_type_is_copy_safe,
)
from services.copy_pg_iceberg import iceberg_copy_endpoint
from services.copy_pg_mysql import mapping_is_plain_carry
from services.copy_sqlite_common import (
    skip_complete_sqlite,
    sqlite_connect,
    sqlite_create_sql,
    sqlite_ident,
    sqlite_pragma_types,
    sqlite_resolved_path,
    sqlite_table_exists,
    sqlite_type_is_copy_safe,
)

logger = logging.getLogger(__name__)


def iceberg_sqlite_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_SQLITE_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def iceberg_sqlite_copy_batch() -> int:
    raw = (getenv_brand("ICEBERG_SQLITE_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def iceberg_value_to_sqlite(value: Any) -> Any:
    """Bind an Iceberg/Arrow Python value. DATE/DATETIME-NTZ land as ISO TEXT."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise FastPathUnavailable(
                "timestamptz Iceberg value is not SQLite COPY-safe"
            )
        if value.hour or value.minute or value.second or value.microsecond:
            return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        return date(value.year, value.month, value.day).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Iceberg field is not SQLite COPY-safe")
    if isinstance(value, UUID):
        raise FastPathUnavailable("uuid Iceberg field is not SQLite COPY-safe")
    if isinstance(value, (dict, list, tuple)):
        raise FastPathUnavailable("nested Iceberg value is not SQLite COPY-safe")
    return value


def copy_iceberg_to_sqlite(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """INSERT Iceberg snapshot rows into SQLite. Dest COUNT(*) before commit is the proof."""
    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_sqlite_copy_enabled():
        raise FastPathUnavailable("Iceberg→SQLite COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or "") or is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    sqlite_resolved_path(dest_cfg)
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
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    batch_size = iceberg_sqlite_copy_batch()

    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    try:
        source_count = _iceberg_source_count(endpoint)

        dest_conn.execute("BEGIN IMMEDIATE")
        exists = sqlite_table_exists(dest_conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                dest_conn.rollback()
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"iceberg_read": "skip", "sqlite_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if exists:
            live_dest = sqlite_pragma_types(dest_conn, dest_table)
            live_dest_l = {k.lower(): v for k, v in live_dest.items()}
            for col in target_cols:
                declared = live_dest_l.get(col.lower())
                if declared is None:
                    raise FastPathUnavailable(f"dest column {col!r} absent")
                if not sqlite_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"dest column {col!r} type {declared} is not SQLite COPY-safe"
                    )
        else:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        pa_table = _arrow_from_iceberg_files(endpoint, source_cols)
        if len(pa_table) != source_count:
            dest_conn.rollback()
            raise ValueError(
                "Iceberg→SQLite COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)

        pending: list[tuple[Any, ...]] = []
        inserted = 0
        for batch in pa_table.to_batches(max_chunksize=_ARROW_BATCH):
            cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            if not cols:
                continue
            for row in zip(*cols):
                pending.append(tuple(iceberg_value_to_sqlite(v) for v in row))
                if len(pending) >= batch_size:
                    dest_conn.executemany(insert_sql, pending)
                    inserted += len(pending)
                    pending.clear()
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)

        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "Iceberg→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source footer COUNT {source_count}"
            )
        dest_conn.commit()
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
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
                "sqlite_write": sqlite_write,
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("SQLite dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)
