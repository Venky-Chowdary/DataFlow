"""Choosing the server-to-server COPY path, and shaping what it reports.

Split out of ``src.transfer.stream`` (a module at its size budget). The decision
is deliberately conservative: it returns ``None`` for every route it cannot
prove identical, because a route this path cannot verify belongs on the row
path, which knows how to reconcile the differences it refuses to guess at.
"""

from __future__ import annotations

import logging
from typing import Any

from services.checkpoint_service import Checkpoint

from .models import EndpointConfig

logger = logging.getLogger(__name__)


def _try_copy_fast_path(
    *,
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    src_type: str,
    dest_type: str,
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    effective_sync: str,
    incremental: bool,
    source_filter: dict[str, Any] | None,
    limit: int,
    checkpoint: Checkpoint | None,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Move the whole table server-to-server, or return ``None`` to stream rows.

    PostgreSQL→PostgreSQL: binary COPY when types are identical. Append and
    overwrite both qualify; a non-empty destination on append stays on the
    row path. Proof is the mapped-column digest plus dest ``COUNT(*)``.

    PostgreSQL→MySQL identity append/overwrite: text COPY + STRICT
    ``LOAD DATA LOCAL INFILE`` when every mapping is a no-op carry and every
    type is LOAD-DATA-safe. Proof is dest ``COUNT(*)`` vs the source snapshot.

    MySQL→PostgreSQL identity append/overwrite: unbuffered SELECT + FIFO TSV
    into ``COPY FROM STDIN``. One InnoDB consistent snapshot. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    MySQL→MySQL identity append/overwrite: same-instance ``INSERT SELECT``
    under a consistent snapshot, or cross-host STRICT ``LOAD DATA LOCAL
    INFILE``. Proof is dest ``COUNT(*)`` vs that snapshot.

    PostgreSQL→SQL Server identity append/overwrite: text COPY decoded
    into pyodbc ``fast_executemany`` batches. Not BCP / ``BULK INSERT``
    CSV (quoted empty string collapses to NULL on this engine). Proof
    is dest ``COUNT(*)`` vs the source snapshot.

    SQL Server→PostgreSQL identity append/overwrite: HOLDLOCK SELECT
    encoded as COPY text into ``COPY FROM STDIN``. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    SQL Server→SQL Server identity append/overwrite: same-instance
    ``INSERT SELECT`` (SNAPSHOT when the database allows it, else
    ``HOLDLOCK, TABLOCK``). Proof is dest ``COUNT(*)`` vs that snapshot.
    Cross-host declines to the row path (no BCP yet).

    Oracle→Oracle identity append/overwrite: same-instance ``INSERT
    SELECT`` after ``LOCK TABLE src IN SHARE MODE``. Proof is dest
    ``COUNT(*)`` vs that snapshot. Cross-host declines to the row path
    (no Data Pump / DB link yet).

    PostgreSQL→Oracle identity append/overwrite: text COPY decoded
    into ``oracledb.executemany`` batches. Oracle VARCHAR2 stores
    ``''`` as NULL (engine law, counted in
    ``empty_string_as_null_cells``). Proof is dest ``COUNT(*)`` vs the
    source snapshot.

    Oracle→PostgreSQL identity append/overwrite: SHARE-lock SELECT
    encoded as COPY text into ``COPY FROM STDIN``. Proof is dest
    ``COUNT(*)`` vs that snapshot.

    MySQL→SQL Server identity append/overwrite: consistent-snapshot
    SELECT bound with pyodbc ``fast_executemany``. Not BCP / CSV
    ``BULK INSERT``. Proof is dest ``COUNT(*)`` vs that snapshot.

    SQL Server→MySQL identity append/overwrite: HOLDLOCK SELECT encoded
    as LOAD DATA TSV into a tempfile, then STRICT ``LOAD DATA LOCAL
    INFILE`` (no pyodbc FIFO). Proof is dest ``COUNT(*)`` vs that
    snapshot.

    MySQL→Oracle identity append/overwrite: consistent-snapshot SELECT
    bound with ``oracledb.executemany``. Oracle VARCHAR2 stores ``''``
    as NULL (engine law, counted in ``empty_string_as_null_cells``).
    Proof is dest ``COUNT(*)`` vs the source snapshot.

    Oracle→MySQL identity append/overwrite: SHARE-lock SELECT encoded
    as LOAD DATA TSV into a tempfile, then STRICT ``LOAD DATA LOCAL
    INFILE``. Proof is dest ``COUNT(*)`` vs that snapshot.

    SQL Server→Oracle identity append/overwrite: HOLDLOCK SELECT bound
    with ``oracledb.executemany``. Oracle VARCHAR2 stores ``''`` as
    NULL (engine law, counted in ``empty_string_as_null_cells``). Proof
    is dest ``COUNT(*)`` vs the source snapshot.

    Oracle→SQL Server identity append/overwrite: SHARE-lock SELECT
    bound with pyodbc ``fast_executemany``. Not BCP / CSV
    ``BULK INSERT``. Proof is dest ``COUNT(*)`` vs that snapshot.

    PostgreSQL→Iceberg identity append/overwrite: COPY CSV into one
    Arrow table and one catalog snapshot commit (CoW append, or
    snapshot replace on overwrite). Dest COUNT is file footers, never
    ``scan().count()``. Occupied dest with a different COUNT declines
    (leftover MERGE stays on the row path). Filesystem CoW declines.

    Iceberg→PostgreSQL identity append/overwrite: current-snapshot
    Parquet files encoded as COPY text into ``COPY FROM STDIN``. Source
    COUNT is file footers, never ``scan().count()``. Occupied dest with
    a different COUNT declines. MoR snapshots decline.

    MySQL→Iceberg identity append/overwrite: consistent-snapshot SELECT
    encoded as CSV into one Arrow table and one catalog snapshot. Dest
    COUNT is file footers, never ``scan().count()``. Occupied dest with
    a different COUNT declines. Empty dest is CoW snapshot append, not
    ``MERGE INTO``.

    Returning ``None`` rather than raising is deliberate — every route this
    cannot prove belongs on the row path, which knows how to reconcile the
    differences this one refuses to guess at.
    """
    from services.procedure_source import is_callable_source
    from services.sync_cursor import is_append_sync, is_overwrite_sync

    # Studio may set source.table to the procedure stream name (e.g. get_orders).
    # COPY of a colliding real table would move the wrong population — refuse.
    if is_callable_source(source) or is_callable_source(src_cfg):
        logger.info("COPY fast path declined: callable source is a result set, not a table")
        return None

    if (
        incremental
        or source_filter
        or limit
        or (checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    ):
        return None

    source_table = source.table or source.collection or ""
    from .stream import _source_name, resolve_dest_table

    dest_table = resolve_dest_table(dest_type, destination, _source_name(source))
    if not source_table or not dest_table:
        return None

    src_n = (src_type or "").strip().lower()
    dest_n = (dest_type or "").strip().lower()
    if src_n in {"postgresql", "postgres"} and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_fast = _try_pg_mysql_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_fast is not None:
            return mysql_fast
        return None

    from services.copy_sqlserver_sqlserver import sqlserver_family_name

    if (
        src_n in {"postgresql", "postgres"}
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ss = _try_pg_sqlserver_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ss is not None:
            return pg_ss
        return None

    from services.copy_oracle_oracle import oracle_family_name

    if (
        src_n in {"postgresql", "postgres"}
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ora = _try_pg_oracle_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ora is not None:
            return pg_ora
        return None

    if src_n in {"postgresql", "postgres"} and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_ice = _try_pg_iceberg_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_ice is not None:
            return pg_ice
        return None

    if src_n in {"iceberg", "apache_iceberg"} and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ice_pg = _try_iceberg_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "default",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ice_pg is not None:
            return ice_pg
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"postgresql", "postgres"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        pg_fast = _try_mysql_pg_copy_fast_path(
            source=source,
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if pg_fast is not None:
            return pg_fast
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"mysql", "mariadb"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_mysql = _try_mysql_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_mysql is not None:
            return mysql_mysql
        return None

    if (
        src_n in {"mysql", "mariadb"}
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ss = _try_mysql_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ss is not None:
            return mysql_ss
        return None

    if (
        src_n in {"mysql", "mariadb"}
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ora = _try_mysql_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ora is not None:
            return mysql_ora
        return None

    if src_n in {"mysql", "mariadb"} and dest_n in {"iceberg", "apache_iceberg"}:
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        mysql_ice = _try_mysql_iceberg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            dest_schema=destination.schema or dest_cfg.get("schema") or "default",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if mysql_ice is not None:
            return mysql_ice
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_fast = _try_sqlserver_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_fast is not None:
            return ss_fast
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and dest_n in {"postgresql", "postgres"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_pg = _try_sqlserver_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_pg is not None:
            return ss_pg
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and dest_n in {"mysql", "mariadb"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_mysql = _try_sqlserver_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_mysql is not None:
            return ss_mysql
        return None

    if (
        sqlserver_family_name(src_n) == "sqlserver"
        and oracle_family_name(dest_n) == "oracle"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ss_ora = _try_sqlserver_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or "dbo",
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "DATAFLOW",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ss_ora is not None:
            return ss_ora
        return None

    if oracle_family_name(src_n) == "oracle" and oracle_family_name(dest_n) == "oracle":
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_fast = _try_oracle_oracle_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or dest_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_fast is not None:
            return ora_fast
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and dest_n in {"postgresql", "postgres"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_pg = _try_oracle_pg_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or "public",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_pg is not None:
            return ora_pg
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and dest_n in {"mysql", "mariadb"}
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_mysql = _try_oracle_mysql_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_mysql is not None:
            return ora_mysql
        return None

    if (
        oracle_family_name(src_n) == "oracle"
        and sqlserver_family_name(dest_n) == "sqlserver"
    ):
        if not (is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)):
            return None
        ora_ss = _try_oracle_sqlserver_copy_fast_path(
            source_table=source_table,
            dest_table=dest_table,
            mappings=mappings,
            schema=schema,
            src_cfg=src_cfg,
            dest_cfg=dest_cfg,
            dest_type=dest_n,
            source_schema=source.schema or src_cfg.get("schema") or src_cfg.get("username") or "",
            dest_schema=destination.schema or dest_cfg.get("schema") or "dbo",
            replace_destination=is_overwrite_sync(effective_sync),
        )
        if ora_ss is not None:
            return ora_ss
        return None

    if not (
        is_overwrite_sync(effective_sync) or is_append_sync(effective_sync)
    ):
        return None

    from services.engine_checksum import comparable_column_pairs, engines_comparable

    if not engines_comparable(src_type, dest_type):
        return None

    from services.copy_fast_path import (
        FastPathUnavailable,
        copy_between_postgres,
        source_column_types,
    )

    # Both sides are described by the source catalog: the destination is created
    # from the source's own declarations, so "identical" is true by construction
    # rather than by comparing two independently resolved spellings.
    try:
        conn = _pg_connect_for_probe(src_cfg)
    except Exception as exc:
        logger.info("COPY fast path declined (source probe): %s", exc)
        return None
    try:
        with conn.cursor() as cur:
            declared = source_column_types(
                cur,
                source.schema or "public",
                source_table,
                [str(m.get("source") or "") for m in mappings if m.get("source")],
            )
    except Exception as exc:
        logger.info("COPY fast path declined (source catalog): %s", exc)
        return None
    finally:
        try:
            conn.close()
        except Exception:  # nosec B110 — probe connection only
            pass

    pairs = comparable_column_pairs(mappings, declared, declared, engine=src_type)
    if not pairs:
        return None
    # ``comparable_column_pairs`` compared the source against itself above, which
    # proves the mapping is a plain carry but not that the *destination* agrees.
    # The destination is created from these same declarations, so it does.
    try:
        result = copy_between_postgres(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=destination.schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            replace_destination=is_overwrite_sync(effective_sync),
        )
    except FastPathUnavailable as exc:
        logger.info("COPY fast path declined: %s", exc)
        return None
    except Exception as exc:
        # A refusal here means the copy ran and did not verify, which is a real
        # finding — never silently retry it on the row path and report success.
        logger.warning("COPY fast path failed: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_binary_server_to_server",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "engine_source_checksum": result.source_checksum,
        "engine_target_checksum": result.target_checksum,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": effective_sync,
        # The digest is only comparable because it was read in the same snapshot
        # as the rows, so the snapshot claim travels with the result.
        "source_snapshot": dict(result.source_snapshot or {}),
        # Secondary indexes reproduced after the load — carried, not dropped, so
        # the destination enforces the same rules and reads at the same cost.
        "indexes_carried": list(result.indexes_carried or ()),
        "copy_split": (result.source_snapshot or {}).get("copy_split") or "binary",
        "shard_mode": (result.source_snapshot or {}).get("shard_mode") or "serial",
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
        "proof_scope": result.proof_scope,
    }
    proof_line = (
        "Proof: mapped-column checksum inside the source snapshot; "
        "destination COUNT(*) equals that snapshot."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY {source_table} → {dest_table} "
        f"({result.source_rows:,} rows, binary, server-to-server)",
        proof_line,
    ]
    if result.indexes_carried:
        ddl_log.append(
            f"Carried {len(result.indexes_carried)} secondary index(es) "
            f"after load: {', '.join(result.indexes_carried)}"
        )
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_mysql_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→MySQL: COPY text + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import (
        copy_postgres_to_mysql,
        mapping_is_plain_carry,
        pg_type_is_load_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→MySQL COPY declined: %s type %s is not LOAD DATA safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared))

    try:
        result = copy_postgres_to_mysql(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_mysql_load_data",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    workers = int((result.source_snapshot or {}).get("copy_workers") or 1)
    shard_mode = (result.source_snapshot or {}).get("shard_mode") or "ctid"
    copy_split = (result.source_snapshot or {}).get("copy_split") or shard_mode
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if shard_mode == "pk" and dest_summary.get("partition_proof"):
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, text COPY + STRICT LOAD DATA, "
        f"{workers} worker(s), copy_split={copy_split}, proof={shard_mode})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_sqlserver_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→SQL Server: COPY text + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.copy_pg_sqlserver import copy_postgres_to_sqlserver
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→SQL Server COPY declined: %s type %s is not COPY-text safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_postgres_to_sqlserver(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_sqlserver_fast_executemany",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY PostgreSQL {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, COPY text + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_oracle_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→Oracle: COPY text + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.copy_pg_oracle import copy_postgres_to_oracle
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→Oracle COPY declined: %s type %s is not COPY-text safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_postgres_to_oracle(
            source_cfg=src_cfg,
            source_schema=source.schema or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_pg_to_oracle_executemany",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = (
        "Proof: destination COUNT(*) equals source snapshot count. "
        "Oracle VARCHAR2 stores empty string as NULL (engine law)."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range. "
            "Oracle VARCHAR2 stores empty string as NULL (engine law)."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY PostgreSQL {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, COPY text + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_pg_iceberg_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity PG→Iceberg: COPY CSV + one snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_iceberg import copy_postgres_to_iceberg
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("PG→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not pg_type_is_load_safe(declared):
            logger.info(
                "PG→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_postgres_to_iceberg(
            source_cfg=src_cfg,
            source_schema=source.schema or src_cfg.get("schema") or "public",
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("PG→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("PG→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_csv_pg_to_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, COPY CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_iceberg_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Iceberg→PG: snapshot Parquet + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_iceberg_pg import copy_iceberg_to_postgres, iceberg_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Iceberg→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if declared and not (
            pg_type_is_load_safe(declared) or iceberg_type_is_copy_safe(declared)
        ):
            logger.info(
                "Iceberg→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_iceberg_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Iceberg→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Iceberg→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "iceberg_parquet_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "iceberg_file_footers",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_read": snapshot.get("iceberg_read"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    proof_line = (
        "Proof: destination COUNT(*) equals Iceberg source footer COUNT. "
        "Not scan().count()."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY Iceberg {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, snapshot Parquet + COPY FROM STDIN, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_pg import (
        copy_sqlserver_to_postgres,
        sqlserver_type_is_copy_safe,
    )

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_sqlserver_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "sqlserver_isolation": (result.source_snapshot or {}).get("sqlserver_isolation"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    isolation = dest_summary.get("sqlserver_isolation") or "holdlock"
    ddl_log = [
        f"COPY SQL Server {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN, {isolation})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_pg_copy_fast_path(
    *,
    source: EndpointConfig,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import copy_mysql_to_postgres, mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared))

    try:
        result = copy_mysql_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "copy_text_mysql_to_pg_stdin",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "tsv_encoder": (result.source_snapshot or {}).get("tsv_encoder"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
    ddl_log = [
        f"COPY MySQL {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→MySQL: INSERT SELECT or STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_mysql import copy_mysql_to_mysql
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→MySQL copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_mysql_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→MySQL copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→MySQL copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    split = str((result.source_snapshot or {}).get("copy_split") or "")
    load_method = (
        "insert_select_mysql_same_instance"
        if split == "insert_select"
        else "copy_text_mysql_to_mysql_load_data"
    )
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": load_method,
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": "full_refresh_append" if not replace_destination else "full_refresh_overwrite",
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
    how = (
        "INSERT SELECT (same instance)"
        if split == "insert_select"
        else "SELECT + STRICT LOAD DATA"
    )
    ddl_log = [
        f"COPY MySQL {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, {how})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→SQL Server: SELECT + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_mysql_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY MySQL {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, SELECT + fast_executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→MySQL: SELECT + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_mysql import copy_sqlserver_to_mysql
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_sqlserver_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlserver_isolation": snapshot.get("sqlserver_isolation"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY SQL Server {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + STRICT LOAD DATA, tempfile)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→Oracle: SELECT + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_oracle import copy_mysql_to_oracle
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_mysql_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY MySQL {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, SELECT + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_mysql_iceberg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity MySQL→Iceberg: SELECT + CSV + snapshot. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_mysql_iceberg import copy_mysql_to_iceberg
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("MySQL→Iceberg COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    iceberg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not mysql_type_is_copy_safe(declared):
            logger.info(
                "MySQL→Iceberg COPY declined: %s type %s is not Iceberg COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        iceberg_ddls.append(ddl_type("iceberg", declared) if declared else "string")

    try:
        result = copy_mysql_to_iceberg(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            iceberg_ddls=iceberg_ddls,
            replace_destination=replace_destination,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("MySQL→Iceberg COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("MySQL→Iceberg COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_mysql_csv_iceberg_snapshot",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "iceberg_write": snapshot.get("iceberg_write"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    split = dest_summary.get("copy_split") or "serial"
    write = dest_summary.get("iceberg_write") or "append"
    proof_line = (
        "Proof: Iceberg dest COUNT (file footers) equals source snapshot count. "
        "Not scan().count(). Empty dest is CoW snapshot append, not MERGE INTO."
    )
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if split == "skip" and skipped:
        proof_line += " Resume skipped complete dest (COUNT only)."
    ddl_log = [
        f"COPY MySQL {source_table} → Iceberg {dest_table} "
        f"({result.source_rows:,} rows, SELECT + CSV + {write} snapshot, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_mysql_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→MySQL: SELECT + STRICT LOAD DATA. Dest COUNT is the proof."""
    from connectors.mysql_writer import mysql_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_mysql import copy_oracle_to_mysql
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→MySQL COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    mysql_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→MySQL COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        mysql_ddls.append(mysql_type(declared) if declared else "TEXT")

    try:
        result = copy_oracle_to_mysql(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→MySQL COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→MySQL COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_load_data_mysql",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → MySQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + STRICT LOAD DATA, tempfile, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→Oracle: SELECT + executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_oracle import copy_sqlserver_to_oracle
    from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→Oracle COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not sqlserver_type_is_copy_safe(declared):
            logger.info(
                "SQL Server→Oracle COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_sqlserver_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→Oracle COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→Oracle COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_sqlserver_executemany_oracle",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "empty_string_as_null_cells": snapshot.get("empty_string_as_null_cells") or 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "sqlserver_isolation": snapshot.get("sqlserver_isolation"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    empty_cells = int(dest_summary.get("empty_string_as_null_cells") or 0)
    if empty_cells:
        proof_line += (
            f" Oracle VARCHAR2 stored {empty_cells} empty string(s) as NULL "
            "(engine law, not a row drop)."
        )
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY SQL Server {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, SELECT + executemany, "
        f"copy_split={split})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→SQL Server: SELECT + fast_executemany. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import oracle_type_is_copy_safe
    from services.copy_oracle_sqlserver import copy_oracle_to_sqlserver
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→SQL Server COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→SQL Server COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_oracle_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→SQL Server COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→SQL Server COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_fast_executemany_sqlserver",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    split = dest_summary.get("copy_split") or "serial"
    ddl_log = [
        f"COPY Oracle {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, SELECT + fast_executemany, "
        f"copy_split={split}, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_sqlserver_sqlserver_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity SQL Server→SQL Server: INSERT SELECT. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.copy_sqlserver_sqlserver import copy_sqlserver_to_sqlserver
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("SQL Server→SQL Server copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    sqlserver_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        sqlserver_ddls.append(
            ddl_type("sqlserver", declared) if declared else "NVARCHAR(MAX)"
        )

    try:
        result = copy_sqlserver_to_sqlserver(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            sqlserver_ddls=sqlserver_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("SQL Server→SQL Server copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("SQL Server→SQL Server copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "insert_select_sqlserver_same_instance",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "sqlserver_isolation": (result.source_snapshot or {}).get("sqlserver_isolation"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    isolation = dest_summary.get("sqlserver_isolation") or "holdlock"
    ddl_log = [
        f"COPY SQL Server {source_table} → SQL Server {dest_table} "
        f"({result.source_rows:,} rows, INSERT SELECT, {isolation})",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_oracle_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→Oracle: INSERT SELECT. Dest COUNT is the proof."""
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_oracle import copy_oracle_to_oracle
    from services.copy_pg_mysql import mapping_is_plain_carry
    from services.type_system import ddl_type

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→Oracle copy declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    oracle_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        pairs.append((source_col, target_col))
        oracle_ddls.append(
            ddl_type("oracle", declared) if declared else "VARCHAR2(4000)"
        )

    try:
        result = copy_oracle_to_oracle(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            pairs=pairs,
            oracle_ddls=oracle_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
            dest_schema=dest_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→Oracle copy declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→Oracle copy failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "insert_select_oracle_same_instance",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": dict(result.source_snapshot or {}),
        "copy_workers": int((result.source_snapshot or {}).get("copy_workers") or 1),
        "copy_partitions": (result.source_snapshot or {}).get("copy_partitions"),
        "partitions_skipped": (result.source_snapshot or {}).get("partitions_skipped"),
        "shard_mode": (result.source_snapshot or {}).get("shard_mode"),
        "copy_split": (result.source_snapshot or {}).get("copy_split"),
        "oracle_lock": (result.source_snapshot or {}).get("oracle_lock"),
        "partition_proof": list(
            (result.source_snapshot or {}).get("partition_proof") or []
        ),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        skipped = int(dest_summary.get("partitions_skipped") or 0)
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → Oracle {dest_table} "
        f"({result.source_rows:,} rows, INSERT SELECT, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _try_oracle_pg_copy_fast_path(
    *,
    source_table: str,
    dest_table: str,
    mappings: list[dict],
    schema: dict[str, str],
    src_cfg: dict[str, Any],
    dest_cfg: dict[str, Any],
    dest_type: str,
    source_schema: str,
    dest_schema: str,
    replace_destination: bool,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity Oracle→PG: SELECT + COPY FROM STDIN. Dest COUNT is the proof."""
    from connectors.postgresql_writer import pg_type
    from services.copy_fast_path import FastPathUnavailable
    from services.copy_oracle_pg import copy_oracle_to_postgres, oracle_type_is_copy_safe
    from services.copy_pg_mysql import mapping_is_plain_carry

    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("Oracle→PG COPY declined: %s", reason)
        return None

    pairs: list[tuple[str, str]] = []
    pg_ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if not oracle_type_is_copy_safe(declared):
            logger.info(
                "Oracle→PG COPY declined: %s type %s is not COPY-safe",
                source_col,
                declared,
            )
            return None
        pairs.append((source_col, target_col))
        pg_ddls.append(pg_type(declared) if declared else "TEXT")

    try:
        result = copy_oracle_to_postgres(
            source_cfg=src_cfg,
            source_table=source_table,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema or "public",
            dest_table=dest_table,
            pairs=pairs,
            pg_ddls=pg_ddls,
            replace_destination=replace_destination,
            source_schema=source_schema,
        )
    except FastPathUnavailable as exc:
        logger.info("Oracle→PG COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("Oracle→PG COPY failed after starting: %s", exc)
        raise

    columns = [p[1] for p in pairs]
    snapshot = dict(result.source_snapshot or {})
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": "select_oracle_copy_from_stdin_pg",
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": (
            "full_refresh_append" if not replace_destination else "full_refresh_overwrite"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "oracle_lock": snapshot.get("oracle_lock"),
        "partition_proof": list(snapshot.get("partition_proof") or []),
    }
    proof_line = "Proof: destination COUNT(*) equals source snapshot count."
    skipped = int(dest_summary.get("partitions_skipped") or 0)
    if dest_summary.get("shard_mode") == "pk" and dest_summary.get("partition_proof"):
        proof_line = (
            "Proof: destination COUNT(*) equals source snapshot count; "
            "each PK range dest COUNT matched its source range."
        )
        if skipped == len(dest_summary["partition_proof"]):
            proof_line += f" Resume skipped {skipped} complete range(s) (COUNT only)."
        elif skipped:
            proof_line += f" Resume skipped {skipped} complete range(s)."
    ddl_log = [
        f"COPY Oracle {source_table} → PostgreSQL {dest_table} "
        f"({result.source_rows:,} rows, SELECT + COPY FROM STDIN, SHARE lock)",
        proof_line,
    ]
    return result.rows_copied, ddl_log, dest_summary, columns


def _pg_connect_for_probe(cfg: dict[str, Any]) -> Any:
    from connectors.postgresql_conn import get_connection

    return get_connection(
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 5432),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        ssl=bool(cfg.get("ssl", False)),
    )
