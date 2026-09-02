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

    PostgreSQL→PostgreSQL: binary COPY when types are identical and the sync
    replaces the destination.

    PostgreSQL→MySQL identity append/overwrite: text COPY + STRICT
    ``LOAD DATA LOCAL INFILE`` when every mapping is a no-op carry and every
    type is LOAD-DATA-safe. Proof is dest ``COUNT(*)`` vs the source snapshot.

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

    if not is_overwrite_sync(effective_sync):
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
            replace_destination=True,
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
        "sync_mode": effective_sync,
        # The digest is only comparable because it was read in the same snapshot
        # as the rows, so the snapshot claim travels with the result.
        "source_snapshot": dict(result.source_snapshot or {}),
        # Secondary indexes reproduced after the load — carried, not dropped, so
        # the destination enforces the same rules and reads at the same cost.
        "indexes_carried": list(result.indexes_carried or ()),
        "proof_scope": result.proof_scope,
    }
    ddl_log = [
        f"COPY {source_table} → {dest_table} "
        f"({result.source_rows:,} rows, binary, server-to-server)",
        "Gate-8: mapped-column population checksum inside the source snapshot.",
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
