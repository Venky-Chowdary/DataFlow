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

    Only taken when nothing in the route can change a value: same engine, a
    declared type that matches on both sides for every mapped column, a plain
    carry with no transform or declared omission, a full refresh that replaces
    the destination, and no filter, limit, incremental scope or resume that
    would make this a partial read.

    Returning ``None`` rather than raising is deliberate — every route this
    cannot prove belongs on the row path, which knows how to reconcile the
    differences this one refuses to guess at.
    """
    from services.sync_cursor import is_overwrite_sync

    if (
        incremental
        or source_filter
        or limit
        or not is_overwrite_sync(effective_sync)
        or (checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    ):
        return None

    from services.engine_checksum import comparable_column_pairs, engines_comparable

    if not engines_comparable(src_type, dest_type):
        return None

    source_table = source.table or source.collection or ""
    from .stream import _source_name, resolve_dest_table

    dest_table = resolve_dest_table(dest_type, destination, _source_name(source))
    if not source_table or not dest_table:
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
