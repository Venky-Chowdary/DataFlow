"""Paging reads of the streaming DB loop."""

from __future__ import annotations

import time
from typing import Any

from services.phase_profile import PHASE_READ

from .stream_row_accounting import _raw_page_keyset, _raw_page_rows
from .type_mapper import normalize_inferred
from . import stream as _stream


def _cursor_read_args(
    run,
    cursor_after: str | None, *, run_seed: bool = False
) -> dict[str, Any]:
    """The cursor arguments every read of this run must agree on.

    A watermark is composite when a tie-break primary key is in play, and
    only a read that also names that column can decode it. Reads that left
    ``cursor_primary_key`` out bound the whole composite against the cursor
    column instead, so every run after the first died on the sizing probe.

    ``run_seed`` marks a read bounded by the stored watermark itself rather
    than by a page edge this run advanced. D124/D60/D98: the stored
    ``(cursor, last key)`` only orders *this* run's pages — between runs a
    row can be inserted or restamped at the watermark cursor with a lower
    key, so the seed re-reads the whole tie group (``cursor >= watermark``).
    """
    cfg = run.cfg
    if not cfg.incremental or not cfg.cursor_source_col:
        return {}
    return {
        "cursor_column": cfg.cursor_source_col,
        "cursor_after": cursor_after,
        "cursor_inclusive": bool(run_seed and cursor_after not in (None, "")),
        "cursor_type": (
            normalize_inferred(cfg.schema.get(cfg.cursor_source_col, "string")).upper()
            if cfg.schema
            else None
        ),
        "cursor_primary_key": cfg.cursor_pk_source or None,
    }


def _batch_limit(run, offset: int = 0, *, default: int | None = None) -> int:
    cfg = run.cfg
    if default is None:
        default = cfg.chunk_size
    if cfg.limit > 0:
        return max(0, min(default, cfg.limit - offset))
    return default


def _fetch_source_batch(run, last_batch):
    cfg = run.cfg
    cur = run.cur
    from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES as _SNAPSHOT_SCAN_SOURCES
    from services.procedure_source import is_callable_source
    from services.sync_cursor import max_cursor_value
    from services.keyset_pagination import max_keyset_bookmark
    from services.sync_cursor import source_bounds_cursor_reads
    if cfg.bulk_export_iter is not None:
        try:
            return next(cfg.bulk_export_iter)
        except StopIteration:
            return None
    if cfg.limit > 0 and cur.fetch_offset >= cfg.limit:
        return None
    if (
        cfg.total_rows is not None
        and cur.fetch_offset >= cfg.total_rows
        and cfg.src_type != "dynamodb"
        and not cfg.sdk_source
    ):
        return None
    if cfg.sdk_source:
        last_meta = (
            last_batch.meta
            if last_batch is not None and isinstance(last_batch.meta, dict)
            else {}
        )
        if last_batch is not None and last_meta.get("sdk_done"):
            return None
        sdk_state_for_read = (
            last_meta.get("sdk_state") if last_batch is not None else cfg.initial_sdk_state
        )
        batch, _ = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                _batch_limit(run, cur.fetch_offset),
                database=cfg.src_db,
                sdk_state=sdk_state_for_read or None,
            )
        )
        return batch
    # A zero-row page is drained. Short *non-empty* pages may be driver
    # vectors (DuckDB 2048) and must continue — that is ``page_may_be_partial``
    # below. An empty fetchmany after ``close_table_scan`` used to fall
    # through and reopen the snapshot from row 0 because ``while batch:``
    # is true for any ReadBatch object, so SQL Server (no keyset bookmark)
    # checkpoint-looped forever on a 2-row table.
    if last_batch is not None and _raw_page_rows(last_batch) == 0:
        return None
    batch_limit = _batch_limit(run, cur.fetch_offset)
    # A short page only proves the source is drained when the reader owned
    # the page size. A held snapshot scan hands back whatever ``fetchmany``
    # buffered (DuckDB returns one vector), and a catalog count that still
    # exceeds the rows read says more remain — stopping there drops rows
    # while every count and checksum agrees on the truncated set.
    page_may_be_partial = bool(
        last_batch is not None and _raw_page_rows(last_batch)
    ) and (
        bool(cfg.src_scan.get("started"))
        or (cfg.total_rows is not None and cur.fetch_offset < cfg.total_rows)
    )
    if (
        last_batch is not None
        and _raw_page_rows(last_batch) < cfg.chunk_size
        and not page_may_be_partial
    ):
        if (
            cfg.src_read_type
            in (
                "postgresql",
                "redshift",
                "mysql",
                "snowflake",
                "mongodb",
                "bigquery",
                "sqlserver",
                "oracle",
                "sqlite",
                "generic_sql",
            )
            and (cfg.incremental or cfg.use_keyset)
        ) or cfg.src_read_type in ("elasticsearch", "redis", "qdrant"):
            return None
    if cfg.src_type == "dynamodb":
        if not cur.ddb_cursor:
            return None
        batch, cur.ddb_cursor = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                dynamodb_cursor=cur.ddb_cursor,
                dynamodb_total=cur.ddb_total,
            )
        )
        return batch
    if cfg.src_type == "kafka":
        if last_batch is not None and (
            cur.kafka_cursor is None or cur.kafka_cursor.get("exhausted")
        ):
            return None
        batch, cur.kafka_cursor = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                kafka_cursor=cur.kafka_cursor,
                known_total_rows=cfg.total_rows,
            )
        )
        return batch
    if cfg.src_type == "qdrant":
        if cur.qdrant_offset is None and last_batch is not None:
            return None
        batch, cur.qdrant_offset = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                qdrant_offset=cur.qdrant_offset,
                known_total_rows=cfg.total_rows,
            )
        )
        return batch
    elif cfg.incremental and cfg.cursor_source_col and (
        is_callable_source(cfg.src_cfg)
    ):
        # Spool is not cursor-sorted. Filter against the *run* watermark
        # then OFFSET — never the advancing page max (that drops rows).
        batch, _ = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                **_cursor_read_args(run, cfg.watermark, run_seed=True),
            )
        )
        return batch
    elif (
        cfg.incremental
        and cfg.cursor_source_col
        and not cfg._filtered_scan_reason
        and source_bounds_cursor_reads(cfg.src_read_type)
    ):
        batch, _ = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                **_cursor_read_args(run, cur.fetch_cursor, run_seed=cur.fetch_from_seed),
            )
        )
        return batch
    elif cfg.use_keyset:
        if last_batch is not None and _raw_page_rows(last_batch):
            # Advance on the max of the page across the full composite key
            # (Phase F2) — max_keyset_bookmark does not assume page sort. A
            # shaped page carries the bookmark of the rows it was read as,
            # taken before the recipe could drop the highest one.
            advanced = _raw_page_keyset(last_batch)
            if not advanced:
                order_cols = cfg.keyset_order_cols or (
                    [cfg.keyset_col] + ([cfg.keyset_tiebreak] if cfg.keyset_tiebreak else [])
                )
                advanced = max_keyset_bookmark(
                    last_batch.rows, last_batch.headers, order_cols
                )
                if advanced is None and cfg.keyset_col in (last_batch.headers or []):
                    advanced = max_cursor_value(
                        last_batch.rows,
                        last_batch.headers,
                        cfg.keyset_col,
                        cfg.keyset_tiebreak or None,
                    )
            cur.keyset_after = advanced or cur.keyset_after
            if advanced:
                cur.keyset_run_seed = False
        _key_cols = cfg.keyset_order_cols or (
            [cfg.keyset_col] + ([cfg.keyset_tiebreak] if cfg.keyset_tiebreak else [])
        )
        batch, extra = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                cursor_column=cfg.keyset_col or (_key_cols[0] if _key_cols else ""),
                cursor_after=cur.keyset_after,
                cursor_inclusive=cur.keyset_run_seed,
                cursor_type=cfg.column_types.get(
                    cfg.keyset_col or (_key_cols[0] if _key_cols else ""), "VARCHAR"
                ),
                known_total_rows=cfg.total_rows,
                cursor_primary_key=(
                    cfg.keyset_tiebreak or None if len(_key_cols) <= 2 else None
                ),
                cursor_key_columns=_key_cols if len(_key_cols) >= 2 else None,
            )
        )
        if cfg.src_type == "elasticsearch":
            cur.es_search_after = extra
        elif cfg.src_type == "redis":
            cur.redis_scan_state = extra
        return batch
    elif cfg.src_type == "elasticsearch":
        batch, cur.es_search_after = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                es_search_after=cur.es_search_after,
            )
        )
        return batch
    elif cfg.src_type == "qdrant":
        if cur.qdrant_offset is None and last_batch is not None:
            return None
        batch, cur.qdrant_offset = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                qdrant_offset=cur.qdrant_offset,
            )
        )
        return batch
    elif cfg.src_type == "redis":
        batch, cur.redis_scan_state = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                redis_scan_state=cur.redis_scan_state,
            )
        )
        return batch
    elif cfg.total_rows is not None and cur.fetch_offset >= cfg.total_rows and not cfg.sdk_source:
        return None
    elif cfg._filtered_scan_reason:
        # One held scan bound to the *run* watermark; the page max never
        # becomes the predicate, so rows tied at a page edge are kept.
        batch, _ = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                **cfg._scan_kw,
            )
        )
        return batch
    else:
        batch, extra = _stream._unwrap_read(
            _stream._read_batch(
                cfg.src_read_type,
                cfg.src_cfg,
                cfg.table,
                cfg.columns,
                cur.fetch_offset,
                batch_limit,
                database=cfg.src_db,
                known_total_rows=cfg.total_rows,
                **(
                    {"scan_state": cfg.src_scan}
                    if cfg.src_read_type in _SNAPSHOT_SCAN_SOURCES and cfg.src_scan
                    else {}
                ),
            )
        )
        if cfg.src_type == "elasticsearch":
            cur.es_search_after = extra
        elif cfg.src_type == "redis":
            cur.redis_scan_state = extra
        return batch


def _fetch_next_batch(run, last_batch):
    """Timed wrapper — source read time is the first thing to rule out."""
    cfg = run.cfg
    started = time.perf_counter()
    try:
        result = _fetch_source_batch(run, last_batch)
    except BaseException:
        cfg.phase_profile.add(PHASE_READ, time.perf_counter() - started)
        raise
    cfg.phase_profile.add(
        PHASE_READ,
        time.perf_counter() - started,
        rows=len(getattr(result, "rows", None) or []),
    )
    return result
