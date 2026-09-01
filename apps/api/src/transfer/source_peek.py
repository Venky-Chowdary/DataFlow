"""Reading enough of a source to plan a transfer, without moving it.

Split out of ``src.transfer.stream`` (a module at its size budget). Preflight
needs the shape and a sample before anything is written, and that question is
separable from the streaming loop that answers it later.

Stream internals are imported inside the function: ``stream`` re-exports this
name for its historical import surface, so a module-level import would be
circular.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from .models import EndpointConfig

# Offset paging is only honest where the reader pages by numeric offset. Cursor /
# continuation-token readers (DynamoDB, Elasticsearch, Redis, Kafka) hold their
# state in the write loop, so a second independent pass cannot claim to have
# walked the same population — those stay preview-only evidence.
_OFFSET_PAGEABLE = frozenset({
    "postgresql", "mysql", "snowflake", "bigquery", "redshift", "sqlserver",
    "oracle", "sqlite", "generic_sql", "mongodb", "s3", "gcs", "adls", "sftp",
    "iceberg",
})


def peek_stream_source(source: EndpointConfig) -> tuple[list[str], dict[str, str], int, list[dict]]:
    """Return columns, schema, row count, and sample rows for preflight."""
    from .adapters import _introspect_table_schema, resolve_connector_config
    from .connector_capabilities import resolve_driver_type
    from .stream import CHUNK_SIZE, _read_batch, _source_name, _unwrap_read

    src_type = resolve_driver_type(source.format or "")
    src_cfg = resolve_connector_config(source)
    table = _source_name(source)
    if not table:
        raise ValueError("Source table/collection name required for streaming transfer")

    src_db = source.database or src_cfg.get("database") or ("test" if src_type == "mongodb" else "")

    probe, _ = _unwrap_read(_read_batch(src_type, src_cfg, table, None, 0, CHUNK_SIZE, database=src_db))
    columns = probe.headers
    if not columns and probe.total_rows == 0:
        raise ValueError(f"Source `{table}` has no columns or is empty")

    if src_type in ("s3", "gcs", "adls"):
        try:
            from services.object_store_introspect import profile_object_batch
            profiled = profile_object_batch(columns, probe.rows)
            schema = profiled.get("schema") or {c: "string" for c in columns}
        except Exception:
            schema = {c: "string" for c in columns}
    elif src_type == "redis":
        # Placeholder ``string`` is not a Redis declaration — Validate re-infers
        # from the same page. Peek must use that profiler or Execute invents TEXT
        # while the proof_bundle hashes NUMERIC/BIGINT (redis→SQL DDL identity).
        try:
            from services.object_store_introspect import (
                profile_schemaless_source_schema,
            )

            schema = profile_schemaless_source_schema(
                columns, probe.rows, source_format="redis"
            ) or {c: "string" for c in columns}
        except Exception:
            schema = {c: "string" for c in columns}
    else:
        probe_meta = getattr(probe, "meta", None) or {}
        native = probe_meta.get("native_types") or probe_meta.get("schema") or {}
        if isinstance(native, dict) and native:
            schema = {c: str(native.get(c) or "string") for c in columns}
        else:
            schema = _introspect_table_schema(src_type, src_cfg, table, columns)
            if not schema:
                schema = {c: "string" for c in columns}
    sample_rows = [dict(zip(probe.headers, row)) for row in probe.rows[:100]]
    return columns, schema, probe.total_rows, sample_rows


def iter_stream_source_column_rows(
    source: EndpointConfig,
    columns: list[str],
    *,
    limit: int = 0,
    chunk_size: int = 0,
    cursor_column: str = "",
    cursor_after: str | None = None,
    cursor_primary_key: str = "",
) -> Iterator[dict[str, Any]]:
    """Yield every source row, projected to ``columns``, for a pre-write check.

    A read-only second pass over the table the write loop is about to stream, so
    a bounded destination carrier (``NUMBER(11,8)``, ``VARCHAR(n)``) is decided
    before the first batch instead of by the writer at row 431. Projected on
    purpose: only the columns whose declared type can exceed their destination
    are read, so a table with no narrowing mapping costs nothing.

    Yields nothing for a reader that pages by opaque cursor rather than offset —
    the caller then reports unmeasured/preview evidence rather than claiming a
    population walk it did not do.

    Snapshot-scan sources (Postgres/MySQL/…) share the write loop's one-SELECT
    + ``fetchmany`` contract. A 1M-row walk that used OFFSET + COUNT-per-page
    was O(n²) and could time out, leaving Execute-time population fit
    unproven while Validate's 100-row sample still looked clean.
    """
    from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES, close_table_scan

    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type
    from .stream import CHUNK_SIZE, _read_batch, _source_name, _unwrap_read

    wanted = [c for c in (columns or []) if c]
    if not wanted:
        return
    src_type = resolve_driver_type(source.format or "")
    if src_type not in _OFFSET_PAGEABLE:
        return
    table = _source_name(source)
    if not table:
        return
    src_cfg = resolve_connector_config(source)
    src_db = source.database or src_cfg.get("database") or (
        "test" if src_type == "mongodb" else ""
    )
    page = int(chunk_size or CHUNK_SIZE)
    scan_state: dict[str, Any] | None = (
        {} if src_type in SNAPSHOT_SCAN_SOURCES else None
    )
    total: int | None = None
    offset = 0
    emitted = 0
    try:
        while True:
            extra: dict[str, Any] = {}
            if scan_state is not None:
                extra["scan_state"] = scan_state
            if total is not None:
                extra["known_total_rows"] = total
            if cursor_column:
                extra["cursor_column"] = cursor_column
                extra["cursor_after"] = cursor_after
                if cursor_primary_key:
                    extra["cursor_primary_key"] = cursor_primary_key
            batch, _ = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    wanted,
                    offset,
                    page,
                    database=src_db,
                    **extra,
                )
            )
            rows = list(batch.rows or [])
            if not rows:
                return
            if total is None:
                reported = getattr(batch, "total_rows", None)
                if isinstance(reported, int) and reported >= 0:
                    total = reported
            headers = list(batch.headers or wanted)
            for row in rows:
                yield dict(zip(headers, row))
                emitted += 1
                if limit > 0 and emitted >= limit:
                    return
            if len(rows) < page:
                return
            offset += len(rows)
            if total is not None and offset >= total:
                return
    finally:
        if scan_state is not None:
            close_table_scan(scan_state)


def iter_bounded_table_population_rows(
    source: EndpointConfig,
    mappings: Any,
    *,
    column_types: Mapping[str, str] | None = None,
    dest_types: Mapping[str, str] | None = None,
    dest_db: str = "",
    source_kind: str = "",
    source_format: str = "",
    limit: int = 0,
    shape_runner: Any = None,
    read_scope: Any = None,
    source_filter: Mapping[str, Any] | None = None,
) -> Iterator[dict[str, Any]] | None:
    """Projected table walk for the same population-fit scan Execute uses.

    Studio Validate used to post a 25–500 row preview. Execute already re-reads
    the bounded columns. One helper keeps Validate≡Execute: no second query
    planner.

    Returns ``None`` when this reader cannot page independently (cursor sources)
    or the table name is missing — callers must then use the preview, never
    claim an empty population. An empty iterator is a real empty table.
    Exceptions propagate so Validate can fall back instead of going silent.
    """
    from .connector_capabilities import resolve_driver_type
    from .stream import _source_name
    from services.population_fit_scan import bounded_targets

    targets, _undecidable, _safe = bounded_targets(
        mappings,
        dest_types=dest_types,
        source_types=column_types,
        dest_db=dest_db,
        source_kind=source_kind or getattr(source, "kind", "") or "",
        source_format=source_format or getattr(source, "format", "") or "",
    )
    wanted = sorted({t.source for t in targets if t.source})
    if not wanted:
        return iter(())
    src_type = resolve_driver_type(source.format or "")
    if src_type not in _OFFSET_PAGEABLE:
        return None
    if not _source_name(source):
        return None
    columns = (
        sorted({str(c) for c in shape_runner.recipe.input_columns})
        if shape_runner is not None
        else wanted
    )
    cursor_column = ""
    cursor_after = None
    cursor_pk = ""
    if read_scope is not None and getattr(read_scope, "bounded", False):
        cursor_column = str(getattr(read_scope, "cursor_column", "") or "")
        cursor_after = getattr(read_scope, "watermark", None)
        cursor_pk = str(getattr(read_scope, "primary_key", "") or "")
        for extra_col in (cursor_column, cursor_pk):
            if extra_col and extra_col not in columns:
                columns = [*columns, extra_col]
    if source_filter:
        from services.row_filter import filter_columns

        for extra_col in filter_columns(dict(source_filter)):
            if extra_col and extra_col not in columns:
                columns = [*columns, extra_col]

    def _walk() -> Iterator[dict[str, Any]]:
        rows = iter_stream_source_column_rows(
            source,
            columns,
            limit=limit,
            cursor_column=cursor_column,
            cursor_after=cursor_after,
            cursor_primary_key=cursor_pk,
        )
        if source_filter:
            from services.row_filter import iter_filtered_rows

            filtered = iter_filtered_rows(rows, dict(source_filter))
            if filtered is not None:
                rows = filtered
        if shape_runner is not None:
            from services.shape_apply import ShapeRunner

            probe = ShapeRunner(shape_runner.recipe)
            for row in rows:
                shaped = probe.records([dict(row)])
                if shaped:
                    yield shaped[0]
            return
        yield from rows

    from services.sync_cursor import iter_rows_after_watermark

    walked = _walk()
    if read_scope is not None and getattr(read_scope, "bounded", False):
        return iter_rows_after_watermark(walked, read_scope, keep_unreadable=True)
    return walked
