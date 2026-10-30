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


def _peek_each_file(
    source: EndpointConfig,
    *,
    src_type: str,
    src_cfg: dict[str, Any],
    table: str,
    src_db: str,
    sdk_state: str | None,
):
    """Peek each matched file in name order and tag rows with ``_df_file``.

    A glob validated as one batch treats a key shared by two files as a
    duplicate. Files are applied in that same order, so the key is last-wins
    and must not block. Returns None when the source is not a file pattern.

    A pattern is never read as an object key (D34): one match is peeked by
    its own name, no match is "file not arrived", and a listing or read
    error is that error — the old fallback re-read the glob literally and
    failed with "no object 'in/*.csv'".
    """
    if src_type not in {"sftp", "s3", "gcs", "adls"}:
        return None
    from services.file_pickup import (
        file_not_arrived,
        is_file_pattern,
        list_remote_files,
        ordered_files,
    )

    if not is_file_pattern(table):
        return None
    listed = ordered_files(
        list_remote_files(src_type, src_cfg, str(source.database or src_db or ""), table)
    )
    if not listed:
        raise file_not_arrived(table, getattr(source, "extra", None))
    from .stream import CHUNK_SIZE, _read_batch, _unwrap_read

    columns: list[str] | None = None
    schema: dict[str, str] | None = None
    sample_rows: list[dict[str, Any]] = []
    total = 0
    probe = None
    for item in listed:
        probe, _ = _unwrap_read(
            _read_batch(
                src_type,
                src_cfg,
                item.name,
                None,
                0,
                CHUNK_SIZE,
                database=src_db,
                sdk_state=sdk_state,
            )
        )
        if columns is None:
            columns = list(probe.headers or [])
            probe_meta = getattr(probe, "meta", None) or {}
            native = probe_meta.get("native_types") or probe_meta.get("schema") or {}
            if isinstance(native, dict) and native:
                schema = {col: str(native.get(col) or "string") for col in columns}
            else:
                schema = {col: "string" for col in columns}
        total += int(probe.total_rows or len(probe.rows or []) or 0)
        for raw in probe.rows or []:
            if len(sample_rows) >= 100:
                break
            record = dict(zip(probe.headers, raw))
            record["_df_file"] = item.name
            sample_rows.append(record)
    if not columns:
        return None
    return columns, schema or {col: "string" for col in columns}, total, sample_rows, probe


def peek_stream_source(
    source: EndpointConfig,
    *,
    sdk_state: str | None = None,
    include_batch: bool = False,
) -> tuple:
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

    multi = _peek_each_file(
        source,
        src_type=src_type,
        src_cfg=src_cfg,
        table=table,
        src_db=src_db,
        sdk_state=sdk_state,
    )
    if multi is not None:
        columns, schema, total_rows, sample_rows, probe = multi
        result = (columns, schema, total_rows, sample_rows)
        return (*result, probe) if include_batch else result

    probe, _ = _unwrap_read(
        _read_batch(
            src_type,
            src_cfg,
            table,
            None,
            0,
            CHUNK_SIZE,
            database=src_db,
            sdk_state=sdk_state,
        )
    )
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
    total_rows = probe.total_rows
    if total_rows is None and not probe.rows and (getattr(probe, "meta", None) or {}).get("final_page"):
        # A topic read to the run's frozen log end with nothing in it: a
        # measured 0, not an unknown size (D151).
        total_rows = 0
    result = (columns, schema, total_rows, sample_rows)
    return (*result, probe) if include_batch else result


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
    if cursor_column:
        # A cursor-bounded read is a keyset seek: the next page starts after
        # this page's last (cursor, key), so both must be read.
        for key_col in (cursor_column, cursor_primary_key):
            if key_col and key_col not in wanted:
                wanted.append(key_col)
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
            if cursor_column:
                # The keyset reader ignores ``offset``: re-sending the same
                # ``cursor_after`` re-read the first page for ever (a resumed
                # incremental run's preflight walk never ended). Seek past this
                # page's high mark - the write loop's own advance - and stop
                # if the mark cannot move.
                from services.sync_cursor import compare_cursor_values, max_cursor_value

                mark = max_cursor_value(
                    [list(r) for r in rows], headers, cursor_column,
                    cursor_primary_key or None,
                )
                if mark is None or (
                    cursor_after not in (None, "")
                    and compare_cursor_values(mark, str(cursor_after)) <= 0
                ):
                    return
                cursor_after = mark
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
    source_columns: list[str] | None = None,
) -> Iterator[dict[str, Any]] | None:
    """Projected table walk for the same population-fit scan Execute uses.

    Studio Validate used to post a 25–500 row preview. Execute already re-reads
    the bounded columns. One helper keeps Validate≡Execute: no second query
    planner.

    Returns ``None`` when this reader cannot page independently (cursor sources)
    or the table name is missing — callers must then use the preview, never
    claim an empty population. An empty iterator is a real empty table.
    Exceptions propagate so Validate can fall back instead of going silent.

    ``source_columns`` names the columns to walk for a probe that is not the
    bounded-carrier fit (the destination CHECK probe); by default the walk
    reads the source side of every bounded target.
    """
    from .connector_capabilities import resolve_driver_type
    from .stream import _source_name
    from services.population_fit_scan import bounded_targets

    if source_columns is not None:
        wanted = sorted({str(c) for c in source_columns if str(c)})
    else:
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
            yield from shape_runner.fork().population(rows)
            return
        yield from rows

    from services.sync_cursor import iter_rows_after_watermark

    walked = _walk()
    if read_scope is not None and getattr(read_scope, "bounded", False):
        return iter_rows_after_watermark(walked, read_scope, keep_unreadable=True)
    return walked
