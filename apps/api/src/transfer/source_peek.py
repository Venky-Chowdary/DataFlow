"""Reading enough of a source to plan a transfer, without moving it.

Split out of ``src.transfer.stream`` (a module at its size budget). Preflight
needs the shape and a sample before anything is written, and that question is
separable from the streaming loop that answers it later.

Stream internals are imported inside the function: ``stream`` re-exports this
name for its historical import surface, so a module-level import would be
circular.
"""

from __future__ import annotations

from collections.abc import Iterator
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
    """
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
    offset = 0
    emitted = 0
    while True:
        batch, _ = _unwrap_read(
            _read_batch(src_type, src_cfg, table, wanted, offset, page, database=src_db)
        )
        rows = list(batch.rows or [])
        if not rows:
            return
        headers = list(batch.headers or wanted)
        for row in rows:
            yield dict(zip(headers, row))
            emitted += 1
            if limit > 0 and emitted >= limit:
                return
        if len(rows) < page:
            return
        offset += len(rows)
