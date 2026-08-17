"""Reading enough of a source to plan a transfer, without moving it.

Split out of ``src.transfer.stream`` (a module at its size budget). Preflight
needs the shape and a sample before anything is written, and that question is
separable from the streaming loop that answers it later.

Stream internals are imported inside the function: ``stream`` re-exports this
name for its historical import surface, so a module-level import would be
circular.
"""

from __future__ import annotations

from .models import EndpointConfig


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
