"""Streaming DB→DB transfer — batched read/write without loading full dataset into RAM."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Callable

from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from connectors.writer_common import CHUNK_SIZE  # noqa: E402

from .adapters import _introspect_table_schema, resolve_connector_config, resolve_dest_table
from .connector_capabilities import resolve_driver_type


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    return {
        "rejected_rows": rejected,
        "rejected_details": list(getattr(result, "rejected_details", []) or [])[:50],
        "warnings": list(getattr(result, "warnings", []) or [])[:10],
        "error_policy": "quarantine" if rejected else "none",
        "load_method": getattr(result, "load_method", None),
    }


_STREAMING_TYPES = frozenset({
    "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
    "s3", "gcs", "dynamodb", "elasticsearch", "redis", "sqlite", "generic_sql",
})


def _source_name(source: EndpointConfig) -> str:
    from .connector_capabilities import resolve_driver_type
    fmt = resolve_driver_type(source.format or "")
    if fmt == "mongodb":
        return source.collection or source.table or ""
    if fmt == "dynamodb":
        return source.database or source.table or ""
    if fmt == "elasticsearch":
        return source.database or source.table or source.collection or ""
    if fmt == "s3":
        return source.table or source.collection or source.schema or ""
    if fmt == "gcs":
        return source.table or source.collection or source.schema or ""
    if fmt == "redis":
        return source.table or source.collection or source.schema or "*"
    return source.table or source.collection or ""


def _read_batch(
    src_type: str,
    cfg: dict[str, Any],
    table: str,
    columns: list[str] | None,
    offset: int,
    limit: int,
    database: str = "",
    dynamodb_cursor: dict | None = None,
    dynamodb_total: int | None = None,
    *,
    cursor_column: str = "",
    cursor_after: str | None = None,
    known_total_rows: int | None = None,
    es_search_after: list | None = None,
    redis_scan_state=None,
):
    if src_type == "postgresql" or src_type == "redshift":
        from connectors.postgresql_reader import read_table_batch, read_table_cursor_batch

        pg_port = int(cfg.get("port") or (5439 if src_type == "redshift" else 5432))
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=pg_port,
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "public"),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
            )
        return read_table_batch(
            host=cfg["host"],
            port=pg_port,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "mysql":
        from connectors.mysql_reader import read_table_batch, read_table_cursor_batch

        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 3306),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
            )
        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 3306),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "mongodb":
        from connectors.mongodb_reader import read_collection_batch, read_collection_cursor_batch

        if cursor_column:
            return read_collection_cursor_batch(
                cfg=cfg,
                database=database or cfg.get("database", "test"),
                collection=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
            )
        return read_collection_batch(
            cfg=cfg,
            database=database or cfg.get("database", "test"),
            collection=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "snowflake":
        from connectors.snowflake_reader import read_table_batch, read_table_cursor_batch

        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 443),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "PUBLIC"),
                connection_string=cfg.get("connection_string", ""),
                warehouse=cfg.get("warehouse", ""),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
            )
        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "bigquery":
        from connectors.bigquery_reader import read_table_batch

        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "dataflow"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "gcs":
        from connectors.gcs_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "s3":
        from connectors.s3_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "dynamodb":
        from connectors.dynamodb_reader import read_table_batch

        batch, _next = read_table_batch(
            cfg=cfg,
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            exclusive_start_key=dynamodb_cursor,
            total_rows=dynamodb_total,
        )
        return batch, _next
    if src_type == "elasticsearch":
        from connectors.elasticsearch_reader import read_index_batch

        return read_index_batch(
            cfg=cfg, index=table, columns=columns, limit=limit,
            known_total_rows=known_total_rows, search_after=es_search_after,
        )
    if src_type == "redis":
        from connectors.redis_reader import read_keys_batch

        return read_keys_batch(
            cfg=cfg, pattern=table or "*", limit=limit,
            known_total_rows=known_total_rows, scan_state=redis_scan_state,
        )
    if src_type == "sqlite":
        from connectors.sqlite_reader import read_table_batch

        return read_table_batch(
            host=cfg["host"],
            port=0,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            table=table,
            limit=limit,
            offset=offset,
        )
    if resolve_driver_type(src_type) == "generic_sql":
        from connectors.generic_sql import read_table_batch, read_table_cursor_batch

        type_name = cfg.get("type", "") or src_type
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=False,
                type=type_name,
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
            )
        return read_table_batch(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            type=type_name,
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    raise ValueError(f"Streaming read not supported for source type '{src_type}'")


def _unwrap_read(result):
    """Normalize _read_batch return — dynamodb returns (batch, cursor)."""
    if isinstance(result, tuple) and len(result) == 2 and hasattr(result[0], "headers"):
        return result
    return result, None


def _write_batch(
    dest_type: str,
    dest: EndpointConfig,
    cfg: dict[str, Any],
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    create_table: bool,
    on_checkpoint: Callable[[int, int, int], None] | None,
    chunk_idx: int,
    total_chunks: int,
    rows_so_far: int,
    *,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
) -> tuple[int, str, dict]:
    if dest_type == "postgresql" or dest_type == "redshift":
        from connectors.postgresql_writer import write_mapped_rows

        pg_port = int(cfg.get("port") or (5439 if dest_type == "redshift" else 5432))
        result = write_mapped_rows(
            host=cfg["host"],
            port=pg_port,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or f"{dest_type} batch write failed")
        summary = {
            "type": dest_type,
            "schema": result.target_schema,
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        return result.rows_written, result.checksum, summary

    if dest_type == "mysql":
        from connectors.mysql_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 3306),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "MySQL batch write failed")
        summary = {
            "type": "mysql",
            "database": cfg["database"],
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        return result.rows_written, result.checksum, summary

    if dest_type == "mongodb":
        from connectors.mongodb_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 27017),
            database=dest.database or cfg.get("database") or "test_db",
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "db"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "MongoDB batch write failed")
        summary = {
            "type": "mongodb",
            "database": result.target_schema,
            "collection": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        return result.rows_written, result.checksum, summary

    if dest_type == "sqlite":
        from connectors.sqlite_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=0,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "SQLite batch write failed")
        summary = {
            "type": "sqlite",
            "database": cfg["database"],
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        return result.rows_written, result.checksum, summary

    if dest_type == "snowflake":
        from connectors.snowflake_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "Snowflake batch write failed")
        summary = {"type": "snowflake", "schema": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    if dest_type == "bigquery":
        from connectors.bigquery_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "dataflow"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "BigQuery batch write failed")
        summary = {"type": "bigquery", "dataset": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    if dest_type in ("s3", "gcs", "dynamodb", "elasticsearch", "redis"):
        writers = {
            "s3": "connectors.s3_writer",
            "gcs": "connectors.gcs_writer",
            "dynamodb": "connectors.dynamodb_writer",
            "elasticsearch": "connectors.elasticsearch_writer",
            "redis": "connectors.redis_writer",
        }
        import importlib
        mod = importlib.import_module(writers[dest_type])
        result = mod.write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 0),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or f"{dest_type} batch write failed")
        summary = {"type": dest_type, "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    if resolve_driver_type(dest_type) == "generic_sql":
        from connectors.generic_sql import write_mapped_rows

        type_name = cfg.get("type", "") or dest_type
        result = write_mapped_rows(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            type=type_name,
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or f"{dest_type} batch write failed")
        summary = {"type": type_name, "schema": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    raise ValueError(f"Streaming write not supported for destination type '{dest_type}'")


def stream_database_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    *,
    sync_mode: str = "full_refresh_overwrite",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """
    Extract source table in CHUNK_SIZE batches and load to destination.
    Returns (rows_written, ddl_log, dest_summary, columns).
    """
    from .connector_capabilities import resolve_driver_type
    src_type = resolve_driver_type(source.format)
    dest_type = resolve_driver_type(destination.format)
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)

    from services.sync_cursor import (
        build_cursor_key,
        get_watermark,
        map_source_to_target,
        max_cursor_value,
        requires_incremental,
        requires_upsert,
        resolve_sync_contract,
        set_watermark,
    )

    contract = resolve_sync_contract(stream_contracts)
    effective_sync = contract.sync_mode if contract else sync_mode
    incremental = requires_incremental(effective_sync)
    cursor_source_col = contract.cursor_field if contract else ""
    cursor_col = cursor_source_col
    if cursor_source_col and mappings:
        cursor_col = map_source_to_target(cursor_source_col, mappings)
    pk_target_cols: list[str] = []
    if contract and contract.primary_key:
        pk_target_cols = [map_source_to_target(contract.primary_key, mappings)]
    write_mode = "upsert" if requires_upsert(effective_sync) and pk_target_cols else "insert"
    watermark = None
    cursor_key = ""
    if incremental and cursor_source_col:
        dest_table_key = resolve_dest_table(dest_type, destination, _source_name(source))
        cursor_key = build_cursor_key(
            source_type=src_type,
            source_database=source.database or src_cfg.get("database", ""),
            source_object=_source_name(source),
            dest_type=dest_type,
            dest_database=destination.database or dest_cfg.get("database", ""),
            dest_object=dest_table_key,
            stream_name=contract.name if contract else "stream",
        )
        watermark = get_watermark(cursor_key)

    if src_type not in _STREAMING_TYPES:
        raise ValueError(f"Streaming source '{src_type}' not supported")
    if dest_type not in _STREAMING_TYPES:
        raise ValueError(f"Streaming destination '{dest_type}' not supported")

    table = _source_name(source)
    if not table:
        raise ValueError("Source table/collection name required for streaming transfer")

    src_db = source.database or src_cfg.get("database") or ("test" if src_type == "mongodb" else "")

    if src_type == "gcs" and not src_cfg.get("database"):
        raise ValueError("GCS source requires bucket name in the database field")
    if src_type == "s3" and not src_cfg.get("database"):
        raise ValueError("S3 source requires bucket name in the database field")

    if src_type in ("s3", "gcs"):
        from connectors.object_read_cache import clear_object_cache

        clear_object_cache()

    probe, ddb_cursor = _unwrap_read(
        _read_batch(
            src_type, src_cfg, table, None, 0, CHUNK_SIZE, database=src_db,
            cursor_column=cursor_source_col if incremental else "",
            cursor_after=watermark if incremental else None,
        )
    )
    columns = probe.headers
    if not columns:
        raise ValueError(f"Source table `{table}` has no columns or is empty")

    if not schema:
        if src_type in ("s3", "gcs"):
            try:
                from services.object_store_introspect import profile_object_batch
                profiled = profile_object_batch(columns, probe.rows)
                schema = profiled.get("schema") or {c: "string" for c in columns}
            except Exception:
                schema = {c: "string" for c in columns}
        elif src_type == "redis":
            schema = {c: "string" for c in columns}
        else:
            schema = _introspect_table_schema(src_type, src_cfg, table, columns)
            if not schema:
                schema = {c: "string" for c in columns}

    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    total_rows = probe.total_rows
    chunks = max(1, (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE) if total_rows else 1
    dest_table = resolve_dest_table(dest_type, destination, table)

    ddl_log: list[str] = [
        f"STREAM {src_type}.{table} → {dest_type}.{dest_table} "
        f"({total_rows:,} rows est., {chunks} batches, sync={effective_sync})"
    ]
    if incremental and watermark:
        ddl_log.append(f"INCREMENTAL cursor {cursor_source_col} > {watermark}")
    for col in columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")

    written = 0
    offset = 0
    chunk_idx = 0
    dest_summary: dict[str, Any] = {}
    last_checksum = ""
    rejected_total = 0
    warning_samples: list[str] = []
    ddb_total = probe.total_rows if src_type == "dynamodb" else None
    batch = probe
    running_cursor = watermark
    es_search_after: list | None = None
    redis_scan_state = None
    keyset_col = columns[0] if columns and not incremental else ""
    keyset_after: str | None = None
    use_keyset = bool(keyset_col) and src_type in ("postgresql", "redshift", "mysql", "snowflake", "mongodb")

    while True:
        if not batch.rows:
            break

        chunk_idx += 1
        batch_written, last_checksum, dest_summary = _write_batch(
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            batch.headers,
            batch.rows,
            mappings,
            column_types,
            create_table=(chunk_idx == 1),
            on_checkpoint=on_checkpoint,
            chunk_idx=chunk_idx,
            total_chunks=chunks,
            rows_so_far=written,
            write_mode=write_mode,
            conflict_columns=pk_target_cols or None,
        )
        written += batch_written
        if incremental and cursor_source_col:
            batch_max = max_cursor_value(batch.rows, batch.headers, cursor_source_col)
            if batch_max and (running_cursor is None or batch_max > running_cursor):
                running_cursor = batch_max
        rejected_total += int(dest_summary.get("rejected_rows", 0) or 0)
        warning_samples.extend(dest_summary.get("warnings", []) or [])
        if on_checkpoint:
            on_checkpoint(chunk_idx, chunks, written)
        offset += len(batch.rows)

        if src_type == "dynamodb":
            if not ddb_cursor:
                break
            batch, ddb_cursor = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    dynamodb_cursor=ddb_cursor,
                    dynamodb_total=ddb_total,
                )
            )
        elif incremental and cursor_source_col and src_type in ("postgresql", "redshift", "mysql", "snowflake", "mongodb"):
            if len(batch.rows) < CHUNK_SIZE:
                break
            batch, _ = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    cursor_column=cursor_source_col,
                    cursor_after=running_cursor,
                    known_total_rows=total_rows,
                )
            )
        elif use_keyset:
            if len(batch.rows) < CHUNK_SIZE:
                break
            if keyset_col in batch.headers and batch.rows:
                keyset_after = batch.rows[-1][batch.headers.index(keyset_col)]
            batch, extra = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    cursor_column=keyset_col,
                    cursor_after=keyset_after,
                    known_total_rows=total_rows,
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra
        elif src_type == "elasticsearch":
            if len(batch.rows) < CHUNK_SIZE:
                break
            batch, es_search_after = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    known_total_rows=total_rows,
                    es_search_after=es_search_after,
                )
            )
        elif src_type == "redis":
            if len(batch.rows) < CHUNK_SIZE:
                break
            batch, redis_scan_state = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    known_total_rows=total_rows,
                    redis_scan_state=redis_scan_state,
                )
            )
        elif offset >= total_rows:
            break
        else:
            batch, extra = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    offset,
                    CHUNK_SIZE,
                    database=src_db,
                    known_total_rows=total_rows,
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra

    if written == 0 and incremental:
        ddl_log.append("INCREMENTAL — no new rows since last watermark")
        dest_summary["sync_mode"] = effective_sync
        dest_summary["watermark"] = watermark
        return 0, ddl_log, dest_summary, columns

    if written == 0:
        raise ValueError("Source table is empty")

    if incremental and running_cursor and cursor_key and running_cursor != watermark:
        set_watermark(cursor_key, running_cursor, metadata={"job_id": job_id, "sync_mode": effective_sync})

    dest_summary["checksum"] = last_checksum
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["warnings"] = warning_samples[:10]
    dest_summary["error_policy"] = "quarantine" if rejected_total else "none"
    dest_summary["sync_mode"] = effective_sync
    dest_summary["watermark"] = running_cursor
    ddl_log.insert(1, f"CREATE TABLE IF NOT EXISTS {dest_table}")
    return written, ddl_log, dest_summary, columns


def peek_stream_source(source: EndpointConfig) -> tuple[list[str], dict[str, str], int, list[dict]]:
    """Return columns, schema, row count, and sample rows for preflight."""
    src_type = source.format.lower()
    src_cfg = resolve_connector_config(source)
    table = _source_name(source)
    if not table:
        raise ValueError("Source table/collection name required for streaming transfer")

    src_db = source.database or src_cfg.get("database") or ("test" if src_type == "mongodb" else "")

    probe, _ = _unwrap_read(_read_batch(src_type, src_cfg, table, None, 0, CHUNK_SIZE, database=src_db))
    columns = probe.headers
    if not columns and probe.total_rows == 0:
        raise ValueError(f"Source `{table}` has no columns or is empty")

    if src_type in ("s3", "gcs"):
        try:
            from services.object_store_introspect import profile_object_batch
            profiled = profile_object_batch(columns, probe.rows)
            schema = profiled.get("schema") or {c: "string" for c in columns}
        except Exception:
            schema = {c: "string" for c in columns}
    elif src_type == "redis":
        schema = {c: "string" for c in columns}
    else:
        schema = _introspect_table_schema(resolve_driver_type(src_type), src_cfg, table, columns)
        if not schema:
            schema = {c: "string" for c in columns}
    sample_rows = [dict(zip(probe.headers, row)) for row in probe.rows[:100]]
    return columns, schema, probe.total_rows, sample_rows


def supports_streaming(source: EndpointConfig, destination: EndpointConfig) -> bool:
    if source.kind != "database" or destination.kind != "database":
        return False
    from .connector_capabilities import resolve_driver_type
    return (
        resolve_driver_type(source.format) in _STREAMING_TYPES
        and resolve_driver_type(destination.format) in _STREAMING_TYPES
    )
