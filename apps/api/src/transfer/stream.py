"""Streaming DB→DB transfer — batched read/write without loading full dataset into RAM."""

from __future__ import annotations

import os
import re
import sys
from functools import partial
from pathlib import Path
from typing import Any, Callable

from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred

_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from connectors.writer_common import (  # noqa: E402
    CHUNK_SIZE,
    build_mapped_rows,
    resolve_target_columns,
    row_fingerprints,
    transform_error_policy_for_validation_mode,
)

# Keep resilient batch/quarantine path importable for streaming callers.
from services.resilience import ResilientBatcher, adaptive_chunk_size  # noqa: E402, F401

from .adapters import (
    _introspect_table_schema,
    resolve_connector_config,
    resolve_dest_table,
)
from .connector_capabilities import resolve_driver_type

try:
    from services.checkpoint_service import Checkpoint, CheckpointService
    from services.data_quality import run_integrity_audit
    from services.error_handling import RetryBudget, with_retry
    from services.parallel_chunks import ChunkDispatcher
    from services.reconciliation import FingerprintAccumulator
    from services.resilience import adaptive_chunk_size
    from services.row_filter import apply_row_filter_to_matrix
except ImportError:  # pragma: no cover - tests with api root on path
    from src.services.checkpoint_service import Checkpoint, CheckpointService
    from src.services.data_quality import run_integrity_audit
    from src.services.error_handling import RetryBudget, with_retry
    from src.services.parallel_chunks import ChunkDispatcher
    from src.services.reconciliation import FingerprintAccumulator
    from src.services.resilience import adaptive_chunk_size
    from src.services.row_filter import apply_row_filter_to_matrix


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    coerced = int(getattr(result, "coerced_null_rows", 0) or 0)
    return {
        "rejected_rows": rejected,
        "coerced_null_rows": coerced,
        "rejected_details": list(getattr(result, "rejected_details", []) or [])[:500],
        "warnings": list(getattr(result, "warnings", []) or [])[:10],
        "error_policy": "quarantine" if (rejected or coerced) else "none",
        "load_method": getattr(result, "load_method", None),
    }


_STREAMING_TYPES = frozenset({
    "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
    "sqlserver", "oracle",
    "s3", "gcs", "adls", "sftp", "dynamodb", "elasticsearch", "redis", "sqlite", "generic_sql",
    "iceberg", "kafka", "salesforce", "hubspot",
})


def _source_name(source: EndpointConfig) -> str:
    from .connector_capabilities import resolve_driver_type
    fmt = resolve_driver_type(source.format or "")
    if fmt == "mongodb":
        return source.collection or source.table or ""
    if fmt == "dynamodb":
        return source.table or source.collection or source.database or ""
    if fmt == "elasticsearch":
        return source.table or source.database or source.collection or ""
    if fmt == "s3":
        return source.table or source.collection or source.schema or ""
    if fmt == "gcs":
        return source.table or source.collection or source.schema or ""
    if fmt == "adls":
        return source.table or source.collection or source.schema or ""
    if fmt == "redis":
        return source.table or source.collection or source.schema or "*"
    return source.table or source.collection or ""


def _read_batch_impl(
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
    cursor_type: str | None = None,
    known_total_rows: int | None = None,
    es_search_after: list | None = None,
    redis_scan_state=None,
):
    if src_type == "postgresql" or src_type == "redshift":
        from connectors.postgresql_reader import (
            read_table_batch,
            read_table_cursor_batch,
        )

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
        from connectors.mongodb_reader import (
            read_collection_batch,
            read_collection_cursor_batch,
        )

        if cursor_column:
            return read_collection_cursor_batch(
                cfg=cfg,
                database=database or cfg.get("database", "test"),
                collection=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                cursor_type=cursor_type,
                columns=columns,
                limit=limit,
                known_total_rows=known_total_rows,
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
        from connectors.snowflake_reader import (
            read_table_batch,
            read_table_cursor_batch,
        )

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
                role=cfg.get("role", ""),
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
            role=cfg.get("role", ""),
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
            service_account=cfg.get("service_account", ""),
        )
    if src_type == "gcs":
        from connectors.gcs_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "s3":
        from connectors.s3_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "adls":
        from connectors.adls_reader import read_object

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

        pattern = table or "*"
        if pattern != "*" and "*" not in pattern and "?" not in pattern:
            pattern = f"{pattern}:*"
        return read_keys_batch(
            cfg=cfg, pattern=pattern, limit=limit,
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
            known_total_rows=known_total_rows,
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
    if src_type in ("sqlserver", "oracle"):
        from .connector_dispatch import read_via_registry

        return read_via_registry(
            src_type,
            cfg=cfg,
            table=table,
            limit=limit,
            offset=offset,
            columns=columns,
        )
    if src_type in ("salesforce", "hubspot"):
        from .connector_dispatch import read_via_registry

        return read_via_registry(src_type, cfg=cfg, table=table, limit=limit, offset=offset)
    raise ValueError(f"Streaming read not supported for source type '{src_type}'")


def _read_batch(*args, **kwargs):
    """Retry transient source-read errors without leaking partial state."""
    return with_retry(
        lambda: _read_batch_impl(*args, **kwargs),
        budget=RetryBudget(
            max_attempts=3,
            base_delay_seconds=0.5,
            max_delay_seconds=5.0,
            exponential_base=2.0,
            jitter=True,
        ),
    )


def _unwrap_read(result):
    """Normalize _read_batch return — dynamodb returns (batch, cursor)."""
    if isinstance(result, tuple) and len(result) == 2 and hasattr(result[0], "headers"):
        return result
    return result, None


def _raise_write_failure(result: Any, label: str) -> None:
    """Fail a batch write; connection drops are retriable when the writer ledger can skip commits."""
    err = result.error or label
    written = int(getattr(result, "rows_written", 0) or 0)
    try:
        from connectors.write_resilience import is_connection_lost
    except ImportError:
        is_connection_lost = lambda _e: False  # noqa: E731
    # Writers stamp committed chunks in `_dataflow_write_ledger` when job_id is set,
    # so re-invoking the same batch after a proxy drop is safe (already-written
    # chunks are skipped). Prefer ConnectionError so with_retry can finish the job.
    if is_connection_lost(err):
        raise ConnectionError(err)
    if written > 0:
        raise RuntimeError(f"partial write ({written} rows committed before failure): {err}")
    raise RuntimeError(err)


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
    on_checkpoint: Callable[..., None] | None,
    chunk_idx: int,
    total_chunks: int,
    rows_so_far: int,
    *,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    backfill_new_fields: bool = False,
    error_policy: str | None = None,
    connection: Any | None = None,
    close_connection: bool | None = None,
    skip_session_setup: bool = False,
    job_id: str | None = None,
) -> tuple[int, str, dict]:
    if dest_type == "postgresql" or dest_type == "redshift":
        from connectors.postgresql_writer import write_mapped_rows
        from connectors.write_resilience import build_write_batch_key

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
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            job_id=job_id,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            _raise_write_failure(result, f"{dest_type} batch write failed")
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
        from connectors.write_resilience import build_write_batch_key

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
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            job_id=job_id,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            _raise_write_failure(result, "MySQL batch write failed")
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
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
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
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
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
            role=cfg.get("role", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            connection=connection,
            close_connection=close_connection,
            skip_session_setup=skip_session_setup,
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
            service_account=cfg.get("service_account", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "BigQuery batch write failed")
        summary = {"type": "bigquery", "dataset": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    if dest_type in ("s3", "gcs", "adls", "dynamodb", "elasticsearch", "redis", "pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        writers = {
            "s3": "connectors.s3_writer",
            "gcs": "connectors.gcs_writer",
            "adls": "connectors.adls_writer",
            "dynamodb": "connectors.dynamodb_writer",
            "elasticsearch": "connectors.elasticsearch_writer",
            "redis": "connectors.redis_writer",
            "pgvector": "connectors.pgvector_writer",
            "qdrant": "connectors.qdrant_writer",
            "weaviate": "connectors.weaviate_writer",
            "pinecone": "connectors.pinecone_writer",
            "milvus": "connectors.milvus_writer",
        }
        import importlib
        mod = importlib.import_module(writers[dest_type])
        kwargs = {
            "host": cfg["host"],
            "port": int(cfg.get("port") or 0),
            "database": cfg["database"],
            "username": cfg.get("username", ""),
            "password": cfg.get("password", ""),
            "schema": cfg.get("schema", ""),
            "connection_string": cfg.get("connection_string", ""),
            "ssl": cfg.get("ssl", False),
            "warehouse": cfg.get("warehouse", ""),
            "role": cfg.get("role", ""),
            "auth_mode": cfg.get("auth_mode", ""),
            "api_key": cfg.get("api_key", ""),
            "service_account": cfg.get("service_account", ""),
            "auth_source": cfg.get("auth_source", ""),
            # MinIO / custom S3 / DynamoDB local — must reach writers (was dropped).
            "endpoint_url": cfg.get("endpoint_url", "") or "",
            "path_style": bool(cfg.get("path_style", False)),
            "region": cfg.get("region", "") or "",
            "table_name": table_name,
            "headers": headers,
            "data_rows": data_rows,
            "mappings": mappings,
            "column_types": column_types,
            "create_table": create_table,
            "error_policy": error_policy,
            "on_checkpoint": lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        }
        if dest_type in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
            extra = getattr(dest, "extra", {}) or {}
            kwargs["content_column"] = extra.get("content_column")
            kwargs["embedding_column"] = extra.get("embedding_column")
            kwargs["metadata_columns"] = extra.get("metadata_columns")
            kwargs["exclude_pii_columns"] = extra.get("exclude_pii_columns")
            kwargs["embedding_model"] = extra.get("embedding_model")
            kwargs["chunk_size"] = int(extra.get("chunk_size", 512)) if extra.get("chunk_size") else 512
            kwargs["chunk_overlap"] = int(extra.get("chunk_overlap", 50)) if extra.get("chunk_overlap") else 50
            kwargs["skip_chunking"] = bool(extra.get("skip_chunking"))
        result = mod.write_mapped_rows(**kwargs)
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
            ssl=cfg.get("ssl", False),
            type=type_name,
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or f"{dest_type} batch write failed")
        summary = {"type": type_name, "schema": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        return result.rows_written, result.checksum, summary

    from .connector_dispatch import has_writer, write_via_registry

    if has_writer(dest_type):
        mode = write_mode
        if dest_type in ("salesforce", "hubspot") and (mode or "insert").lower() == "insert":
            mode = "upsert"
        common = {
            "host": cfg.get("host", ""),
            "port": int(cfg.get("port") or 0),
            "database": cfg.get("database", ""),
            "username": cfg.get("username", ""),
            "password": cfg.get("password", ""),
            "schema": cfg.get("schema", ""),
            "connection_string": cfg.get("connection_string", ""),
            "ssl": cfg.get("ssl", False),
            "api_key": cfg.get("api_key", ""),
            "table_name": table_name,
            "headers": headers,
            "data_rows": data_rows,
            "mappings": mappings,
            "column_types": column_types,
            "create_table": create_table,
            "error_policy": error_policy,
            "on_checkpoint": (
                (lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r))
                if on_checkpoint
                else None
            ),
        }
        extra = {}
        if dest_type == "kafka":
            extra["schema_registry_url"] = str(
                (getattr(dest, "extra", None) or {}).get("schema_registry_url")
                or cfg.get("schema_registry_url")
                or ""
            )
        result = write_via_registry(
            dest_type,
            common=common,
            write_mode=mode or "insert",
            conflict_columns=conflict_columns or [],
            extra=extra or None,
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

    raise ValueError(f"Streaming write not supported for destination type '{dest_type}'")


def stream_database_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_append",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    limit: int = 0,
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
        compare_cursor_values,
        get_watermark,
        map_source_to_target,
        max_cursor_value,
        requires_incremental,
        requires_upsert,
        resolve_effective_sync_mode,
        resolve_sync_contract,
        set_watermark,
    )

    contract = resolve_sync_contract(stream_contracts)
    effective_sync = resolve_effective_sync_mode(
        sync_mode,
        contract.sync_mode if contract else None,
    )
    incremental = requires_incremental(effective_sync)
    cursor_source_col = contract.cursor_field if contract else ""
    pk_target_cols: list[str] = []
    if contract and contract.primary_key:
        pk_target_cols = [
            map_source_to_target(col, mappings) for col in contract.primary_key_columns()
        ]
    write_mode = "upsert" if requires_upsert(effective_sync) and pk_target_cols else "insert"
    # Parallel/chunked resume is only safe with idempotent writes.
    resuming = bool(checkpoint and getattr(checkpoint, "chunk_index", 0) > 0)
    if resuming and write_mode == "insert":
        if pk_target_cols:
            write_mode = "upsert"
        else:
            raise ValueError(
                "Cannot resume a streaming insert without a primary key; "
                "set primary_key on the stream contract or use an upsert sync mode"
            )
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

    if src_type in ("s3", "gcs", "adls", "sftp"):
        from services.object_streaming import (
            download_for_object_store,
            download_object,
            stream_spilled_file_to_database,
        )

        bucket = source.database or src_cfg.get("database", "")
        key = table
        if src_type in ("s3", "gcs") and not bucket:
            raise ValueError(f"{src_type.upper()} source requires bucket name in the database field")
        cache_key = f"{src_type}:{bucket}:{key}"
        # Always fetch a fresh copy for a transfer run; atomic download protects
        # against partial/corrupt reuse.
        path = download_object(
            cache_key,
            lambda p: download_for_object_store(src_type, p, src_cfg, bucket, key),
            force=True,
        )
        return stream_spilled_file_to_database(
            path=path,
            filename=key,
            destination=destination,
            mappings=mappings,
            schema=schema,
            sync_mode=effective_sync,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
        )

    # Memory-safe chunk sizing: sample a few rows, then size batches to keep
    # per-batch memory within a destination-safe limit while respecting CHUNK_SIZE.
    sample_limit = 100 if limit == 0 else min(100, limit)
    sample_probe, _ = _unwrap_read(
        _read_batch(
            src_type, src_cfg, table, None, 0, sample_limit, database=src_db,
            cursor_column=cursor_source_col if incremental else "",
            cursor_after=watermark if incremental else None,
            cursor_type=normalize_inferred(schema.get(cursor_source_col, "string")).upper() if schema and incremental else None,
        )
    )
    sample_rows = sample_probe.rows or []
    avg_row_size = 100
    if sample_rows:
        avg_row_size = max(1, int(sum(len(str(row)) for row in sample_rows) / len(sample_rows)))
    # Warehouses benefit from larger stream chunks so batches clear COPY/MERGE
    # thresholds; Mongo already used 64MB — extend the same class to SF/BQ/RS.
    _warehouse_dests = {"mongodb", "snowflake", "bigquery", "redshift"}
    target_memory_bytes = 64 * 1024 * 1024 if dest_type in _warehouse_dests else 8 * 1024 * 1024
    chunk_size = adaptive_chunk_size(CHUNK_SIZE, avg_row_size, max_size=CHUNK_SIZE, target_memory_bytes=target_memory_bytes)
    # Cap stream batches on public TCP proxies so one socket never holds a 20k-row window.
    try:
        from connectors.write_resilience import proxy_stream_batch_size
    except ImportError:
        proxy_stream_batch_size = None  # type: ignore
    if proxy_stream_batch_size is not None:
        chunk_size = proxy_stream_batch_size(
            dest_cfg.get("host"),
            connection_string=dest_cfg.get("connection_string") or dest_cfg.get("uri") or "",
            default=chunk_size,
        )
    # Object-store writers (S3/GCS/ADLS) emit a single destination object per call.
    # Chunked writes would overwrite the same key and silently lose data.
    # Force a single chunk so all rows are written once.
    if dest_type in ("s3", "gcs", "adls") and sample_probe.total_rows:
        chunk_size = max(1, sample_probe.total_rows)

    def _batch_limit(offset: int = 0, *, default: int = chunk_size) -> int:
        if limit > 0:
            return max(0, min(default, limit - offset))
        return default

    probe, ddb_cursor = _unwrap_read(
        _read_batch(
            src_type, src_cfg, table, None, 0, _batch_limit(0), database=src_db,
            cursor_column=cursor_source_col if incremental else "",
            cursor_after=watermark if incremental else None,
            cursor_type=normalize_inferred(schema.get(cursor_source_col, "string")).upper() if schema and incremental else None,
        )
    )
    columns = probe.headers
    if not columns:
        raise ValueError(f"Source table `{table}` has no columns or is empty")

    if not schema:
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
            schema = _introspect_table_schema(src_type, src_cfg, table, columns)
            if not schema:
                schema = {c: "string" for c in columns}

    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]
    target_cols, _ = resolve_target_columns(mappings, column_types, preserve_case=True)

    total_rows = probe.total_rows
    if total_rows is not None and limit > 0:
        total_rows = min(total_rows, limit)

    checkpoint_service = checkpoint_service or CheckpointService()
    checkpoint = checkpoint or Checkpoint(job_id=job_id or "")
    checkpoint.source_type = src_type
    checkpoint.dest_type = dest_type
    checkpoint.write_mode = write_mode
    checkpoint.conflict_columns = pk_target_cols or []

    # Account for resume state when calculating progress so the UI doesn't see
    # a chunk index greater than the total chunk estimate.
    resume_offset = checkpoint.offset or 0
    chunk_idx = checkpoint.chunk_index or 0
    if chunk_idx == 0 and resume_offset == 0:
        rows_accounted_for = len(probe.rows)
        completed_chunks = 0
    else:
        rows_accounted_for = resume_offset
        completed_chunks = chunk_idx
    remaining_rows = max(0, (total_rows or 0) - rows_accounted_for)
    remaining_chunks = (remaining_rows + chunk_size - 1) // chunk_size if remaining_rows else 0
    if not total_rows and ddb_cursor:
        remaining_chunks = max(remaining_chunks, 1)
    chunks = max(1, completed_chunks + (1 if chunk_idx == 0 and resume_offset == 0 else 0) + remaining_chunks)
    dest_table = resolve_dest_table(dest_type, destination, table)

    ddl_log: list[str] = [
        f"STREAM {src_type}.{table} → {dest_type}.{dest_table} "
        f"({total_rows:,} rows est., {chunks} batches, sync={effective_sync})"
    ]
    if incremental and watermark:
        ddl_log.append(f"INCREMENTAL cursor {cursor_source_col} > {watermark}")
    for col in columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")

    written = checkpoint.rows_processed or 0
    offset = checkpoint.offset or 0
    dest_summary: dict[str, Any] = {}
    last_checksum = ""
    rejected_total = 0
    coerced_null_total = 0
    # Strict/maximum FAIL-FAST on coercion errors; balanced quarantines them.
    # Threaded to every writer so the streaming path matches the buffered path.
    stream_error_policy = transform_error_policy_for_validation_mode(validation_mode)
    warning_samples: list[str] = []
    ddb_total = probe.total_rows if src_type == "dynamodb" else None
    running_cursor = checkpoint.cursor_value if checkpoint.cursor_value is not None else watermark
    es_search_after = checkpoint.es_search_after or (ddb_cursor if src_type == "elasticsearch" else None)
    redis_scan_state = checkpoint.redis_scan_state or (ddb_cursor if src_type == "redis" else None)
    keyset_col = checkpoint.cursor_column or (columns[0] if columns and not incremental else "")
    keyset_after = checkpoint.cursor_value
    use_keyset = bool(keyset_col) and src_type in ("postgresql", "redshift", "mysql", "snowflake", "mongodb")
    ddb_cursor = checkpoint.dynamodb_cursor

    retry = retry_budget or RetryBudget()

    # Separate fetch cursors from committed checkpoint cursors so we can read
    # ahead in parallel while only persisting durable offsets after a batch is
    # successfully written.
    fetch_cursor = running_cursor
    fetch_offset = offset
    committed_offset = offset

    def _fetch_next_batch(last_batch):
        nonlocal ddb_cursor, es_search_after, redis_scan_state, keyset_after
        if limit > 0 and fetch_offset >= limit:
            return None
        if total_rows is not None and fetch_offset >= total_rows:
            return None
        batch_limit = _batch_limit(fetch_offset)
        if last_batch is not None and len(last_batch.rows) < chunk_size:
            if (
                src_type in ("postgresql", "redshift", "mysql", "snowflake", "mongodb")
                and (incremental or use_keyset)
            ) or src_type in ("elasticsearch", "redis"):
                return None
        if src_type == "dynamodb":
            if not ddb_cursor:
                return None
            batch, ddb_cursor = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    dynamodb_cursor=ddb_cursor,
                    dynamodb_total=ddb_total,
                )
            )
            return batch
        elif incremental and cursor_source_col and src_type in ("postgresql", "redshift", "mysql", "snowflake", "mongodb"):
            batch, _ = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    cursor_column=cursor_source_col,
                    cursor_after=fetch_cursor,
                    cursor_type=column_types.get(cursor_source_col, "VARCHAR"),
                    known_total_rows=total_rows,
                )
            )
            return batch
        elif use_keyset:
            if keyset_col in columns and last_batch is not None and last_batch.rows:
                keyset_after = last_batch.rows[-1][last_batch.headers.index(keyset_col)]
            batch, extra = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    cursor_column=keyset_col,
                    cursor_after=keyset_after,
                    cursor_type=column_types.get(keyset_col, "VARCHAR"),
                    known_total_rows=total_rows,
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra
            return batch
        elif src_type == "elasticsearch":
            batch, es_search_after = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    es_search_after=es_search_after,
                )
            )
            return batch
        elif src_type == "redis":
            batch, redis_scan_state = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                    redis_scan_state=redis_scan_state,
                )
            )
            return batch
        elif fetch_offset >= total_rows:
            return None
        else:
            batch, extra = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    fetch_offset,
                    batch_limit,
                    database=src_db,
                    known_total_rows=total_rows,
                )
            )
            if src_type == "elasticsearch":
                es_search_after = extra
            elif src_type == "redis":
                redis_scan_state = extra
            return batch

    def _filter_batch(batch):
        if source_filter and batch and batch.rows:
            batch.rows = apply_row_filter_to_matrix(batch.headers, batch.rows, source_filter)
        return batch

    batch = _filter_batch(_fetch_next_batch(None) if (offset > 0 or chunk_idx > 0) else probe)
    batch_quality_enabled = validation_mode in ("strict", "maximum")
    # Identity key for duplicate audit — NEVER reuse the CDC cursor column.
    # Cursor fields (updated_at, id) are not the uniqueness contract; Mongo→Redis
    # must audit `_id`, not a business `id` that legitimately repeats.
    pk_source_col = ""
    if pk_target_cols and mappings:
        target_to_source = {m.get("target", "").lower(): m.get("source", "") for m in mappings if m.get("target")}
        for pk in pk_target_cols:
            src = target_to_source.get(pk.lower())
            if src:
                pk_source_col = src
                break
    if not pk_source_col:
        try:
            from services.primary_key import resolve_primary_key_source

            pk_source_col = (
                resolve_primary_key_source(
                    mappings,
                    list(columns or (batch.headers if batch else [])),
                    dest_type,
                    validation_mode=validation_mode,
                    purpose="uniqueness",
                )
                or ""
            )
        except Exception:
            pk_source_col = ""
    # Prefer live batch headers when available for the first chunk.
    if batch and getattr(batch, "headers", None) and pk_source_col:
        lower = {h.lower(): h for h in batch.headers}
        pk_source_col = lower.get(pk_source_col.lower(), pk_source_col)

    max_workers = int(os.getenv("DATAFLOW_PARALLEL_WORKERS", str(min(2, os.cpu_count() or 1))))
    # SQLite handles concurrency poorly with a single shared file, so keep it sequential.
    # Snowflake reuses one connection for the job — must stay serial.
    # Public TCP proxies drop under concurrent bulk writers — force one connection.
    if dest_type in ("sqlite", "snowflake"):
        max_workers = 1
    else:
        try:
            from connectors.write_resilience import is_public_proxy_host
        except ImportError:
            is_public_proxy_host = lambda _h: False  # noqa: E731
        proxy_host = str(dest_cfg.get("host") or "")
        proxy_cs = str(
            dest_cfg.get("connection_string")
            or dest_cfg.get("uri")
            or dest_cfg.get("url")
            or ""
        )
        if is_public_proxy_host(proxy_host) or is_public_proxy_host(proxy_cs):
            max_workers = 1

    # Shared Snowflake connection for the life of this stream job (open once).
    sf_conn_state: dict[str, Any] = {"conn": None, "session_ready": False}
    batches_completed = 0
    load_methods_seen: list[str] = []

    def _ensure_snowflake_conn() -> Any:
        if sf_conn_state["conn"] is not None:
            return sf_conn_state["conn"]
        from connectors.snowflake_conn import get_connection, normalize_account

        sf_conn_state["conn"] = get_connection(
            account=normalize_account(dest_cfg.get("host", "")),
            username=dest_cfg.get("username", ""),
            password=dest_cfg.get("password", ""),
            database=dest_cfg.get("database", ""),
            schema=dest_cfg.get("schema", "PUBLIC"),
            warehouse=dest_cfg.get("warehouse", ""),
            connection_string=dest_cfg.get("connection_string", ""),
            role=dest_cfg.get("role", ""),
        )
        return sf_conn_state["conn"]

    def _process_db_chunk(idx: int, batch: Any) -> dict[str, Any]:
        if not batch or not getattr(batch, "rows", None):
            return {
                "batch_written": 0,
                "last_checksum": "",
                "dest_summary": {},
                "rejected": 0,
                "coerced_null": 0,
                "warnings": [],
                "batch_max": None,
                "batch_rows": 0,
            }
        local_warnings: list[str] = []
        # Per-batch data-quality / anomaly gate for database streams.
        if batch_quality_enabled:
            audit = run_integrity_audit(
                headers=batch.headers,
                rows=batch.rows,
                column_types=column_types,
                mappings=mappings,
                required_targets=pk_target_cols or [],
                primary_key=pk_source_col if pk_source_col in batch.headers else None,
                validation_mode=validation_mode,
                dest_kind=dest_type,
            )
            if audit.issues:
                local_warnings.extend(audit.issues[:10])
            if audit.warnings:
                local_warnings.extend(audit.warnings[:10])
            if not audit.passed:
                raise ValueError(f"Batch {idx} failed data-quality audit: {'; '.join(audit.issues[:5])}")

        batch_max = None
        if incremental and cursor_source_col:
            batch_max = max_cursor_value(batch.rows, batch.headers, cursor_source_col)

        write_kwargs: dict[str, Any] = {}
        if dest_type == "snowflake":
            write_kwargs["connection"] = _ensure_snowflake_conn()
            write_kwargs["close_connection"] = False
            write_kwargs["skip_session_setup"] = bool(sf_conn_state["session_ready"])

        write_op = partial(
            _write_batch,
            dest_type,
            destination,
            dest_cfg,
            dest_table,
            batch.headers,
            batch.rows,
            mappings,
            column_types,
            create_table=(idx == chunk_idx + 1),
            on_checkpoint=None,
            chunk_idx=idx,
            total_chunks=chunks,
            rows_so_far=0,
            write_mode=write_mode,
            conflict_columns=pk_target_cols or None,
            backfill_new_fields=backfill_new_fields,
            error_policy=stream_error_policy,
            job_id=job_id,
            **write_kwargs,
        )
        batch_written, last_checksum, dest_summary = with_retry(
            write_op,
            budget=RetryBudget(
                max_attempts=retry.max_attempts,
                base_delay_seconds=retry.base_delay_seconds,
                max_delay_seconds=retry.max_delay_seconds,
                exponential_base=retry.exponential_base,
                jitter=retry.jitter,
            ),
        )
        if dest_type == "snowflake":
            sf_conn_state["session_ready"] = True
        return {
            "batch_written": batch_written,
            "last_checksum": last_checksum,
            "dest_summary": dest_summary,
            "rejected": int(dest_summary.get("rejected_rows", 0) or 0),
            "coerced_null": int(dest_summary.get("coerced_null_rows", 0) or 0),
            "warnings": (dest_summary.get("warnings") or [])[:10] + local_warnings,
            "batch_max": batch_max,
            "batch_rows": len(batch.rows),
        }

    def _apply_result(idx: int, result: dict[str, Any]) -> None:
        nonlocal written, rejected_total, coerced_null_total, last_checksum, running_cursor, committed_offset, dest_summary, batches_completed
        written += result["batch_written"]
        rejected_total += result["rejected"]
        coerced_null_total += result.get("coerced_null", 0)
        last_checksum = result["last_checksum"] or last_checksum
        warning_samples.extend(result["warnings"])
        if result["batch_max"] is not None:
            if running_cursor is None or compare_cursor_values(result["batch_max"], running_cursor) > 0:
                running_cursor = result["batch_max"]
        # Absolute source-row offset for this batch (0-based start before commit).
        batch_start = int(committed_offset or 0)
        committed_offset += result["batch_rows"]
        if result["dest_summary"]:
            incoming = dict(result["dest_summary"])
            method = incoming.get("load_method")
            if method:
                load_methods_seen.append(str(method))
            # Merge quarantine findings across batches — never replace with last batch only.
            prev = dest_summary if isinstance(dest_summary, dict) else {}
            prev_details = list(prev.get("rejected_details") or [])
            new_details: list[dict[str, Any]] = []
            for raw in incoming.get("rejected_details") or []:
                if not isinstance(raw, dict):
                    continue
                detail = dict(raw)
                local_row = int(detail.get("row") or 0)
                if local_row > 0:
                    detail["batch_row"] = local_row
                    detail["batch_offset"] = batch_start
                    detail["row"] = batch_start + local_row  # 1-based absolute across the transfer
                new_details.append(detail)
            merged_details = (prev_details + new_details)[:2000]
            dest_summary = {
                **prev,
                **incoming,
                "rejected_details": merged_details,
                "rejected_rows": rejected_total,
                "coerced_null_rows": coerced_null_total,
            }
            batches_completed += 1

        # Persist durable checkpoint after the batch is committed.  We use the
        # ordered result stream so checkpoint offsets/cursors can never skip
        # ahead of an in-flight batch.
        checkpoint.chunk_index = idx
        checkpoint.offset = committed_offset
        checkpoint.rows_processed = written
        checkpoint.cursor_value = running_cursor or keyset_after
        checkpoint.cursor_column = cursor_source_col if incremental else keyset_col
        checkpoint.es_search_after = es_search_after
        checkpoint.redis_scan_state = redis_scan_state
        checkpoint.dynamodb_cursor = ddb_cursor
        checkpoint.checksum = last_checksum
        checkpoint.phase = "writing"
        checkpoint.chunk_total = chunks
        checkpoint.status = "running"
        checkpoint_service.save(checkpoint)
        if on_checkpoint:
            on_checkpoint(idx, chunks, written, checkpoint.to_dict())

    first_idx = chunk_idx + 1

    def _prepare_and_submit(dispatcher: ChunkDispatcher, idx: int, batch: Any) -> None:
        nonlocal fetch_cursor, fetch_offset
        batch = _filter_batch(batch)
        if incremental and cursor_source_col and batch.rows:
            batch_max = max_cursor_value(batch.rows, batch.headers, cursor_source_col)
            if batch_max and (fetch_cursor is None or compare_cursor_values(batch_max, fetch_cursor) > 0):
                fetch_cursor = batch_max
        dispatcher.submit(idx, batch, _process_db_chunk)
        fetch_offset += len(batch.rows)

    try:
        # Process the first batch synchronously so DDL (table/index creation) is
        # committed before any parallel workers try to insert into the new table.
        if batch:
            batch = _filter_batch(batch)
            if incremental and cursor_source_col and batch.rows:
                batch_max = max_cursor_value(batch.rows, batch.headers, cursor_source_col)
                if batch_max and (fetch_cursor is None or compare_cursor_values(batch_max, fetch_cursor) > 0):
                    fetch_cursor = batch_max
            _apply_result(first_idx, _process_db_chunk(first_idx, batch))
            fetch_offset += len(batch.rows)

        idx = first_idx + 1
        with ChunkDispatcher(max_workers=max_workers) as dispatcher:
            # Fetch the next batch (the first one has already been committed).
            batch = _fetch_next_batch(batch)
            while batch:
                _prepare_and_submit(dispatcher, idx, batch)

                # Process any completed batches in ascending index order.
                for ready_idx, result in dispatcher.ready():
                    _apply_result(ready_idx, result)

                # Fetch the next batch while earlier batches are still being written.
                batch = _fetch_next_batch(batch)
                idx += 1

            # Drain the remaining in-flight writes.
            for ready_idx, result in dispatcher.results():
                _apply_result(ready_idx, result)
    finally:
        conn = sf_conn_state.get("conn")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            sf_conn_state["conn"] = None

    if written == 0 and incremental:
        ddl_log.append("INCREMENTAL — no new rows since last watermark")
        dest_summary["sync_mode"] = effective_sync
        dest_summary["watermark"] = watermark
        return 0, ddl_log, dest_summary, columns

    if written == 0:
        details = "; ".join(filter(None, warning_samples[:10]))
        if details:
            raise ValueError(f"No rows were written to the destination: {details}")
        raise ValueError("Source table is empty")

    if incremental and running_cursor and cursor_key and running_cursor != watermark:
        set_watermark(cursor_key, running_cursor, metadata={"job_id": job_id, "sync_mode": effective_sync})

    # Re-read the source and compute a checksum over the complete logical source.
    # This avoids using the per-batch writer checksum for chunked/resumable runs,
    # which only represented the last batch.
    if src_type in {"postgresql", "redshift", "mysql", "snowflake", "bigquery", "sqlite", "generic_sql", "mongodb", "s3", "gcs", "adls"}:
        # Re-read the source in chunks and feed mapped fingerprints into the
        # streaming accumulator.  This keeps database→database strict-mode
        # reconciliation memory-bounded by a single batch, even for billion-row
        # tables, and avoids the previous `source_rows_for_checksum` list that
        # materialized the full source a second time.
        fp_accumulator = FingerprintAccumulator()
        cursor_type_for_read: str | None = None
        if incremental and cursor_source_col:
            cursor_type_for_read = normalize_inferred(schema.get(cursor_source_col, "string")).upper()
        # For keyset-capable NoSQL sources, use the same cursor column from the
        # main write loop so the checksum re-read does not pay an O(n²) skip
        # penalty on large collections (e.g. MongoDB with random-hash _ids).
        checksum_cursor_col = cursor_source_col if incremental else ""
        checksum_cursor_after = watermark if incremental else None
        use_checksum_keyset = False
        if not incremental and keyset_col and keyset_col in (columns or []):
            if src_type in {"mongodb"}:
                checksum_cursor_col = keyset_col
                checksum_cursor_after = None
                use_checksum_keyset = True
                cursor_type_for_read = normalize_inferred(column_types.get(keyset_col, "string")).upper()
        read_offset = 0
        checksum_rows_read = 0
        while True:
            read_limit = _batch_limit(read_offset, default=chunk_size)
            if read_limit <= 0:
                break
            batch, _ = _unwrap_read(
                _read_batch(
                    src_type,
                    src_cfg,
                    table,
                    columns,
                    read_offset,
                    read_limit,
                    database=src_db,
                    cursor_column=checksum_cursor_col,
                    cursor_after=checksum_cursor_after,
                    cursor_type=cursor_type_for_read,
                    known_total_rows=total_rows,
                )
            )
            if not batch or not batch.rows:
                break
            mapped, _ = build_mapped_rows(
                headers=batch.headers,
                data_rows=batch.rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                # Use the SAME policy as the write path so strict/maximum cannot
                # silently coerce a bad cell to NULL here and mask a mismatch.
                error_policy=stream_error_policy,
                preserve_case=True,
            )
            if mapped:
                fp_accumulator.add_many(row_fingerprints(mapped, target_cols))
            checksum_rows_read += len(batch.rows)
            if limit > 0 and checksum_rows_read >= limit:
                break
            if len(batch.rows) < read_limit:
                break
            if use_checksum_keyset:
                checksum_cursor_after = batch.rows[-1][batch.headers.index(checksum_cursor_col)]
            else:
                read_offset += len(batch.rows)
        final_checksum = fp_accumulator.digest() if fp_accumulator.total else last_checksum
    else:
        final_checksum = last_checksum

    dest_summary["checksum"] = final_checksum or last_checksum
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["coerced_null_rows"] = coerced_null_total
    dest_summary["rejected_details"] = list(dest_summary.get("rejected_details") or [])[:2000]
    dest_summary["warnings"] = warning_samples[:10]
    dest_summary["error_policy"] = "quarantine" if (rejected_total or coerced_null_total) else "none"
    dest_summary["sync_mode"] = effective_sync
    dest_summary["watermark"] = running_cursor
    dest_summary["chunk_size"] = chunk_size
    dest_summary["batches"] = batches_completed
    if load_methods_seen:
        if "copy_into" in load_methods_seen:
            dest_summary["load_method"] = "copy_into"
        elif "merge_batch" in load_methods_seen:
            dest_summary["load_method"] = "merge_batch"
        else:
            dest_summary["load_method"] = load_methods_seen[-1]
    ddl_log.insert(1, f"CREATE TABLE IF NOT EXISTS {dest_table}")
    return written, ddl_log, dest_summary, columns


def _drop_destination_endpoint(destination: EndpointConfig) -> bool:
    """Drop the remapped destination object (overwrite sync, multi-stream)."""
    if destination.kind != "database":
        return False
    try:
        from connectors.table_manager import drop_table

        db_type = resolve_driver_type(destination.format)
        cfg = resolve_connector_config(destination)
        table_name = resolve_dest_table(db_type, destination)
        schema = cfg.get("schema")
        return drop_table(db_type, cfg, table_name, schema)
    except Exception:
        return False


def run_non_cdc_multi_stream_sequential(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_append",
    stream_contracts: list[dict] | None = None,
    selected: list[Any] | None = None,
    job_id: str | None = None,
    checkpoint: Checkpoint | None = None,
    checkpoint_service: CheckpointService | None = None,
    retry_budget: RetryBudget | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    limit: int = 0,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Run full/incremental for N streams sequentially (one object at a time).

    Mirrors CDC ``_run_cdc_multi_stream_sequential``: remap source/dest per stream,
    prefer per-stream mappings, aggregate ``streams[]`` health. Overwrite DROP is
    per remapped destination (not once on the primary). Delivery remains
    **at-least-once** on resume (shared job checkpoint).
    """
    from services.sync_cursor import (
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
        should_drop_destination_for_sync,
    )

    selected_list = list(selected or resolve_selected_sync_contracts(stream_contracts))
    if len(selected_list) < 2:
        return stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            on_checkpoint,
            sync_mode=sync_mode,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
            limit=limit,
        )

    total_rows = 0
    ddl_log: list[str] = [
        f"MULTI-STREAM sequential ({len(selected_list)} streams, sync={sync_mode}; "
        "each stream has its own watermark; at-least-once)"
    ]
    headers: list[str] = list(schema.keys()) if schema else []
    stream_health: list[dict[str, Any]] = []
    last_summary: dict[str, Any] = {}
    remaining_limit = int(limit or 0)

    original_table = getattr(source, "table", None)
    original_collection = getattr(source, "collection", None)
    original_dest_table = getattr(destination, "table", None)
    original_dest_collection = getattr(destination, "collection", None)

    try:
        for contract in selected_list:
            if remaining_limit == 0 and limit > 0:
                break
            stream_name = (contract.name or "").strip() or "stream"
            if getattr(source, "format", "") == "mongodb" or original_collection:
                source.collection = stream_name
            else:
                source.table = stream_name
            if original_dest_table is not None or original_dest_collection is not None:
                if getattr(destination, "format", "") == "mongodb" or original_dest_collection:
                    destination.collection = stream_name
                else:
                    destination.table = stream_name

            raw = next(
                (c for c in (stream_contracts or []) if c.get("name") == stream_name),
                {},
            ) or {}
            single_contracts = [
                {
                    **raw,
                    "name": stream_name,
                    "selected": True,
                    "sync_mode": contract.sync_mode or sync_mode,
                    "cursor_field": contract.cursor_field or raw.get("cursor_field") or "",
                    "primary_key": contract.primary_key or raw.get("primary_key") or "",
                    "schema_policy": contract.schema_policy or raw.get("schema_policy"),
                    "validation_mode": contract.validation_mode or validation_mode,
                }
            ]
            stream_maps = single_contracts[0].get("mappings")
            use_mappings = (
                stream_maps if isinstance(stream_maps, list) and stream_maps else mappings
            )

            # Per-stream overwrite: drop remapped dest (outer engine skip when N>1).
            if should_drop_destination_for_sync(
                request_sync_mode=sync_mode,
                contract_sync_mode=single_contracts[0].get("sync_mode"),
            ):
                _drop_destination_endpoint(destination)

            status = "completed"
            error: str | None = None
            rows = 0
            summary: dict[str, Any] = {}
            stream_limit = remaining_limit if limit > 0 else 0
            try:
                # Empty schema → re-introspect each remapped source table.
                rows, stream_ddl, summary, headers = stream_database_transfer(
                    source,
                    destination,
                    use_mappings,
                    {},
                    on_checkpoint,
                    sync_mode=sync_mode,
                    stream_contracts=single_contracts,
                    job_id=job_id,
                    checkpoint=checkpoint,
                    checkpoint_service=checkpoint_service,
                    retry_budget=retry_budget,
                    backfill_new_fields=backfill_new_fields,
                    validation_mode=validation_mode,
                    source_filter=source_filter,
                    limit=stream_limit,
                )
                ddl_log.extend(stream_ddl)
                total_rows += rows
                last_summary = summary
                if limit > 0:
                    remaining_limit = max(0, remaining_limit - rows)
            except Exception as exc:
                status = "failed"
                error = str(exc)
                stream_health.append(
                    {
                        "name": stream_name,
                        "status": status,
                        "records_processed": rows,
                        "error": error,
                    }
                )
                raise
            stream_health.append(
                {
                    "name": stream_name,
                    "status": status,
                    "records_processed": rows,
                    "watermark": summary.get("watermark"),
                    "sync_mode": summary.get("sync_mode")
                    or resolve_effective_sync_mode(
                        sync_mode, single_contracts[0].get("sync_mode")
                    ),
                    "error": error,
                }
            )
    finally:
        if original_table is not None:
            source.table = original_table
        if original_collection is not None:
            source.collection = original_collection
        if original_dest_table is not None:
            destination.table = original_dest_table
        if original_dest_collection is not None:
            destination.collection = original_dest_collection

    last_summary = dict(last_summary or {})
    last_summary["streams"] = stream_health
    last_summary["multi_stream"] = True
    last_summary["multi_stream_mode"] = "sequential"
    return total_rows, ddl_log, last_summary, headers


class _NoOpCheckpointService:
    """A checkpoint service that does not persist anything.

    Used for temporary staging transfers so the parent job's checkpoint is not
    overwritten by an internal phase.
    """

    def save(self, checkpoint: Any) -> bool:  # noqa: ARG002
        return True

    def load(self, job_id: str) -> Any | None:  # noqa: ARG002
        return None


def _qualified(table: str, schema: str | None) -> str:
    """Return a SQL quoted qualified table name."""
    from connectors.writer_common import quote_sql_identifier

    table_q = quote_sql_identifier(table)
    if schema:
        return f"{quote_sql_identifier(schema)}.{table_q}"
    return table_q


def _staging_endpoint(destination: EndpointConfig, job_id: str) -> EndpointConfig:
    """Clone a destination endpoint for use as a per-transfer staging table."""
    from dataclasses import replace

    suffix = re.sub(r"[^a-zA-Z0-9_]", "", job_id)[:16] or "stg"
    return replace(
        destination,
        table=f"_dataflow_stg_{suffix}",
        collection=f"_dataflow_stg_{suffix}",
    )


def stream_scd2_mirror_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[..., None] | None = None,
    *,
    sync_mode: str = "full_refresh_mirror",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Any = None,  # noqa: ARG001 - reserved for future resume support
    checkpoint_service: Any = None,  # noqa: ARG001
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    limit: int = 0,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Stream a mirror or SCD2 database-to-database transfer through a staging table.

    Instead of loading the entire source table into memory, this helper:
      1. Streams the source into a temporary staging table.
      2. For SCD2, applies the slowly-changing-dimension merge in batches.
      3. For mirror, upserts the staging table into the target and then runs a
         single SQL pass to reactivate present keys and soft-delete missing keys.
    """
    import math

    import sqlalchemy as sa

    from connectors.generic_sql import drop_table, get_sql_schema, get_sqlalchemy_engine
    from connectors.writer_common import quote_sql_identifier
    from services.sync_cursor import (
        map_source_to_target,
        resolve_effective_sync_mode,
        resolve_sync_contract,
    )

    contract = resolve_sync_contract(stream_contracts)
    effective_sync = resolve_effective_sync_mode(
        sync_mode,
        contract.sync_mode if contract else None,
    ).lower()

    src_type = resolve_driver_type(source.format)
    dest_type = resolve_driver_type(destination.format)

    # Supported SQL-backed destinations that can be driven through SQLAlchemy.
    _SQL_STREAMING_DESTS = {
        "generic_sql", "postgresql", "mysql", "sqlite", "snowflake", "bigquery", "redshift",
    }
    if dest_type not in _SQL_STREAMING_DESTS:
        raise NotImplementedError(
            f"{effective_sync} streaming transfer is currently implemented for SQL destinations; "
            f"'{destination.format}' is not yet supported."
        )

    dest_cfg = resolve_connector_config(destination)
    # get_sql_schema() already respects dialect defaults; ignore the default
    # "public" placeholder that resolve_connector_config sets for SQLite/MySQL.
    schema_name = get_sql_schema(dest_cfg) or ""

    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in schema]
    target_cols, _ = resolve_target_columns(mappings, schema, preserve_case=True)
    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in schema}

    staging = _staging_endpoint(destination, job_id or "")
    staging_qualified = _qualified(staging.table, schema_name)
    target_qualified = _qualified(destination.table or staging.table, schema_name)

    # 1. Drop any leftover staging table and stream source into staging.
    drop_table(dest_cfg, staging.table, schema_name or None)

    stage_cb: Callable[..., None] | None = None
    if on_checkpoint:
        def stage_cb(chunk: int, chunks: int, rows: int, checkpoint: dict | None = None) -> None:  # type: ignore[misc]
            pct = int(25 + (chunk / max(chunks, 1)) * 35)
            on_checkpoint(chunk, chunks, rows, checkpoint=checkpoint or {"phase": "staging", "progress_pct": pct})

    rows_staged, stage_ddl, stage_summary, stage_columns = stream_database_transfer(
        source,
        staging,
        mappings,
        schema,
        on_checkpoint=stage_cb,
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"selected": True, "sync_mode": "full_refresh_overwrite"}],
        job_id=f"{job_id or ''}_stage",
        checkpoint_service=_NoOpCheckpointService(),
        backfill_new_fields=backfill_new_fields,
        validation_mode=validation_mode,
        limit=limit,
    )

    ddl_log = [
        f"STAGING {src_type}.{source.table or source.collection} → {staging_qualified} "
        f"({rows_staged:,} rows)",
    ]

    rows_written = 0
    dest_summary: dict[str, Any] = {
        "source_rows": rows_staged,
        "staging_table": staging_qualified,
        "sync_mode": effective_sync,
    }

    try:
        if effective_sync == "scd2":
            from src.services.scd2_engine import apply_scd2

            batch_size = 1_000
            if contract and contract.primary_key:
                conflict_columns = [
                    map_source_to_target(col, mappings) for col in contract.primary_key_columns()
                ]
            else:
                conflict_columns = [target_cols[0]]
            written_total = 0
            updated_total = 0
            active_rows = 0
            active_checksum = ""
            batch_idx = 0
            # Rough batch count for progress reporting; exact number does not matter.
            approx_batches = max(1, math.ceil(rows_staged / batch_size))

            for records in _read_staging_batches(staging, dest_cfg, schema_name, target_cols, batch_size):
                if not records:
                    break
                summary = apply_scd2(
                    destination,
                    records,
                    target_cols,
                    column_types,
                    mappings=mappings,
                    conflict_columns=conflict_columns,
                    batch_size=batch_size,
                )
                written_total += int(summary.get("rows_written", 0))
                updated_total += int(summary.get("updated_rows", 0))
                active_rows = int(summary.get("active_rows", 0))
                active_checksum = str(summary.get("active_checksum", ""))
                batch_idx += 1
                if on_checkpoint:
                    on_checkpoint(batch_idx, approx_batches, written_total, checkpoint={"phase": "scd2"})

            rows_written = written_total
            dest_summary["active_rows"] = active_rows
            dest_summary["active_checksum"] = active_checksum
            dest_summary["updated_rows"] = updated_total

        elif effective_sync in ("full_refresh_mirror", "mirror"):
            from src.services.mirror_engine import (
                _compute_active_checksum,
                _ensure_soft_delete_column,
            )

            # Stream upsert staging → target.
            upsert_contract = [{"selected": True, "sync_mode": "upsert", "primary_key": contract.primary_key}] if contract else None
            rows_upserted, _, upsert_summary, _ = stream_database_transfer(
                staging,
                destination,
                mappings,
                schema,
                on_checkpoint=on_checkpoint,
                sync_mode="upsert",
                stream_contracts=upsert_contract,
                job_id=f"{job_id or ''}_mirror",
                checkpoint_service=_NoOpCheckpointService(),
                backfill_new_fields=backfill_new_fields,
                validation_mode=validation_mode,
            )

            rows_written = rows_upserted
            dest_summary["upserted"] = rows_upserted
            dest_summary["checksum"] = upsert_summary.get("checksum", "")

            # Single SQL pass to reactivate present keys and soft-delete missing keys.
            engine = get_sqlalchemy_engine(dest_cfg)
            soft_delete_col = quote_sql_identifier("_deleted")
            if contract and contract.primary_key:
                pk_cols = [
                    map_source_to_target(col, mappings) for col in contract.primary_key_columns()
                ]
            else:
                pk_cols = [target_cols[0]]
            # Correlate target↔staging without UPDATE aliases (SQLite-compatible).
            join_pred = " AND ".join(
                f"{staging_qualified}.{quote_sql_identifier(c)} = "
                f"{target_qualified}.{quote_sql_identifier(c)}"
                for c in pk_cols
            )
            with engine.connect() as conn:
                _ensure_soft_delete_column(conn, target_qualified, "_deleted")
                # Reactivate rows that are back in the source.
                conn.execute(
                    sa.text(
                        f"UPDATE {target_qualified} "
                        f"SET {soft_delete_col} = FALSE "
                        f"WHERE EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred})"
                    )
                )
                # Soft-delete rows that are no longer in the source.
                conn.execute(
                    sa.text(
                        f"UPDATE {target_qualified} "
                        f"SET {soft_delete_col} = TRUE "
                        f"WHERE NOT EXISTS (SELECT 1 FROM {staging_qualified} WHERE {join_pred}) "
                        f"AND ({soft_delete_col} IS NULL OR {soft_delete_col} = FALSE)"
                    )
                )
                conn.commit()

                # Read active rows and compute checksum.
                active_count, active_checksum = _compute_active_checksum(
                    conn, target_qualified, target_cols, "_deleted", batch_size=1_000
                )
                conn.commit()
            engine.dispose()
            dest_summary["active_rows"] = active_count
            dest_summary["active_checksum"] = active_checksum

        else:
            raise ValueError(f"Unsupported sync mode for SCD2/mirror streaming: {effective_sync}")
    finally:
        # Clean up the temporary staging table.
        try:
            drop_table(dest_cfg, staging.table, schema_name or None)
        except Exception:
            pass

    ddl_log.append(f"{effective_sync.upper()} {staging_qualified} → {target_qualified}")
    dest_summary["rejected_rows"] = max(0, rows_staged - rows_written)
    return rows_written, ddl_log, dest_summary, target_cols


def _read_staging_batches(
    endpoint: EndpointConfig,
    cfg: dict[str, Any],
    schema_name: str,
    columns: list[str],
    batch_size: int,
):
    """Yield batches of dicts from the staging table using LIMIT/OFFSET."""
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.writer_common import quote_sql_identifier

    engine = get_sqlalchemy_engine(cfg)
    qualified = _qualified(endpoint.table, schema_name)
    try:
        with engine.connect() as conn:
            offset = 0
            while True:
                cols = ",".join(quote_sql_identifier(c) for c in columns)
                sql = f"SELECT {cols} FROM {qualified} LIMIT {batch_size} OFFSET {offset}"
                result = conn.execute(sa.text(sql))
                rows = result.mappings().all()
                if not rows:
                    break
                yield [{c: row[c] for c in columns} for row in rows]
                if len(rows) < batch_size:
                    break
                offset += batch_size
    finally:
        engine.dispose()


def peek_stream_source(source: EndpointConfig) -> tuple[list[str], dict[str, str], int, list[dict]]:
    """Return columns, schema, row count, and sample rows for preflight."""
    from .connector_capabilities import resolve_driver_type
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
        schema = _introspect_table_schema(src_type, src_cfg, table, columns)
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
