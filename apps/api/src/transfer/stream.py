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

from .adapters import _introspect_table_schema, resolve_connector_config


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    return {
        "rejected_rows": rejected,
        "warnings": list(getattr(result, "warnings", []) or [])[:10],
        "error_policy": "quarantine" if rejected else "none",
    }


def _source_name(source: EndpointConfig) -> str:
    if source.format.lower() == "mongodb":
        return source.collection or source.table or ""
    return source.table or source.collection or ""


def _read_batch(
    src_type: str,
    cfg: dict[str, Any],
    table: str,
    columns: list[str] | None,
    offset: int,
    limit: int,
    database: str = "",
):
    if src_type == "postgresql":
        from connectors.postgresql_reader import read_table_batch

        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 5432),
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
        )
    if src_type == "mysql":
        from connectors.mysql_reader import read_table_batch

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
        )
    if src_type == "mongodb":
        from connectors.mongodb_reader import read_collection_batch

        return read_collection_batch(
            cfg=cfg,
            database=database or cfg.get("database", "test"),
            collection=table,
            columns=columns,
            offset=offset,
            limit=limit,
        )
    raise ValueError(f"Streaming read not supported for source type '{src_type}'")


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
) -> tuple[int, str, dict]:
    if dest_type == "postgresql":
        from connectors.postgresql_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 5432),
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
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
        )
        if not result.ok:
            raise RuntimeError(result.error or "PostgreSQL batch write failed")
        summary = {
            "type": "postgresql",
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
        from pymongo import MongoClient

        from ..services.mongodb_service import get_mongodb_service

        records = [dict(zip(headers, row)) for row in data_rows]
        if cfg.get("connection_string"):
            conn_str = cfg["connection_string"]
        elif cfg.get("username") and cfg.get("password"):
            conn_str = (
                f"mongodb://{cfg['username']}:{cfg['password']}"
                f"@{cfg['host']}:{cfg['port'] or 27017}/"
            )
        else:
            conn_str = f"mongodb://{cfg['host']}:{cfg['port'] or 27017}/"
        client = MongoClient(conn_str, serverSelectionTimeoutMS=10000)
        try:
            mongo = get_mongodb_service()
            db_name = dest.database or cfg.get("database") or "test_db"
            coll_name = table_name
            if create_table:
                schema = {c: column_types.get(c, "string") for c in headers}
                mongo.create_collection_from_schema(db_name, coll_name, schema, client=client)
            insert = mongo.insert_data(db_name, coll_name, records, client=client)
            if not insert.get("success"):
                raise RuntimeError(insert.get("error") or "MongoDB batch insert failed")
            if on_checkpoint:
                on_checkpoint(chunk_idx, total_chunks, rows_so_far + insert["inserted_count"])
            summary = {
                "type": "mongodb",
                "database": db_name,
                "collection": coll_name,
                "checksum": str(insert["inserted_count"]),
            }
            return insert["inserted_count"], summary["checksum"], summary
        finally:
            client.close()

    raise ValueError(f"Streaming write not supported for destination type '{dest_type}'")


def stream_database_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    mappings: list[dict],
    schema: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """
    Extract source table in CHUNK_SIZE batches and load to destination.
    Returns (rows_written, ddl_log, dest_summary, columns).
    """
    src_type = source.format.lower()
    dest_type = destination.format.lower()
    src_cfg = resolve_connector_config(source)
    dest_cfg = resolve_connector_config(destination)

    if src_type not in ("postgresql", "mysql", "mongodb"):
        raise ValueError(f"Streaming source '{src_type}' not supported")
    if dest_type not in ("postgresql", "mysql", "mongodb"):
        raise ValueError(f"Streaming destination '{dest_type}' not supported — use full load path")

    table = _source_name(source)
    if not table:
        raise ValueError("Source table/collection name required for streaming transfer")

    src_db = source.database or src_cfg.get("database") or ("test" if src_type == "mongodb" else "")

    probe = _read_batch(src_type, src_cfg, table, None, 0, CHUNK_SIZE, database=src_db)
    columns = probe.headers
    if not columns:
        raise ValueError(f"Source table `{table}` has no columns or is empty")

    if not schema:
        if src_type == "mongodb":
            schema = {c: "string" for c in columns}
        else:
            schema = _introspect_table_schema(src_type, src_cfg, table, columns)

    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    total_rows = probe.total_rows
    chunks = max(1, (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
    base = re.sub(r"[^a-zA-Z0-9_]", "_", (destination.table or destination.collection or table).lower())[:40]
    dest_table = destination.table or destination.collection or f"dt_{base}"

    ddl_log: list[str] = [f"STREAM {src_type}.{table} → {dest_type}.{dest_table} ({total_rows:,} rows, {chunks} batches)"]
    for col in columns:
        ddl_log.append(f"{dest_type.upper()} COLUMN {col} {ddl_type(dest_type, schema.get(col, 'string'))}")

    written = 0
    offset = 0
    chunk_idx = 0
    dest_summary: dict[str, Any] = {}
    last_checksum = ""
    rejected_total = 0
    warning_samples: list[str] = []

    while offset < total_rows:
        batch = _read_batch(src_type, src_cfg, table, columns, offset, CHUNK_SIZE, database=src_db)
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
        )
        written += batch_written
        rejected_total += int(dest_summary.get("rejected_rows", 0) or 0)
        warning_samples.extend(dest_summary.get("warnings", []) or [])
        if on_checkpoint:
            on_checkpoint(chunk_idx, chunks, written)
        offset += len(batch.rows)

    if written == 0:
        raise ValueError("Source table is empty")

    dest_summary["checksum"] = last_checksum
    dest_summary["rejected_rows"] = rejected_total
    dest_summary["warnings"] = warning_samples[:10]
    dest_summary["error_policy"] = "quarantine" if rejected_total else "none"
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

    probe = _read_batch(src_type, src_cfg, table, None, 0, CHUNK_SIZE, database=src_db)
    columns = probe.headers
    if not columns and probe.total_rows == 0:
        raise ValueError(f"Source `{table}` has no columns or is empty")

    if src_type == "mongodb":
        schema = {c: "string" for c in columns}
    else:
        schema = _introspect_table_schema(src_type, src_cfg, table, columns)
    sample_rows = [dict(zip(probe.headers, row)) for row in probe.rows[:100]]
    return columns, schema, probe.total_rows, sample_rows


def supports_streaming(source: EndpointConfig, destination: EndpointConfig) -> bool:
    if source.kind != "database" or destination.kind != "database":
        return False
    pair = (source.format.lower(), destination.format.lower())
    return pair in {
        ("postgresql", "postgresql"),
        ("postgresql", "mysql"),
        ("postgresql", "mongodb"),
        ("mysql", "postgresql"),
        ("mysql", "mysql"),
        ("mysql", "mongodb"),
        ("mongodb", "postgresql"),
        ("mongodb", "mysql"),
        ("mongodb", "mongodb"),
    }
