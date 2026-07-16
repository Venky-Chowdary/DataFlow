"""Read/write adapters for universal transfer — files, databases, warehouses."""

from __future__ import annotations
import csv
import io
import json
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

# Legacy connectors live under apps/api/
_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

try:
    from services.file_parser import FileParser
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.file_parser import FileParser

try:
    from services.mongodb_service import get_mongodb_service
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service

try:
    from services.value_serializer import cell_to_string, json_default
except ImportError:  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.value_serializer import cell_to_string, json_default

from .connector_registry import run_probe
from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred


def resolve_dest_table(dest_type: str, destination: EndpointConfig, fallback_name: str = "import") -> str:
    """Resolve destination object name — table, key, index, or collection."""
    base = re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        (destination.table or destination.collection or fallback_name).lower(),
    )[:40]
    dt = dest_type.lower()
    if dt == "dynamodb":
        return destination.table or destination.collection or destination.database or f"dt_{base}"
    if dt in ("s3", "gcs"):
        return destination.table or destination.collection or destination.schema or f"exports/dt_{base}.json"
    if dt == "elasticsearch":
        # Elasticsearch uses an index name; prefer the table name (UI's destination name)
        # and fall back to the database field when it is intentionally supplied.
        return destination.table or destination.collection or destination.database or f"dt_{base}"
    return destination.table or destination.collection or f"dt_{base}"


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    warnings = list(getattr(result, "warnings", []) or [])
    rejected_details = list(getattr(result, "rejected_details", []) or [])
    return {
        "rejected_rows": rejected,
        "rejected_details": rejected_details[:200],
        "warnings": warnings[:10],
        "error_policy": "quarantine" if rejected else "none",
    }


def parse_file_content(content: bytes, filename: str) -> tuple[list[dict], list[str], dict[str, str]]:
    result = FileParser.parse(content, filename)
    if not result.success:
        raise ValueError(result.error or "File parse failed")
    schema = FileParser.infer_schema(result.data)
    return result.data, result.columns, schema


def parse_file_route_sample(
    content: bytes,
    filename: str,
    preview_rows: int = 200,
) -> tuple[list[str], dict[str, str], int]:
    """Headers + schema for route analysis without loading entire files."""
    ftype = FileParser.detect_file_type(filename, content)
    if ftype in ("csv", "tsv"):
        from services.csv_profiler import count_csv_rows, detect_encoding, parse_csv_preview

        enc = detect_encoding(content)
        headers, rows, _enc, _delim = parse_csv_preview(content, encoding=enc, preview_rows=preview_rows)
        if not headers:
            raise ValueError("CSV/TSV has no header row")
        records = [
            {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
            for row in rows[:preview_rows]
        ]
        schema = FileParser.infer_schema(records) if records else {h: "string" for h in headers}
        return headers, schema, count_csv_rows(content, enc)

    result = FileParser.parse(content, filename)
    if not result.success:
        raise ValueError(result.error or "File parse failed")
    sample = result.data[:preview_rows]
    schema = FileParser.infer_schema(sample) if sample else {c: "string" for c in result.columns}
    return result.columns, schema, result.row_count


def _matrix_cell(value: Any) -> str:
    return cell_to_string(value)


def records_to_matrix(records: list[dict], columns: list[str]) -> tuple[list[str], list[list[str]]]:
    headers = columns or (list(records[0].keys()) if records else [])
    rows = [[_matrix_cell(rec.get(h)) for h in headers] for rec in records]
    return headers, rows


def mongodb_connection_string(cfg: dict[str, Any]) -> str:
    from connectors.mongodb_common import normalize_mongodb_connection_string

    return normalize_mongodb_connection_string(
        cfg.get("connection_string", ""),
        database=cfg.get("database", ""),
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 0),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source", ""),
    )


def probe_mongodb(cfg: dict[str, Any]) -> tuple[bool, str]:
    """Ping MongoDB and automatically resolve the correct authSource.

    When auth_source is not supplied, the connection string may still work with
    the default database (path) or admin. We try the candidates in order and
    return the first one that succeeds, which keeps the UI connection-string
    flow simple for users who do not know the authentication database.
    """
    from urllib.parse import parse_qs, urlparse

    from pymongo import MongoClient

    connection_string = (cfg.get("connection_string") or "").strip()
    qs = parse_qs(urlparse(connection_string).query, keep_blank_values=True)
    url_auth_source = qs.get("authSource", qs.get("authsource", [""]))[0]
    database = (cfg.get("database") or "").strip()

    candidates: list[str] = []
    if cfg.get("auth_source"):
        candidates.append(str(cfg.get("auth_source")).strip())
    if url_auth_source:
        candidates.append(url_auth_source)
    if database:
        candidates.append(database)
    candidates.append("admin")

    last_error = ""
    for auth_source in candidates:
        try:
            conn_str = mongodb_connection_string({**cfg, "auth_source": auth_source})
            client = MongoClient(conn_str, serverSelectionTimeoutMS=2500)
            client.admin.command("ping")
            client.close()
            cfg["auth_source"] = auth_source
            return True, f"MongoDB reachable (authSource={auth_source})"
        except Exception as exc:
            last_error = str(exc)
    return False, last_error


def resolve_connector_config(endpoint: EndpointConfig) -> dict[str, Any]:
    """Merge saved connector with inline overrides."""
    from .connector_capabilities import resolve_driver_type
    driver = resolve_driver_type(endpoint.format or "")
    fmt = driver
    default_port = (
        27017 if fmt == "mongodb" else
        3306 if fmt == "mysql" else
        6379 if fmt == "redis" else
        9200 if fmt == "elasticsearch" else
        5439 if fmt == "redshift" else
        0 if fmt == "sqlite" else
        0 if fmt == "generic_sql" else
        22 if fmt == "sftp" else
        587 if fmt == "email" else
        443 if fmt in ("snowflake", "bigquery", "dynamodb", "s3", "gcs", "adls") else 5432
    )
    default_schema = (
        "PUBLIC" if fmt == "snowflake" else
        None if fmt == "generic_sql" else
        "public"
    )
    cfg = {
        "host": endpoint.host or "localhost",
        "port": endpoint.port or default_port,
        "database": endpoint.database if fmt in ("generic_sql", "sftp", "email") else (endpoint.database or endpoint.host or ""),
        "schema": endpoint.schema or default_schema,
        "username": endpoint.username,
        "password": endpoint.password,
        "connection_string": endpoint.connection_string,
        "warehouse": endpoint.warehouse,
        "ssl": endpoint.ssl,
        "type": endpoint.format,
        "auth_mode": endpoint.auth_mode,
        "auth_role": endpoint.auth_role,
        "auth_source": endpoint.auth_source,
        "api_key": endpoint.api_key,
        "service_account": endpoint.service_account,
    }
    # Keep "role" as the canonical key used by Snowflake connector functions.
    cfg["role"] = endpoint.auth_role or cfg.get("role", "")
    cfg.update(endpoint.extra)
    if endpoint.connector_id:
        conn_dict = _lookup_saved_connector(endpoint.connector_id)
        if not conn_dict:
            raise ValueError(f"Connector {endpoint.connector_id} not found")
        cfg.update({
            "host": conn_dict.get("host") or cfg["host"],
            "port": conn_dict.get("port") or cfg["port"],
            "database": conn_dict.get("database") or cfg["database"],
            "schema": conn_dict.get("schema") or cfg["schema"],
            "username": conn_dict.get("username") or cfg["username"],
            "password": conn_dict.get("password") or cfg["password"],
            "connection_string": conn_dict.get("connection_string") or cfg["connection_string"],
            "warehouse": conn_dict.get("warehouse") or cfg["warehouse"],
            "ssl": conn_dict.get("ssl", cfg["ssl"]),
            "type": conn_dict.get("type") or endpoint.format,
            "auth_mode": conn_dict.get("auth_mode") or cfg["auth_mode"],
            "auth_role": conn_dict.get("auth_role") or cfg["auth_role"],
            "auth_source": conn_dict.get("auth_source") or cfg["auth_source"],
            "api_key": conn_dict.get("api_key") or cfg["api_key"],
            "service_account": conn_dict.get("service_account") or cfg["service_account"],
            "role": conn_dict.get("role") or cfg["role"],
        })
    return cfg


def _lookup_saved_connector(connector_id: str) -> dict[str, Any] | None:
    """Find a saved connector in the file-backed store, falling back to MongoDB."""
    try:
        from services.connector_store import get_connector as fs_get

        conn = fs_get(connector_id)
        if conn:
            return {
                "host": conn.host,
                "port": conn.port,
                "database": conn.database,
                "schema": conn.schema,
                "username": conn.username,
                "password": conn.password,
                "connection_string": conn.connection_string,
                "warehouse": conn.warehouse,
                "ssl": conn.ssl,
                "type": conn.type,
                "auth_mode": getattr(conn, "auth_mode", ""),
                "auth_role": getattr(conn, "auth_role", ""),
                "auth_source": getattr(conn, "auth_source", ""),
                "api_key": getattr(conn, "api_key", ""),
                "service_account": getattr(conn, "service_account", ""),
                "role": getattr(conn, "auth_role", ""),
            }
    except Exception:
        pass
    try:
        mongo = get_mongodb_service()
        return mongo.get_connector(connector_id)
    except Exception:
        return None


def _introspect_table_schema(db_type: str, cfg: dict[str, Any], table: str, headers: list[str]) -> dict[str, str]:
    """Load column types from INFORMATION_SCHEMA when the driver is available."""
    if db_type == "generic_sql":
        try:
            from connectors.generic_sql import introspect_table_schema

            info = introspect_table_schema(cfg, table)
            if info.get("ok") and info.get("columns"):
                return {c["name"]: c["inferred_type"] for c in info["columns"]}
        except Exception:
            pass

    from services.schema_introspect import introspect_schema

    info = introspect_schema(
        db_type,
        host=cfg.get("host", ""),
        port=int(cfg.get("port", 5432) or 5432),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        schema=cfg.get("schema", "public"),
        connection_string=cfg.get("connection_string", ""),
        ssl=cfg.get("ssl", False),
        warehouse=cfg.get("warehouse", ""),
        table=table,
        catalog_type=cfg.get("type", ""),
        auth_source=cfg.get("auth_source", ""),
    )
    if info.get("ok") and info.get("columns"):
        return {c["name"]: c["inferred_type"] for c in info["columns"]}
    return {h: "TEXT" for h in headers}


_NON_STREAMING_ROW_LIMIT = 100_000


def _guard_truncated_read(batch, db_type: str, name: str) -> None:
    """Fail closed when a non-streaming read would silently drop rows."""
    if batch.total_rows > len(batch.rows):
        raise ValueError(
            f"Source {db_type}.{name} has {batch.total_rows:,} rows but non-streaming reads "
            f"are capped at {len(batch.rows):,}. Use database-to-database transfer (async) "
            "for large tables."
        )


def read_source_database(
    endpoint: EndpointConfig,
    *,
    limit: int = _NON_STREAMING_ROW_LIMIT,
    raise_on_truncate: bool = True,
) -> tuple[list[dict], list[str], dict[str, str]]:
    from .connector_capabilities import resolve_driver_type
    cfg = resolve_connector_config(endpoint)
    db_type = resolve_driver_type(endpoint.format)

    if db_type == "postgresql" or db_type == "redshift":
        from connectors.postgresql_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError(f"Source {db_type} table name required")
        pg_port = cfg["port"] or (5439 if db_type == "redshift" else 5432)
        batch = read_table_batch(
            host=cfg["host"],
            port=pg_port,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema("postgresql", cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "mongodb":
        from connectors.mongodb_reader import read_collection_batch

        # Resolve the auth database for MongoDB so the same credentials work
        # even when the user was created in a different DB than the data DB.
        ok, msg = run_probe("mongodb", cfg)
        if not ok:
            raise RuntimeError(msg)

        coll_name = endpoint.collection or endpoint.table
        if not coll_name:
            raise ValueError("Source MongoDB collection name required")
        db_name = endpoint.database or cfg["database"] or "test"
        batch = read_collection_batch(
            cfg=cfg,
            database=db_name,
            collection=coll_name,
            offset=0,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, "mongodb", coll_name)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema("mongodb", cfg, coll_name, batch.headers)
        return records, batch.headers, schema

    if db_type == "mysql":
        from connectors.mysql_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source MySQL table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"] or 3306,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "bigquery":
        from connectors.bigquery_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source BigQuery table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"] or 443,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "dataflow"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            limit=limit,
            service_account=cfg.get("service_account", ""),
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "snowflake":
        from connectors.snowflake_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source Snowflake table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"] or 443,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            limit=limit,
            role=cfg.get("role", ""),
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "gcs":
        from connectors.gcs_reader import read_object

        bucket = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not bucket or not key:
            raise ValueError("GCS source requires bucket (database) and object key (table/collection)")
        batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "adls":
        from connectors.adls_reader import read_object

        container = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not container or not key:
            raise ValueError("Azure Blob source requires container (database) and blob key (table/collection)")
        batch = read_object(cfg=cfg, bucket=container, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "s3":
        from connectors.s3_reader import read_object

        bucket = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not bucket or not key:
            raise ValueError("S3 source requires bucket (database) and object key (table/collection)")
        batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "dynamodb":
        from connectors.dynamodb_reader import read_all_paginated

        table = endpoint.table or endpoint.collection or endpoint.database
        if not table:
            raise ValueError("DynamoDB table name required (table field)")
        batch = read_all_paginated(cfg, table, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "elasticsearch":
        from connectors.elasticsearch_reader import read_index_batch

        index = endpoint.table or endpoint.database or endpoint.collection
        if not index:
            raise ValueError("Elasticsearch index name required (table or database field)")
        batch, _ = read_index_batch(cfg=cfg, index=index, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, index)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "redis":
        from connectors.redis_reader import read_keys_batch

        pattern = endpoint.table or endpoint.collection or endpoint.schema or "*"
        if pattern != "*" and "*" not in pattern and "?" not in pattern:
            pattern = f"{pattern}:*"
        batch, _ = read_keys_batch(cfg=cfg, pattern=pattern, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, pattern)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "sqlite":
        from connectors.sqlite_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source SQLite table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "generic_sql":
        from connectors.generic_sql import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema") or "",
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            type=cfg.get("type", ""),
            table=table,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "sftp":
        from connectors.sftp_reader import read_object

        if not endpoint.table and not endpoint.connection_string and not endpoint.database:
            raise ValueError("SFTP source requires a remote file path (connection_string, database, or table field)")
        batch = read_object(
            cfg=cfg,
            bucket=endpoint.database,
            key=endpoint.table,
            offset=0,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, endpoint.table or endpoint.database or endpoint.connection_string)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = FileParser.infer_schema(records) if records else {c: "string" for c in batch.headers}
        return records, batch.headers, schema

    if db_type == "email":
        raise ValueError("Email cannot be a transfer source; configure it as a destination only.")

    raise ValueError(f"Database source '{db_type}' read not implemented")


def write_destination_database(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    validation_mode: str = "strict",
    backfill_new_fields: bool = False,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
) -> tuple[int, list[str], dict]:
    from .connector_capabilities import resolve_driver_type
    db_type = resolve_driver_type(endpoint.format)
    cfg = resolve_connector_config(endpoint)
    ddl_log: list[str] = []

    from connectors.writer_common import transform_error_policy_for_validation_mode
    error_policy = transform_error_policy_for_validation_mode(validation_mode)

    headers, data_rows = records_to_matrix(records, columns)
    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    table_name = resolve_dest_table(db_type, endpoint, "dt_import")

    common = {
        "host": cfg["host"],
        "port": cfg["port"] or (
            5439 if db_type == "redshift" else
            5432 if db_type == "postgresql" else
            3306 if db_type == "mysql" else
            22 if db_type == "sftp" else
            587 if db_type == "email" else
            0 if db_type == "generic_sql" else 443
        ),
        "database": cfg["database"],
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema", "public"),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "auth_source": cfg.get("auth_source", ""),
        "service_account": cfg.get("service_account", ""),
        "api_key": cfg.get("api_key", ""),
        "role": cfg.get("role", ""),
        "table_name": table_name,
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "column_types": column_types,
        "on_checkpoint": on_checkpoint,
        "error_policy": error_policy,
        "backfill_new_fields": backfill_new_fields,
    }

    if db_type == "snowflake":
        from connectors.snowflake_writer import write_mapped_rows
        common["schema"] = cfg.get("schema", "PUBLIC")
        common["warehouse"] = cfg.get("warehouse", "")
        for col in columns:
            ddl_log.append(f"SNOWFLAKE COLUMN {col} {ddl_type('snowflake', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or "Snowflake write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "snowflake", "schema": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "postgresql" or db_type == "redshift":
        from connectors.postgresql_writer import write_mapped_rows
        common["schema"] = cfg.get("schema", "public")
        if db_type == "redshift":
            common["port"] = cfg["port"] or 5439
        for col in columns:
            ddl_log.append(f"PG COLUMN {col} {ddl_type('postgresql', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or f"{db_type} write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": db_type, "schema": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "mysql":
        from connectors.mysql_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"MYSQL COLUMN {col} {ddl_type('mysql', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or "MySQL write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "mysql", "database": cfg["database"], "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "bigquery":
        from connectors.bigquery_writer import write_mapped_rows
        common["schema"] = cfg.get("schema", "dataflow")
        for col in columns:
            ddl_log.append(f"BQ COLUMN {col} {ddl_type('bigquery', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common, warehouse=cfg.get("warehouse", ""))
        if not result.ok:
            raise RuntimeError(result.error or "BigQuery write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {cfg['database']}.{result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "bigquery", "project": cfg["database"], "dataset": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "mongodb":
        from connectors.mongodb_writer import write_mapped_rows

        # Resolve the auth database for MongoDB before the actual write.
        ok, msg = run_probe("mongodb", cfg)
        if not ok:
            raise RuntimeError(msg)
        common["auth_source"] = cfg.get("auth_source", "")

        common["schema"] = cfg.get("schema", "db")
        for col in columns:
            ddl_log.append(f"MONGODB FIELD {col} string")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or "MongoDB write failed")
        ddl_log.insert(0, f"CREATE COLLECTION IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "mongodb", "database": result.target_schema, "collection": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "gcs":
        from connectors.gcs_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"GCS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "GCS write failed")
        ddl_log.insert(0, f"PUT gs://{cfg['database']}/{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "gcs", "bucket": cfg["database"], "key": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "adls":
        from connectors.adls_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"ADLS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "Azure Blob write failed")
        ddl_log.insert(0, f"PUT abfs://{cfg['database']}/{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "adls", "container": cfg["database"], "key": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "s3":
        from connectors.s3_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"S3 FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "S3 write failed")
        ddl_log.insert(0, f"PUT s3://{cfg['database']}/{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "s3", "bucket": cfg["database"], "key": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "dynamodb":
        from connectors.dynamodb_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"DYNAMODB ATTR {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "DynamoDB write failed")
        ddl_log.insert(0, f"BATCH WRITE DynamoDB table {result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "dynamodb", "table": result.table_name, "region": result.target_schema,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "elasticsearch":
        from connectors.elasticsearch_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"ES FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "Elasticsearch write failed")
        ddl_log.insert(0, f"BULK INDEX {result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "elasticsearch", "index": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "redis":
        from connectors.redis_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"REDIS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "Redis write failed")
        ddl_log.insert(0, f"SET keys under prefix {result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "redis", "prefix": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "sqlite":
        from connectors.sqlite_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"SQLITE COLUMN {col} {ddl_type('sqlite', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or "SQLite write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "sqlite", "database": cfg["database"], "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "generic_sql":
        from connectors.generic_sql import write_mapped_rows
        for col in columns:
            ddl_log.append(f"GENERIC_SQL COLUMN {col} {ddl_type('generic_sql', schema.get(col, 'string'))}")
        common["type"] = cfg.get("type", "")
        result = write_mapped_rows(**common, write_mode=write_mode, conflict_columns=conflict_columns or [])
        if not result.ok:
            raise RuntimeError(result.error or "Generic SQL write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "generic_sql", "driver": result.driver,
            "schema": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, **_writer_diagnostics(result),
        }

    if db_type == "sftp":
        from connectors.sftp_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"SFTP FIELD {col}")
        if not common.get("table_name") and not endpoint.connection_string and not endpoint.database:
            raise ValueError("SFTP destination requires a remote file path (connection_string, database, or table field)")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "SFTP write failed")
        ddl_log.insert(0, f"PUT sftp://{cfg.get('host', '')}/{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "sftp", "host": cfg.get("host", ""), "path": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "email":
        from connectors.email import write_mapped_rows
        for col in columns:
            ddl_log.append(f"EMAIL FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "Email send failed")
        ddl_log.insert(0, f"EMAIL {result.table_name} via {cfg.get('host', '')}")
        return result.rows_written, ddl_log, {
            "type": "email", "host": cfg.get("host", ""), "subject": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    raise ValueError(f"Database destination '{db_type}' write not implemented")


def write_destination_file(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    *,
    source_format: str | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
) -> tuple[bytes, str, dict]:
    """Write records to CSV, JSON, JSONL, or TSV using unified format converter."""
    import sys
    from pathlib import Path

    _api_root = Path(__file__).resolve().parents[2]
    if str(_api_root) not in sys.path:
        sys.path.insert(0, str(_api_root))
    from connectors.writer_common import build_mapped_rows, resolve_target_columns
    from services.format_converter import can_convert, convert_rows

    fmt = (endpoint.format or "json").lower()
    # ndjson is a JSON Lines alias for the conversion engine
    if fmt == "ndjson":
        fmt = "jsonl"
    src_fmt = (source_format or fmt).lower()
    if src_fmt == "ndjson":
        src_fmt = "jsonl"
    types = column_types or {}

    export_columns = columns
    export_records = records
    transform_errors: list[str] = []

    if mappings:
        headers = columns
        data_rows = [[cell_to_string(rec.get(col, "")) for col in headers] for rec in records]
        target_cols, _ = resolve_target_columns(mappings, types)
        mapped_rows, transform_errors = build_mapped_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=types,
        )
        export_columns = target_cols
        export_records = [
            dict(zip(target_cols, row))
            for row in mapped_rows
        ]

    grid = [[cell_to_string(rec.get(col, "")) for col in export_columns] for rec in export_records]

    if can_convert(src_fmt, fmt) and grid:
        content, mime = convert_rows(export_columns, grid, source_format=src_fmt, target_format=fmt)
        ext = (
            "tsv" if fmt == "tsv"
            else "xlsx" if fmt == "excel"
            else "parquet" if fmt == "parquet"
            else fmt if fmt in ("csv", "jsonl")
            else "json"
        )
        filename = f"export.{ext}"
        return content, filename, {
            "format": fmt,
            "filename": filename,
            "rows": len(export_records),
            "mime": mime,
            "converted_from": src_fmt if src_fmt != fmt else None,
            "transform_errors": transform_errors[:10],
            "mapped": bool(mappings),
        }

    def _to_json_value(value: Any, col: str) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value
            ctype = normalize_inferred(types.get(col, "string")).lower()
            if ctype in {"json", "array", "object", "struct"}:
                try:
                    return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
                except json.JSONDecodeError:
                    return value
            if ctype in {"text", "string", "varchar", "uuid", "binary", "date", "datetime", "time"}:
                return value
            try:
                return json.loads(text, parse_float=Decimal, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        return value

    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=export_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{c: cell_to_string(v) for c, v in r.items()} for r in export_records])
        content = buf.getvalue().encode("utf-8")
        filename = "export.csv"
    elif fmt == "tsv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=export_columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{c: cell_to_string(v) for c, v in r.items()} for r in export_records])
        content = buf.getvalue().encode("utf-8")
        filename = "export.tsv"
    elif fmt == "jsonl":
        records = [{c: _to_json_value(v, c) for c, v in r.items()} for r in export_records]
        lines = [json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records]
        content = "\n".join(lines).encode("utf-8")
        filename = "export.jsonl"
    elif fmt == "excel":
        content, _ = convert_rows(export_columns, grid, source_format="csv", target_format=fmt)
        filename = "export.xlsx"
    elif fmt == "parquet":
        content, _ = convert_rows(export_columns, grid, source_format="csv", target_format=fmt)
        filename = "export.parquet"
    else:
        records = [{c: _to_json_value(v, c) for c, v in r.items()} for r in export_records]
        content = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")
        filename = "export.json"
    return content, filename, {
        "format": fmt,
        "filename": filename,
        "rows": len(export_records),
        "transform_errors": transform_errors[:10],
        "mapped": bool(mappings),
    }
