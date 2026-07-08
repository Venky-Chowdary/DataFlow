"""Read/write adapters for universal transfer — files, databases, warehouses."""

from __future__ import annotations
import csv
import io
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

# Legacy connectors live under apps/api/
_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from ..services.file_parser import FileParser
from ..services.mongodb_service import get_mongodb_service
from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    warnings = list(getattr(result, "warnings", []) or [])
    return {
        "rejected_rows": rejected,
        "warnings": warnings[:10],
        "error_policy": "quarantine" if rejected else "none",
    }


def parse_file_content(content: bytes, filename: str) -> tuple[list[dict], list[str], dict[str, str]]:
    result = FileParser.parse(content, filename)
    if not result.success:
        raise ValueError(result.error or "File parse failed")
    schema = FileParser.infer_schema(result.data)
    return result.data, result.columns, schema


def records_to_matrix(records: list[dict], columns: list[str]) -> tuple[list[str], list[list[str]]]:
    headers = columns or (list(records[0].keys()) if records else [])
    rows = [[str(rec.get(h, "") if rec.get(h) is not None else "") for h in headers] for rec in records]
    return headers, rows


def resolve_connector_config(endpoint: EndpointConfig) -> dict[str, Any]:
    """Merge saved connector with inline overrides."""
    cfg = {
        "host": endpoint.host or "localhost",
        "port": endpoint.port or 5432,
        "database": endpoint.database,
        "schema": endpoint.schema or "public",
        "username": endpoint.username,
        "password": endpoint.password,
        "connection_string": endpoint.connection_string,
        "warehouse": endpoint.warehouse,
        "ssl": endpoint.ssl,
        "type": endpoint.format,
    }
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
    )
    if info.get("ok") and info.get("columns"):
        return {c["name"]: c["inferred_type"] for c in info["columns"]}
    return {h: "TEXT" for h in headers}


def read_source_database(endpoint: EndpointConfig) -> tuple[list[dict], list[str], dict[str, str]]:
    cfg = resolve_connector_config(endpoint)
    db_type = endpoint.format.lower()

    if db_type == "postgresql":
        from connectors.postgresql_reader import read_table_batch

        table = endpoint.table
        if not table:
            raise ValueError("Source PostgreSQL table name required")
        batch = read_table_batch(
            host=cfg["host"],
            port=cfg["port"] or 5432,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            limit=100_000,
        )
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    if db_type == "mongodb":
        from pymongo import MongoClient

        if cfg.get("connection_string"):
            conn_str = cfg["connection_string"]
        elif cfg.get("username") and cfg.get("password"):
            conn_str = f"mongodb://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port'] or 27017}/"
        else:
            conn_str = f"mongodb://{cfg['host']}:{cfg['port'] or 27017}/"
        client = MongoClient(conn_str, serverSelectionTimeoutMS=10000)
        db = client[endpoint.database or cfg["database"] or "test"]
        coll_name = endpoint.collection
        if not coll_name:
            raise ValueError("Source MongoDB collection name required")
        coll = db[coll_name]
        records = list(coll.find().limit(100_000))
        client.close()
        for r in records:
            if "_id" in r:
                r["_id"] = str(r["_id"])
        columns = list(records[0].keys()) if records else []
        schema = {c: "string" for c in columns}
        return records, columns, schema

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
            limit=100_000,
        )
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
            limit=100_000,
        )
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
            limit=100_000,
        )
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(db_type, cfg, table, batch.headers)
        return records, batch.headers, schema

    raise ValueError(f"Database source '{db_type}' read not implemented")


def write_destination_database(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
) -> tuple[int, list[str], dict]:
    db_type = endpoint.format.lower()
    cfg = resolve_connector_config(endpoint)
    ddl_log: list[str] = []



    headers, data_rows = records_to_matrix(records, columns)
    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    base = re.sub(r"[^a-zA-Z0-9_]", "_", (endpoint.table or endpoint.collection or "dt_import").lower())[:40]
    table_name = endpoint.table or endpoint.collection or f"dt_{base}"

    common = {
        "host": cfg["host"],
        "port": cfg["port"] or (
            5432 if db_type == "postgresql" else
            3306 if db_type == "mysql" else 443
        ),
        "database": cfg["database"],
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema", "public"),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "table_name": table_name,
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "column_types": column_types,
        "on_checkpoint": on_checkpoint,
    }

    if db_type == "snowflake":
        from connectors.snowflake_writer import write_mapped_rows
        common["schema"] = cfg.get("schema", "PUBLIC")
        common["warehouse"] = cfg.get("warehouse", "")
        for col in columns:
            ddl_log.append(f"SNOWFLAKE COLUMN {col} {ddl_type('snowflake', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "Snowflake write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "snowflake", "schema": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "postgresql":
        from connectors.postgresql_writer import write_mapped_rows
        common["schema"] = cfg.get("schema", "public")
        for col in columns:
            ddl_log.append(f"PG COLUMN {col} {ddl_type('postgresql', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "PostgreSQL write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "postgresql", "schema": result.target_schema, "table": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    if db_type == "mysql":
        from connectors.mysql_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"MYSQL COLUMN {col} {ddl_type('mysql', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common)
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
        common["schema"] = cfg.get("schema", "db")
        for col in columns:
            ddl_log.append(f"MONGODB FIELD {col} string")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "MongoDB write failed")
        ddl_log.insert(0, f"CREATE COLLECTION IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "mongodb", "database": result.target_schema, "collection": result.table_name,
            "checksum": result.checksum, "driver": result.driver,
            **_writer_diagnostics(result),
        }

    raise ValueError(f"Database destination '{db_type}' write not implemented")


def write_destination_file(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
) -> tuple[bytes, str, dict]:
    fmt = endpoint.format.lower() or "json"
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
        content = buf.getvalue().encode("utf-8")
        filename = f"export.{fmt}"
    elif fmt == "jsonl":
        lines = [json.dumps(r, default=str) for r in records]
        content = "\n".join(lines).encode("utf-8")
        filename = "export.jsonl"
    else:
        content = json.dumps(records, indent=2, default=str).encode("utf-8")
        filename = "export.json"
    return content, filename, {"format": fmt, "filename": filename, "rows": len(records)}
