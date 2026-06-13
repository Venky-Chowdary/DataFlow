"""Read/write adapters for universal transfer — files, databases, warehouses."""

from __future__ import annotations
import csv
import io
import json
import re
import sys
from pathlib import Path
from typing import Any

# Legacy connectors live under apps/api/
_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from ..services.file_parser import FileParser
from ..services.mongodb_service import get_mongodb_service
from .models import EndpointConfig
from .type_mapper import ddl_type, normalize_inferred


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
        mongo = get_mongodb_service()
        conn = mongo.get_connector(endpoint.connector_id)
        if not conn:
            raise ValueError(f"Connector {endpoint.connector_id} not found")
        cfg.update({
            "host": conn.get("host", cfg["host"]),
            "port": conn.get("port", cfg["port"]),
            "database": conn.get("database", cfg["database"]),
            "username": conn.get("username", cfg["username"]),
            "password": conn.get("password", cfg["password"]),
            "connection_string": conn.get("connection_string", cfg["connection_string"]),
            "type": conn.get("type", endpoint.format),
        })
    return cfg


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
            ssl=cfg.get("ssl", True),
            table=table,
            limit=100_000,
        )
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = {h: "string" for h in batch.headers}
        return records, batch.headers, schema

    if db_type == "mongodb":
        mongo_svc = get_mongodb_service()
        if endpoint.connector_id:
            client, _ = mongo_svc.get_client_for_connector(endpoint.connector_id)
        else:
            mongo_svc.connect()
            client = mongo_svc.client
        if not client:
            raise ValueError("MongoDB connection failed")
        db = client[endpoint.database or cfg["database"] or "test"]
        coll_name = endpoint.collection
        if not coll_name:
            raise ValueError("Source MongoDB collection name required")
        coll = db[coll_name]
        records = list(coll.find().limit(100_000))
        for r in records:
            if "_id" in r:
                r["_id"] = str(r["_id"])
        columns = list(records[0].keys()) if records else []
        schema = {c: "string" for c in columns}
        return records, columns, schema

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
        schema = {h: "string" for h in batch.headers}
        return records, batch.headers, schema

    raise ValueError(f"Database source '{db_type}' read not implemented")


def write_destination_database(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict],
) -> tuple[int, list[str], dict]:
    db_type = endpoint.format.lower()
    cfg = resolve_connector_config(endpoint)
    ddl_log: list[str] = []

    if db_type == "mongodb":
        mongo = get_mongodb_service()
        client, _ = mongo.get_client_for_connector(endpoint.connector_id) if endpoint.connector_id else (None, None)
        db_name = endpoint.database or cfg["database"] or "test_db"
        coll_name = endpoint.collection or endpoint.table or "imported_data"
        ddl_log.append(f"CREATE COLLECTION {db_name}.{coll_name} (if not exists)")
        create = mongo.create_collection_from_schema(db_name, coll_name, schema, client=client)
        if create.get("message"):
            ddl_log.append(create["message"])
        insert = mongo.insert_data(db_name, coll_name, records, client=client)
        if client:
            client.close()
        if not insert["success"]:
            raise RuntimeError(insert.get("error", "MongoDB insert failed"))
        return insert["inserted_count"], ddl_log, {"database": db_name, "collection": coll_name, "type": "mongodb"}

    headers, data_rows = records_to_matrix(records, columns)
    column_types = {c: normalize_inferred(schema.get(c, "string")).upper() for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    base = re.sub(r"[^a-zA-Z0-9_]", "_", (endpoint.table or endpoint.collection or "dt_import").lower())[:40]
    table_name = endpoint.table or endpoint.collection or f"dt_{base}"

    common = {
        "host": cfg["host"],
        "port": cfg["port"] or (5432 if db_type == "postgresql" else 443),
        "database": cfg["database"],
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": cfg.get("schema", "public"),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", True),
        "table_name": table_name,
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "column_types": column_types,
        "on_checkpoint": None,
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
        }

    if db_type == "postgresql":
        from connectors.postgresql_writer import write_mapped_rows
        for col in columns:
            ddl_log.append(f"PG COLUMN {col} {ddl_type('postgresql', schema.get(col, 'string'))}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise RuntimeError(result.error or "PostgreSQL write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}")
        return result.rows_written, ddl_log, {
            "type": "postgresql", "schema": result.target_schema, "table": result.table_name,
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
