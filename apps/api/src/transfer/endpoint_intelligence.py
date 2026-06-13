"""Understand source and destination endpoints — probe, list objects, plan DDL."""

from __future__ import annotations

from .adapters import parse_file_content, read_source_database, resolve_connector_config
from .models import EndpointConfig
from .type_mapper import ddl_type


def introspect_endpoint(
    endpoint: EndpointConfig,
    sample_content: bytes | None = None,
    filename: str = "",
) -> dict:
    """
    Probe an endpoint: connection health, available tables/collections,
    column schema, and what will be auto-created on write.
    """
    fmt = (endpoint.format or "").lower()
    out: dict = {
        "kind": endpoint.kind,
        "format": endpoint.format,
        "connected": False,
        "objects": [],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "",
    }

    if endpoint.kind == "file":
        if not sample_content:
            out["message"] = "Upload a file to analyze source schema"
            return out
        _, columns, schema = parse_file_content(sample_content, filename or "upload.csv")
        out["connected"] = True
        out["columns"] = columns
        out["schema"] = schema
        out["message"] = f"File parsed — {len(columns)} columns"
        return out

    if endpoint.kind == "file_export":
        out["connected"] = True
        out["format"] = fmt or "json"
        out["message"] = f"Export destination — {out['format'].upper()} file will be generated"
        out["auto_create"].append(f"Write export.{out['format']} to exports folder")
        return out

    if endpoint.kind != "database":
        out["message"] = f"Unknown endpoint kind: {endpoint.kind}"
        return out

    cfg = resolve_connector_config(endpoint)

    if fmt == "postgresql":
        from connectors.postgresql import test_postgresql

        probe = test_postgresql(
            host=cfg["host"],
            port=cfg["port"] or 5432,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", True),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "mongodb":
        try:
            from ..services.mongodb_service import get_mongodb_service

            mongo = get_mongodb_service()
            if endpoint.connector_id:
                client, _ = mongo.get_client_for_connector(endpoint.connector_id)
            else:
                mongo.connect()
                client = mongo.client
            if not client:
                out["message"] = "MongoDB connection failed"
                return out
            db_name = endpoint.database or cfg["database"] or "test"
            db = client[db_name]
            colls = db.list_collection_names()
            out["connected"] = True
            out["objects"] = [{"name": c, "type": "collection"} for c in colls[:50]]
            out["message"] = f"MongoDB connected — {len(colls)} collections in `{db_name}`"
            if endpoint.collection:
                _attach_db_sample(out, endpoint)
            if endpoint.connector_id and client:
                client.close()
        except Exception as e:
            out["message"] = str(e)
        return out

    if fmt == "snowflake":
        from connectors.snowflake import test_snowflake

        probe = test_snowflake(
            host=cfg["host"],
            port=cfg["port"] or 443,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", True),
            warehouse=cfg.get("warehouse", ""),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    out["message"] = f"Introspection for `{fmt}` not yet implemented"
    return out


def _attach_db_sample(out: dict, endpoint: EndpointConfig) -> None:
    try:
        records, columns, schema = read_source_database(endpoint)
        out["columns"] = columns
        out["schema"] = schema
        out["row_estimate"] = len(records)
    except Exception as e:
        out["message"] = f"{out.get('message', '')} · sample read failed: {e}"


def build_transfer_plan(source: EndpointConfig, destination: EndpointConfig, source_info: dict) -> dict:
    """Plan auto-creation and type mappings for a source → destination transfer."""
    from .registry import validate_transfer

    src_fmt = source.format or ("csv" if source.kind == "file" else source.format)
    dst_fmt = destination.format or ("mongodb" if destination.kind == "database" else "json")
    ok, msg = validate_transfer(source.kind, src_fmt, destination.kind, dst_fmt)

    plan: dict = {
        "supported": ok,
        "message": msg,
        "operation": _operation(source.kind, destination.kind),
        "auto_create": [],
        "type_mappings": [],
        "source": source_info,
        "destination": {},
    }

    if not ok:
        return plan

    columns = source_info.get("columns") or []
    schema = source_info.get("schema") or {}

    if destination.kind == "database":
        db = dst_fmt.lower()
        target = destination.table or destination.collection or "imported_data"
        dest_info = introspect_endpoint(destination)
        plan["destination"] = dest_info
        if db == "mongodb":
            plan["auto_create"].append(f"MongoDB collection `{destination.database or 'test_db'}.{target}`")
        else:
            sch = destination.schema or ("PUBLIC" if db == "snowflake" else "public")
            plan["auto_create"].append(f"{db} table `{sch}.{target}` with typed columns (CREATE IF NOT EXISTS)")
        for col in columns:
            plan["type_mappings"].append({
                "column": col,
                "source_type": schema.get(col, "string"),
                "dest_type": ddl_type(db, schema.get(col, "string")),
            })
    elif destination.kind == "file_export":
        plan["destination"] = introspect_endpoint(destination)
        plan["auto_create"].append(f"Export file as `{dst_fmt}` in server exports folder")

    return plan


def _operation(source_kind: str, dest_kind: str) -> str:
    if source_kind == "file" and dest_kind == "database":
        return "upload"
    if source_kind == "database" and dest_kind == "database":
        return "migration"
    if source_kind == "file" and dest_kind == "file_export":
        return "convert"
    if source_kind == "database" and dest_kind == "file_export":
        return "dump"
    return "transfer"
