"""Understand source and destination endpoints — probe, list objects, plan DDL."""

from __future__ import annotations

from .adapters import _introspect_table_schema, mongodb_connection_string, parse_file_content, read_source_database, resolve_connector_config
from .connector_capabilities import resolve_driver_type
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
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "mongodb":
        try:
            from pymongo import MongoClient
            from .connector_registry import humanize_connection_error, run_probe

            ok, msg = run_probe(fmt, cfg)
            if not ok:
                out["message"] = msg
                return out
            # Carry the resolved auth_source into the endpoint so subsequent
            # sample reads use the same authentication database.
            endpoint.auth_source = cfg.get("auth_source", "") or endpoint.auth_source
            client = MongoClient(mongodb_connection_string(cfg), serverSelectionTimeoutMS=10000)
            db_name = endpoint.database or cfg["database"] or "test"
            db = client[db_name]
            colls = db.list_collection_names()
            out["connected"] = True
            out["objects"] = [{"name": c, "type": "collection"} for c in colls[:50]]
            out["message"] = f"MongoDB connected — {len(colls)} collections in `{db_name}`"
            if endpoint.collection:
                _attach_db_sample(out, endpoint)
            client.close()
        except Exception as e:
            out["message"] = humanize_connection_error("mongodb", e)
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
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            role=cfg.get("role", ""),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "mysql":
        from connectors.mysql import test_mysql

        probe = test_mysql(
            host=cfg["host"],
            port=cfg["port"] or 3306,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "bigquery":
        from connectors.bigquery import test_bigquery

        probe = test_bigquery(
            host=cfg["host"],
            port=cfg["port"] or 443,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "dataflow"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            service_account=cfg.get("service_account", ""),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "redshift":
        from connectors.redshift import test_redshift

        probe = test_redshift(
            host=cfg["host"], port=cfg["port"] or 5439, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "s3":
        from connectors.s3 import test_s3

        probe = test_s3(
            host=cfg["host"], port=cfg["port"] or 443, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", ""), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "object"} for t in probe.tables]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        key = endpoint.table or endpoint.collection
        if key and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "gcs":
        from connectors.gcs import test_gcs

        probe = test_gcs(
            host=cfg["host"], port=cfg["port"] or 443, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", ""), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            service_account=cfg.get("service_account", ""),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "object"} for t in probe.tables]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        key = endpoint.table or endpoint.collection
        if key and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "dynamodb":
        from connectors.dynamodb import test_dynamodb

        probe = test_dynamodb(
            host=cfg["host"], port=cfg["port"] or 443, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", ""), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "table"} for t in probe.tables]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "redis":
        from connectors.redis_kv import test_redis

        probe = test_redis(
            host=cfg["host"], port=cfg["port"] or 6379, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", ""), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "keyspace"} for t in probe.tables]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        return out

    if fmt == "elasticsearch":
        from connectors.elasticsearch import test_elasticsearch

        probe = test_elasticsearch(
            host=cfg["host"], port=cfg["port"] or 9200, database=cfg["database"],
            username=cfg.get("username", ""), password=cfg.get("password", ""),
            schema=cfg.get("schema", ""), connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            api_key=cfg.get("api_key", ""),
        )
        out["connected"] = probe.ok
        out["objects"] = [{"name": t, "type": "index"} for t in probe.tables if not t.startswith("(")]
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.database and probe.ok:
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "sqlite" or resolve_driver_type(fmt) == "generic_sql":
        out["connected"] = True
        out["message"] = f"{fmt.title()} connected — introspecting table schema"
        if endpoint.table:
            out["objects"] = [{"name": endpoint.table, "type": "table"}]
            _attach_db_sample(out, endpoint)
        return out

    out["message"] = f"Introspection for `{fmt}` not yet implemented"
    return out


def _attach_db_sample(out: dict, endpoint: EndpointConfig, sample_limit: int = 200) -> None:
    """Bounded schema discovery — safe for million-row tables."""
    try:
        cfg = resolve_connector_config(endpoint)
        fmt = (endpoint.format or "").lower()

        if fmt == "mongodb":
            from pymongo import MongoClient

            coll_name = endpoint.collection
            if not coll_name:
                return
            client = MongoClient(mongodb_connection_string(cfg), serverSelectionTimeoutMS=10000)
            db = client[endpoint.database or cfg["database"] or "test"]
            coll = db[coll_name]
            records = list(coll.find().limit(sample_limit))
            for r in records:
                if "_id" in r:
                    r["_id"] = str(r["_id"])
            columns = list(records[0].keys()) if records else []
            out["columns"] = columns
            out["schema"] = {c: "string" for c in columns}
            out["row_estimate"] = coll.estimated_document_count() if columns else 0
            out["table_exists"] = bool(columns)
            client.close()
            return

        if fmt == "redis":
            from connectors.redis_reader import read_keys_batch

            pattern = endpoint.table or endpoint.collection or endpoint.schema or "*"
            batch = read_keys_batch(cfg=cfg, pattern=pattern, offset=0, limit=sample_limit)
            out["columns"] = batch.headers
            out["schema"] = {c: "string" for c in batch.headers}
            out["row_estimate"] = batch.total_rows
            out["table_exists"] = batch.total_rows > 0
            return

        if fmt == "s3":
            from connectors.s3_reader import read_object

            bucket = cfg["database"]
            key = endpoint.table or endpoint.collection or ""
            if bucket and key:
                batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = batch.total_rows
                out["table_exists"] = True
            return

        if fmt == "gcs":
            from connectors.gcs_reader import read_object

            bucket = cfg["database"]
            key = endpoint.table or endpoint.collection or ""
            if bucket and key:
                batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = batch.total_rows
                out["table_exists"] = True
            return

        if fmt == "dynamodb":
            from connectors.dynamodb_reader import describe_table_schema, estimate_item_count

            table = endpoint.database or endpoint.table
            if table:
                try:
                    names, schema_map = describe_table_schema(cfg, table)
                    out["columns"] = names
                    out["schema"] = schema_map
                    out["row_estimate"] = estimate_item_count(cfg, table)
                    out["table_exists"] = True
                except Exception:
                    from connectors.dynamodb_reader import read_all_paginated

                    batch = read_all_paginated(cfg, table, limit=sample_limit)
                    out["columns"] = batch.headers
                    out["schema"] = {c: "string" for c in batch.headers}
                    out["row_estimate"] = batch.total_rows
                    out["table_exists"] = batch.total_rows > 0
            return

        if fmt == "elasticsearch":
            from connectors.elasticsearch_reader import read_index_batch

            index = endpoint.database or endpoint.table
            if index:
                batch = read_index_batch(cfg=cfg, index=index, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = batch.total_rows
                out["table_exists"] = batch.total_rows > 0
            return

        table = endpoint.table or endpoint.collection
        if not table:
            return
        schema_map = _introspect_table_schema(fmt, cfg, table, [])
        if schema_map:
            out["columns"] = list(schema_map.keys())
            out["schema"] = schema_map
            out["table_exists"] = True
    except Exception as e:
        out["message"] = f"{out.get('message', '')} · schema probe: {e}"


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
            sch = destination.schema or (
                "PUBLIC" if db == "snowflake" else
                "dataflow" if db == "bigquery" else "public"
            )
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
        try:
            from services.format_converter import can_convert
            from services.universal_router import analyze_route

            route = analyze_route(source.kind, src_fmt, destination.kind, dst_fmt)
            plan["route"] = route
            if route.get("conversion_needed"):
                plan["format_conversion"] = {
                    "from": src_fmt,
                    "to": dst_fmt,
                    "supported": can_convert(src_fmt, dst_fmt),
                }
        except Exception:
            pass

    if columns:
        try:
            from services.mapping_pipeline import run_mapping_pipeline

            dest_cols = plan.get("destination", {}).get("columns") or []
            preview = run_mapping_pipeline(
                columns,
                dest_cols,
                source_schemas=[
                    {"name": c, "inferred_type": schema.get(c, "VARCHAR"), "samples": []}
                    for c in columns
                ],
                target_schemas=[
                    {"name": c, "inferred_type": plan.get("destination", {}).get("schema", {}).get(c, "VARCHAR"), "samples": []}
                    for c in dest_cols
                ] if dest_cols else None,
                file_format=src_fmt if source.kind == "file" else None,
                confidence_threshold=0.75,
            )
            plan["mapping_preview"] = preview["mappings"][:20]
            plan["mapping_agents"] = preview.get("agents_used", [])
        except Exception:
            pass

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
