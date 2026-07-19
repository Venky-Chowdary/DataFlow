"""Understand source and destination endpoints — probe, list objects, plan DDL."""

from __future__ import annotations

from typing import Any

from connectors.mongodb_common import _mongo_client

from .adapters import (
    _introspect_table_schema,
    mongodb_connection_string,
    parse_file_content,
    resolve_connector_config,
)
from .connector_capabilities import resolve_driver_type
from .models import EndpointConfig
from .type_mapper import ddl_type
from services.value_serializer import cell_to_string


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
    # When a saved connector is used, its stored driver type is authoritative;
    # ignore an inline format string that may have been sent as a placeholder.
    resolved_fmt = cfg.get("type") or endpoint.format
    fmt = (resolved_fmt or "").lower()
    out["format"] = resolved_fmt

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
            from .connector_registry import humanize_connection_error, run_probe

            ok, msg = run_probe(fmt, cfg)
            if not ok:
                out["message"] = msg
                return out
            # Carry the resolved auth_source into the endpoint so subsequent
            # sample reads use the same authentication database.
            endpoint.auth_source = cfg.get("auth_source", "") or endpoint.auth_source
            client = _mongo_client(mongodb_connection_string(cfg))
            db_name = endpoint.database or cfg["database"] or "test"
            db = client[db_name]
            # When the caller already supplied a collection/table, target it
            # directly instead of listing every collection. This avoids slow
            # namespace scans on large MongoDB deployments and makes the source
            # preview load in one round-trip.
            requested_coll = endpoint.collection
            if requested_coll:
                try:
                    db[requested_coll].find_one({})
                except Exception as coll_err:
                    out["message"] = f"Collection `{requested_coll}` not found or unreadable: {coll_err}"
                    return out
                out["connected"] = True
                out["objects"] = [{"name": requested_coll, "type": "collection"}]
                out["message"] = f"MongoDB connected — reading `{requested_coll}` in `{db_name}`"
                _attach_db_sample(out, endpoint)
            else:
                colls = db.list_collection_names()
                out["connected"] = True
                out["objects"] = [{"name": c, "type": "collection"} for c in colls[:50]]
                out["message"] = f"MongoDB connected — {len(colls)} collections in `{db_name}`"
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


def _attach_db_sample(out: dict, endpoint: EndpointConfig, sample_limit: int = 100) -> None:
    """Bounded schema discovery — safe for million-row tables."""
    try:
        cfg = resolve_connector_config(endpoint)
        # Use the resolved saved-connector driver type if available, otherwise
        # fall back to the inline format string.
        fmt = (cfg.get("type") or endpoint.format or "").lower()

        if fmt == "mongodb":
            import json

            from pymongo import MongoClient

            from services.schema_inference import infer_schema_map

            coll_name = endpoint.collection
            if not coll_name:
                return
            try:
                # Reuse the cached MongoClient to avoid repeated connection handshakes
                # when the UI polls/retries introspection for the same connector.
                client = _mongo_client(mongodb_connection_string(cfg))
                db = client[endpoint.database or cfg["database"] or "test"]
                coll = db[coll_name]
                cursor = coll.find().max_time_ms(5000).limit(sample_limit)
                records = list(cursor)
            except Exception as exc:
                out["message"] = f"Collection sample failed: {exc}"
                return
            # Serialize MongoDB-native types to JSON-safe scalars so the
            # introspection response can be returned without FastAPI serialization errors.
            from services.value_serializer import json_default

            safe_records = []
            for r in records:
                safe_records.append(json.loads(json.dumps(r, default=json_default)))
            columns = list(safe_records[0].keys()) if safe_records else []
            # Canonical schema intelligence choke point (type + semantic_role).
            samples_by_field = {
                col: [cell_to_string(r.get(col)) for r in records[:100] if r.get(col) is not None]
                for col in columns
            }
            schema, intel = infer_schema_map(samples_by_field)
            for col in columns:
                if col not in schema:
                    schema[col] = "VARCHAR"
            out["columns"] = columns
            out["schema"] = schema
            out["schema_intelligence"] = {
                k: {
                    "logical_type": v.get("logical_type"),
                    "semantic_role": v.get("semantic_role"),
                    "confidence": v.get("confidence"),
                    "notes": v.get("notes") or [],
                }
                for k, v in intel.items()
            }
            out["sample_data"] = safe_records[:10]
            out["data"] = safe_records[:10]
            try:
                out["row_estimate"] = coll.estimated_document_count(maxTimeMS=5000) if columns else 0
            except Exception:
                out["row_estimate"] = len(records)
            out["table_exists"] = bool(columns)
            return

        if fmt == "redis":
            from connectors.redis_reader import read_keys_batch

            pattern = endpoint.table or endpoint.collection or endpoint.schema or "*"
            result = read_keys_batch(cfg=cfg, pattern=pattern, offset=0, limit=sample_limit)
            batch = result[0] if isinstance(result, tuple) else result
            out["columns"] = batch.headers
            out["schema"] = {c: "string" for c in batch.headers}
            out["row_estimate"] = (batch.total_rows or 0)
            out["table_exists"] = (batch.total_rows or 0) > 0
            _attach_batch_sample_rows(out, batch)
            return

        if fmt == "s3":
            from connectors.s3_reader import read_object

            bucket = cfg["database"]
            key = endpoint.table or endpoint.collection or ""
            if bucket and key:
                batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = (batch.total_rows or 0)
                out["table_exists"] = True
                _attach_batch_sample_rows(out, batch)
            return

        if fmt == "gcs":
            from connectors.gcs_reader import read_object

            bucket = cfg["database"]
            key = endpoint.table or endpoint.collection or ""
            if bucket and key:
                batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = (batch.total_rows or 0)
                out["table_exists"] = True
                _attach_batch_sample_rows(out, batch)
            return

        if fmt == "dynamodb":
            from connectors.dynamodb_reader import (
                describe_table_schema,
                estimate_item_count,
                read_all_paginated,
            )

            table = endpoint.database or endpoint.table
            if table:
                try:
                    names, schema_map = describe_table_schema(cfg, table)
                    out["columns"] = names
                    out["schema"] = schema_map
                    out["row_estimate"] = estimate_item_count(cfg, table)
                    out["table_exists"] = True
                except Exception:
                    out["columns"] = []
                    out["schema"] = {}
                    out["table_exists"] = False
                # Always load a bounded item sample for Validate dry-run
                # (describe_table alone previously left sample_data empty).
                try:
                    batch = read_all_paginated(cfg, table, limit=sample_limit)
                    if batch.headers:
                        out["columns"] = out.get("columns") or batch.headers
                        if not out.get("schema"):
                            out["schema"] = {c: "string" for c in batch.headers}
                        out["table_exists"] = True
                        if not out.get("row_estimate"):
                            out["row_estimate"] = batch.total_rows or len(batch.rows)
                        _attach_batch_sample_rows(out, batch)
                except Exception as sample_exc:
                    out["sample_error"] = str(sample_exc)
                    out["message"] = (
                        f"{out.get('message', '')} · DynamoDB sample failed: {sample_exc}"
                    ).strip(" ·")
            return

        if fmt == "elasticsearch":
            from connectors.elasticsearch_reader import read_index_batch

            index = endpoint.database or endpoint.table
            if index:
                result = read_index_batch(cfg=cfg, index=index, offset=0, limit=sample_limit)
                batch = result[0] if isinstance(result, tuple) else result
                out["columns"] = batch.headers
                out["schema"] = {c: "string" for c in batch.headers}
                out["row_estimate"] = (batch.total_rows or 0)
                out["table_exists"] = (batch.total_rows or 0) > 0
                _attach_batch_sample_rows(out, batch)
            return

        table = endpoint.table or endpoint.collection
        if not table:
            out["table_exists"] = False
            return
        schema_map = _introspect_table_schema(fmt, cfg, table, [])
        if schema_map:
            out["columns"] = list(schema_map.keys())
            out["schema"] = schema_map
            out["table_exists"] = True
            out["message"] = out.get("message") or f"Found existing table `{table}`"
            # Schema-only is not enough for Validate dry-run — fetch a bounded
            # sample so Transfer Studio can run transform integrity checks.
            _attach_sql_sample_rows(out, endpoint, cfg, fmt, table, sample_limit)
        else:
            # Missing table is a valid destination — writers CREATE TABLE IF NOT EXISTS.
            out["columns"] = []
            out["schema"] = {}
            out["table_exists"] = False
            out["auto_create"] = list(out.get("auto_create") or []) + [
                f'CREATE TABLE IF NOT EXISTS "{table}" (from source schema on first write)'
            ]
            out["message"] = (
                f"Table `{table}` not found — it will be created automatically on first write"
            )
    except Exception as e:
        # Soft-fail: still allow the wizard to continue with auto-create.
        out["table_exists"] = False
        out["columns"] = out.get("columns") or []
        out["schema"] = out.get("schema") or {}
        out["message"] = f"{out.get('message', '')} · schema probe: {e}".strip(" ·")


def _attach_batch_sample_rows(out: dict, batch: Any, *, preview: int = 10) -> None:
    """Attach JSON-safe sample rows from a ReadBatch for Validate dry-run."""
    headers = list(batch.headers or [])
    rows = list(batch.rows or [])
    safe: list[dict] = []
    for row in rows[:preview]:
        if isinstance(row, dict):
            safe.append({h: cell_to_string(row.get(h, "")) for h in headers})
        else:
            safe.append({
                h: cell_to_string(row[i] if i < len(row) else "")
                for i, h in enumerate(headers)
            })
    out["sample_data"] = safe
    out["data"] = safe
    if safe:
        out["message"] = (
            f"{out.get('message', '')} · {len(safe)} sample row(s) loaded"
        ).strip(" ·")
    elif headers:
        out["message"] = (
            f"{out.get('message', '')} · source returned 0 sample rows (empty)"
        ).strip(" ·")


def _attach_sql_sample_rows(
    out: dict,
    endpoint: EndpointConfig,
    cfg: dict,
    fmt: str,
    table: str,
    sample_limit: int,
) -> None:
    """Read up to ``sample_limit`` rows for SQL/warehouse sources (Snowflake, PG, …).

    Introspection historically returned columns only; dry-run then blocked with
    \"No sample rows available\". This attaches ``data`` / ``sample_data`` for
    the Validate step without scanning the full table.
    """
    try:
        from .adapters import read_source_database

        sample_ep = EndpointConfig(
            kind="database",
            format=fmt,
            connector_id=endpoint.connector_id,
            host=endpoint.host or cfg.get("host", ""),
            port=int(endpoint.port or cfg.get("port") or 0),
            database=endpoint.database or cfg.get("database", ""),
            schema=endpoint.schema or cfg.get("schema", ""),
            table=table,
            collection=endpoint.collection,
            username=endpoint.username or cfg.get("username", ""),
            password=endpoint.password or cfg.get("password", ""),
            connection_string=endpoint.connection_string or cfg.get("connection_string", ""),
            warehouse=endpoint.warehouse or cfg.get("warehouse", ""),
            ssl=bool(endpoint.ssl if endpoint.ssl is not None else cfg.get("ssl", False)),
            api_key=endpoint.api_key or cfg.get("api_key", ""),
            service_account=endpoint.service_account or cfg.get("service_account", ""),
            auth_source=endpoint.auth_source or cfg.get("auth_source", ""),
            auth_role=endpoint.auth_role or cfg.get("role", "") or cfg.get("auth_role", ""),
            extra=dict(endpoint.extra or {}),
        )
        # Cap preview reads — Validate only needs a small transform sample.
        limit = max(1, min(int(sample_limit or 100), 100))
        records, headers, inferred = read_source_database(
            sample_ep, limit=limit, raise_on_truncate=False
        )
        if headers and not out.get("columns"):
            out["columns"] = list(headers)
        if inferred:
            # Prefer live samples for type hints when information_schema was sparse.
            merged = dict(out.get("schema") or {})
            for col, typ in inferred.items():
                merged.setdefault(col, typ)
            out["schema"] = merged
        safe_records: list[dict] = []
        for row in records[:10]:
            safe_records.append({k: cell_to_string(row.get(k, "")) for k in (headers or out.get("columns") or [])})
        out["sample_data"] = safe_records
        out["data"] = safe_records
        if out.get("row_estimate") in (None, 0) and records:
            # Best-effort; full COUNT can be expensive on warehouses.
            out["row_estimate"] = max(int(out.get("row_estimate") or 0), len(records))
        if not records:
            out["message"] = (
                f"{out.get('message', '')} · table `{table}` is empty "
                f"(0 sample rows) — dry-run will treat this as an empty source"
            ).strip(" ·")
        else:
            out["message"] = (
                f"{out.get('message', '')} · {len(safe_records)} sample row(s) loaded"
            ).strip(" ·")
    except Exception as exc:
        # Keep schema; surface sample failure so UI can explain dry-run blocks.
        out["sample_error"] = str(exc)
        out["message"] = (
            f"{out.get('message', '')} · sample read failed: {exc}"
        ).strip(" ·")


def build_transfer_plan(source: EndpointConfig, destination: EndpointConfig, source_info: dict) -> dict:
    """Plan auto-creation and type mappings for a source → destination transfer."""
    from .adapters import resolve_connector_config
    from .registry import validate_transfer

    # When a saved connector is referenced, use its stored driver type as the
    # canonical format so the UI cannot accidentally pass an unrelated format.
    def _resolved_fmt(endpoint: EndpointConfig, fallback: str) -> str:
        if endpoint.connector_id:
            try:
                cfg = resolve_connector_config(endpoint)
                return cfg.get("type") or endpoint.format or fallback
            except Exception:
                pass
        return endpoint.format or fallback

    src_fmt = _resolved_fmt(source, "csv" if source.kind == "file" else source.format or "json")
    dst_fmt = _resolved_fmt(destination, "mongodb" if destination.kind == "database" else "json")
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
