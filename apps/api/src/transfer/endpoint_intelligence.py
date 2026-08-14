"""Understand source and destination endpoints — probe, list objects, plan DDL."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from connectors.mongodb_common import _mongo_client
from services.value_serializer import cell_to_string

from .adapters import (
    _introspect_table_schema_rich,
    mongodb_connection_string,
    parse_file_content,
    resolve_connector_config,
)
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
    # When a saved connector is used, its stored driver type is authoritative;
    # ignore an inline format string that may have been sent as a placeholder.
    resolved_fmt = cfg.get("type") or endpoint.format
    fmt = (resolved_fmt or "").lower()
    out["format"] = resolved_fmt

    # pgvector is an extension, not a separate engine: the destination is an
    # ordinary PostgreSQL table with a vector column, reached through the same
    # connection config. Leaving it out of this branch meant existence and
    # column types were never measured, so Validate refused create-new and
    # overwrite for lack of facts one catalog query answers.
    if fmt in {"postgresql", "pgvector"}:
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
        out["objects_truncated"] = bool(getattr(probe, "tables_truncated", False))
        out["message"] = probe.message if probe.ok else (probe.error or "Connection failed")
        if endpoint.table and probe.ok:
            if not _mark_table_listed_if_present(out, endpoint.table):
                # Absence from a bounded page is a hint, not proof. The SQL path
                # below re-checks the specific table and is what Validate trusts;
                # create-on-write is CREATE IF NOT EXISTS, so guessing "new" here
                # is recoverable where refusing to answer would strand the run.
                out["table_exists"] = False
            _attach_db_sample(out, endpoint)
        return out

    if fmt == "mongodb":
        try:
            from pymongo.errors import PyMongoError
        except ImportError:
            out["message"] = "pymongo is not installed"
            return out
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
            requested_coll = endpoint.collection or endpoint.table
            if requested_coll:
                out["connected"] = True
                # find_one on a missing collection does NOT fail — it returns None
                # and Mongo creates the collection on first write. Use the real
                # name list so Transfer Studio can create-new for typed names.
                try:
                    colls = db.list_collection_names()
                except PyMongoError as list_err:
                    out["message"] = f"Could not list collections in `{db_name}`: {list_err}"
                    return out
                out["objects"] = [{"name": c, "type": "collection"} for c in colls[:200]]
                listed = _object_name_match(colls, requested_coll)
                if not listed:
                    out["table_exists"] = False
                    out["columns"] = []
                    out["schema"] = {}
                    purpose = str((endpoint.extra or {}).get("introspect_purpose") or "").lower()
                    if purpose == "source":
                        out["message"] = (
                            f"Collection `{requested_coll}` was not found in `{db_name}`. "
                            f"Check the name."
                        )
                    else:
                        out["auto_create"] = list(out.get("auto_create") or []) + [
                            f"Create collection `{requested_coll}` on first write"
                        ]
                        out["message"] = (
                            f"Collection `{requested_coll}` not found in `{db_name}` — "
                            f"it will be created automatically on first write"
                        )
                    return out
                # Canonical name from the server list (case / alias match).
                endpoint.collection = listed
                out["objects"] = [{"name": listed, "type": "collection"}] + [
                    o for o in out["objects"] if o.get("name") != listed
                ]
                out["table_exists"] = True
                out["message"] = f"MongoDB connected — reading `{listed}` in `{db_name}`"
                _attach_db_sample(out, endpoint)
            else:
                colls = db.list_collection_names()
                out["connected"] = True
                out["objects"] = [{"name": c, "type": "collection"} for c in colls[:50]]
                out["message"] = f"MongoDB connected — {len(colls)} collections in `{db_name}`"
        except PyMongoError as e:
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
            if not _mark_table_listed_if_present(out, endpoint.table):
                out["table_exists"] = False
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
            if not _mark_table_listed_if_present(out, endpoint.table):
                out["table_exists"] = False
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
            if not _mark_table_listed_if_present(out, endpoint.table):
                out["table_exists"] = False
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
            if not _mark_table_listed_if_present(out, endpoint.table):
                out["table_exists"] = False
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

    if fmt == "sftp":
        from connectors.sftp_common import test_sftp
        from connectors.sftp_reader import list_files

        ok, message = test_sftp(**cfg)
        out["connected"] = ok
        out["message"] = message
        if not ok:
            return out
        directory = str(cfg.get("database") or "") or "/"
        key = endpoint.table or endpoint.collection or ""
        try:
            names = list_files(cfg=cfg, directory=directory)
        except Exception as exc:
            # A directory we cannot list is unknown, never "the file is absent"
            # — that answer would flip Map into create-new over a live file and
            # overwrite it.
            out["objects"] = []
            out["sample_error"] = f"SFTP directory listing unavailable: {exc}"
            return out
        out["objects"] = [{"name": n, "type": "object"} for n in names[:200]]
        if key:
            # Existence is decided against the whole listing, not the 200 shown.
            out["table_exists"] = key in set(names)
            if out["table_exists"]:
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
        if probe.ok and (endpoint.table or endpoint.collection):
            # Sample the prefix for its document fields, as Mongo and
            # Elasticsearch already do. Without this the auto-mapper saw a
            # destination with no columns, mapped every source name to itself,
            # and the write then failed on fields the keyspace does not have —
            # even though the writer reads exactly these fields to type its bind.
            _attach_db_sample(out, endpoint)
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

    # SQLAlchemy-backed engines (sqlite, generic_sql, sqlserver, oracle, duckdb):
    # prove existence via information_schema / reflection — never leave sticky None
    # when the driver is live (Validate overwrite needs True/False, not unknown).
    _sql_driver = resolve_driver_type(fmt)
    if fmt == "sqlite" or _sql_driver in {
        "generic_sql",
        "sqlserver",
        "oracle",
        "duckdb",
    }:
        out["connected"] = True
        out["message"] = f"{fmt.title()} connected — introspecting table schema"
        if endpoint.table:
            # Do NOT pre-seed objects with the typed name — that forced
            # table_exists=True for missing tables via _mark_table_listed_if_present.
            out["table_exists"] = False
            _attach_db_sample(out, endpoint)
            return out
        # No table typed — list objects so Pilot / Destination pickers can choose.
        if fmt == "sqlite":
            try:
                from connectors.sqlite import test_sqlite

                cfg = resolve_connector_config(endpoint)
                probe = test_sqlite(
                    host=str(cfg.get("host") or ""),
                    port=int(cfg.get("port") or 0),
                    database=str(cfg.get("database") or ""),
                    username=str(cfg.get("username") or ""),
                    password=str(cfg.get("password") or ""),
                    schema=str(cfg.get("schema") or ""),
                    connection_string=str(cfg.get("connection_string") or ""),
                    ssl=bool(cfg.get("ssl")),
                )
                tables = [
                    t for t in (probe.tables or [])
                    if t and not str(t).startswith("(")
                ]
                out["objects"] = [{"name": t, "type": "table"} for t in tables]
                out["message"] = probe.message or f"SQLite connected — {len(tables)} tables"
                out["connected"] = bool(probe.ok)
                if not probe.ok and probe.error:
                    out["message"] = probe.error
            except Exception as exc:
                out["message"] = f"SQLite object list failed: {exc}"
            return out
        # Other SQLAlchemy engines: attempt reflection listing when no table typed.
        try:
            _attach_db_sample(out, endpoint)
        except Exception:
            pass
        return out

    if fmt in _SAAS_INTROSPECT_DRIVERS:
        return _saas_introspect(out, endpoint, cfg, fmt)

    out["message"] = f"Introspection for `{fmt}` not yet implemented"
    return out


def _object_name_match(names: list[str] | None, target: str | None) -> str | None:
    """Return the canonical listed name for ``target`` (case-insensitive), else None.

    Accepts bare ``jobs`` vs listed ``public.jobs`` / ``db.schema.jobs`` when the
    match is unambiguous — operators often type the table leaf while SHOW TABLES
    returns a qualified name (and vice versa).
    """
    want = (target or "").strip()
    if not want:
        return None
    want_l = want.lower()
    want_leaf = want_l.split(".")[-1]
    exact: list[str] = []
    qualified: list[str] = []
    leaf_only: list[str] = []
    for name in names or []:
        raw = (name or "").strip()
        if not raw or raw.startswith("("):
            continue
        raw_l = raw.lower()
        if raw_l == want_l:
            exact.append(raw)
            continue
        raw_leaf = raw_l.split(".")[-1]
        if raw_l.endswith("." + want_l) or want_l.endswith("." + raw_l):
            qualified.append(raw)
        elif raw_leaf == want_leaf:
            leaf_only.append(raw)
    if exact:
        return exact[0]
    if len(qualified) == 1:
        return qualified[0]
    if len(leaf_only) == 1:
        return leaf_only[0]
    return None


def _listed_object_names(out: dict) -> list[str]:
    return [str(o.get("name") or "") for o in (out.get("objects") or []) if isinstance(o, dict)]


def _mark_table_listed_if_present(out: dict, table: str | None) -> str | None:
    """If probe already listed ``table``, mark exists=True before schema sample."""
    canonical = _object_name_match(_listed_object_names(out), table)
    if canonical:
        out["table_exists"] = True
        out["message"] = out.get("message") or f"Found existing table `{canonical}`"
    return canonical


def _attach_db_sample(out: dict, endpoint: EndpointConfig, sample_limit: int = 100) -> None:
    """Bounded schema discovery — safe for million-row tables."""
    # Keep the raw format outside the try block so the error log can name the
    # driver even if connector resolution itself fails.
    fmt = (endpoint.format or "").lower()
    try:
        cfg = resolve_connector_config(endpoint)
        # Use the resolved saved-connector driver type if available, otherwise
        # fall back to the inline format string.
        fmt = (cfg.get("type") or endpoint.format or "").lower()

        if fmt == "mongodb":
            from services.schema_inference import infer_schema_map

            coll_name = endpoint.collection
            if not coll_name:
                return
            try:
                from pymongo.errors import PyMongoError

                # Reuse the cached MongoClient to avoid repeated connection handshakes
                # when the UI polls/retries introspection for the same connector.
                client = _mongo_client(mongodb_connection_string(cfg))
                db = client[endpoint.database or cfg["database"] or "test"]
                coll = db[coll_name]
                cursor = coll.find().max_time_ms(5000).limit(sample_limit)
                records = list(cursor)
            except PyMongoError as exc:
                out["message"] = f"Collection sample failed: {exc}"
                return
            # Match Execute path (mongodb_reader): expand nested docs + string
            # matrix via cell_to_string so Map/Validate columns ≡ write headers.
            from services.json_intelligence import expand_mongo_documents
            from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

            for doc in records:
                if isinstance(doc, dict) and "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            records = expand_mongo_documents(records, cfg=cfg)

            # Union keys across the sample (sparse nested fields mid-batch).
            columns: list[str] = []
            seen_cols: set[str] = set()
            for doc in records:
                if not isinstance(doc, dict):
                    continue
                for k in doc.keys():
                    if k not in seen_cols:
                        seen_cols.add(k)
                        columns.append(k)

            safe_records: list[dict[str, Any]] = []
            for doc in records:
                if not isinstance(doc, dict):
                    continue
                row: dict[str, Any] = {}
                for col in columns:
                    if col not in doc:
                        row[col] = DF_MISSING_SENTINEL
                    elif doc[col] is None:
                        row[col] = SQL_NULL_SENTINEL
                    else:
                        row[col] = cell_to_string(doc[col], preserve_sql_null=True)
                safe_records.append(row)

            # Canonical schema intelligence choke point (type + semantic_role).
            samples_by_field = {
                col: [
                    r[col]
                    for r in safe_records[:100]
                    if r.get(col) not in (None, "", DF_MISSING_SENTINEL, SQL_NULL_SENTINEL)
                ]
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
            # Keep Validate sample depth aligned with Execute preflight
            # (engine uses records[:100] for encoding / integrity). Truncating
            # to 10 here let Mongo→warehouse routes APPROVE on a clean preview
            # while Execute failed on U+200B / format-control rows later in the set.
            preview_n = max(1, min(int(sample_limit or 100), len(safe_records) or 1))
            out["sample_data"] = safe_records[:preview_n]
            out["data"] = safe_records[:preview_n]
            out["sample_row_count"] = len(records)
            try:
                from pymongo.errors import PyMongoError

                estimate = int(coll.estimated_document_count(maxTimeMS=5000)) if columns else 0
                out["row_estimate"] = estimate
                # Never treat the introspection sample size as the collection size.
                if estimate <= 0 and len(records) >= sample_limit:
                    out["row_estimate_uncertain"] = True
                    out["message"] = (
                        f"{out.get('message', '')} · row estimate unavailable "
                        f"(showing {len(records)}-row sample)"
                    ).strip(" ·")
            except PyMongoError:
                # Prefer unknown over lying that a 100-row sample is the full table.
                out["row_estimate"] = 0
                out["row_estimate_uncertain"] = True
                out["message"] = (
                    f"{out.get('message', '')} · estimated_document_count failed "
                    f"(showing {len(records)}-row sample)"
                ).strip(" ·")
            # Empty collections still exist — never treat "no sample docs" as create-new.
            out["table_exists"] = True
            if not columns:
                out["message"] = (
                    f"{out.get('message', '')} · collection `{coll_name}` exists "
                    f"but has no documents yet (schema pending until first write or sample)."
                ).strip(" ·")
            return

        if fmt == "redis":
            from connectors.redis_reader import read_keys_batch, resolve_key_pattern

            pattern = resolve_key_pattern(
                endpoint.table or endpoint.collection or endpoint.schema
            )
            result = read_keys_batch(cfg=cfg, pattern=pattern, offset=0, limit=sample_limit)
            batch = result[0] if isinstance(result, tuple) else result
            out["columns"] = batch.headers
            out["schema"] = _schema_from_batch(batch)
            out["row_estimate"] = (batch.total_rows or 0)
            # Redis namespaces are logical key prefixes — an empty SCAN is not
            # proof the destination is missing (would falsely flip Map create-new).
            out["table_exists"] = True
            if not (batch.total_rows or 0):
                out["message"] = (
                    f"{out.get('message', '')} · no keys match `{pattern}`; "
                    "Redis namespaces are logical prefixes, not tables."
                ).strip(" ·")
            _attach_batch_sample_rows(out, batch)
            return

        if fmt == "s3":
            from connectors.s3_reader import read_object

            bucket = cfg["database"]
            key = endpoint.table or endpoint.collection or ""
            if bucket and key:
                batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=sample_limit)
                out["columns"] = batch.headers
                out["schema"] = _schema_from_batch(batch)
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
                out["schema"] = _schema_from_batch(batch)
                out["row_estimate"] = (batch.total_rows or 0)
                out["table_exists"] = True
                _attach_batch_sample_rows(out, batch)
            return

        if fmt == "sftp":
            from connectors.sftp_reader import read_object

            directory = str(cfg.get("database") or "") or "/"
            key = endpoint.table or endpoint.collection or ""
            if key:
                batch = read_object(
                    cfg=cfg, bucket=directory, key=key, offset=0, limit=sample_limit
                )
                out["columns"] = batch.headers
                out["schema"] = _schema_from_batch(batch)
                out["row_estimate"] = batch.total_rows or 0
                out["table_exists"] = True
                _attach_batch_sample_rows(out, batch)
            return

        if fmt == "dynamodb":
            from connectors.dynamodb_reader import (
                describe_table_schema,
                estimate_item_count,
                read_all_paginated,
            )

            # Prefer table (object name) over database (often the AWS region).
            # Using database-first made Map/Validate sample the wrong Dynamo
            # table whenever operators put region in the database field.
            table = endpoint.table or endpoint.collection or endpoint.database
            if table:
                try:
                    names, schema_map = describe_table_schema(cfg, table)
                    out["columns"] = names
                    out["schema"] = schema_map
                    out["row_estimate"] = estimate_item_count(cfg, table)
                    out["table_exists"] = True
                except Exception as exc:
                    out["columns"] = []
                    out["schema"] = {}
                    # Only ResourceNotFound means create-new; auth/throttle/outage → unknown.
                    err_code = ""
                    resp = getattr(exc, "response", None)
                    if isinstance(resp, dict):
                        err_code = str((resp.get("Error") or {}).get("Code") or "")
                    if err_code == "ResourceNotFoundException" or "ResourceNotFoundException" in type(exc).__name__:
                        out["table_exists"] = False
                    else:
                        out["table_exists"] = None
                        out["sample_error"] = f"DynamoDB existence unknown: {exc}"
                        out["message"] = (
                            f"{out.get('message', '')} · DynamoDB describe failed: {exc}"
                        ).strip(" ·")
                # Always load a bounded item sample for Validate dry-run
                # (describe_table alone previously left sample_data empty).
                try:
                    batch = read_all_paginated(cfg, table, limit=sample_limit)
                    if batch.headers:
                        out["columns"] = out.get("columns") or batch.headers
                        if not out.get("schema"):
                            out["schema"] = _schema_from_batch(batch)
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
            from connectors.elasticsearch_reader import _client, read_index_batch

            index = endpoint.database or endpoint.table
            if index:
                exists: bool | None = None
                client = None
                try:
                    client = _client(cfg)
                    exists = bool(client.indices.exists(index=index))
                except Exception as exists_exc:
                    out["message"] = (
                        f"{out.get('message', '')} · Elasticsearch exists probe: {exists_exc}"
                    ).strip(" ·")
                    exists = None
                finally:
                    if client is not None:
                        try:
                            client.close()
                        except Exception as exc:
                            logging.getLogger(__name__).debug("Exception suppressed: %s", exc, exc_info=exc)
                result = read_index_batch(cfg=cfg, index=index, offset=0, limit=sample_limit)
                batch = result[0] if isinstance(result, tuple) else result
                out["columns"] = batch.headers
                out["schema"] = _schema_from_batch(batch)
                out["row_estimate"] = (batch.total_rows or 0)
                # Empty indexes still exist — row count must not drive create-new.
                if exists is True or (batch.headers and exists is not False):
                    out["table_exists"] = True
                elif exists is False and not batch.headers:
                    out["table_exists"] = False
                else:
                    out["table_exists"] = exists
                if exists is True and not (batch.total_rows or 0):
                    out["message"] = (
                        f"{out.get('message', '')} · index `{index}` exists but has no documents yet"
                    ).strip(" ·")
                _attach_batch_sample_rows(out, batch)
            return

        table = endpoint.table or endpoint.collection
        if not table:
            out["table_exists"] = False
            return
        listed = _mark_table_listed_if_present(out, table) or _object_name_match(
            _listed_object_names(out), table
        )
        # Prefer the case-correct name from SHOW TABLES / information_schema list.
        resolve_table = listed or table
        purpose = str((endpoint.extra or {}).get("introspect_purpose") or "").lower()
        # Destination: stay in the operator-chosen DB/schema. Cross-namespace
        # "heal" invents Existing table when another DB on the host has the name.
        strict_namespace = purpose == "destination"
        # Always attempt column introspect — even when the schema-scoped probe list
        # missed the name (LIMIT 50, wrong default schema, cross-schema table).
        # Short-circuiting on table_exists=False falsely flipped Map into create-new
        # while writers still appended into the real table (e.g. railway.airports).
        # For destination, same-namespace-only (strict) — LIMIT miss still works.
        schema_map, schema_nulls, schema_keys = _introspect_table_schema_rich(
            fmt, cfg, resolve_table, [], strict_namespace=strict_namespace
        )
        if not schema_map and listed and listed != table:
            schema_map, schema_nulls, schema_keys = _introspect_table_schema_rich(
                fmt, cfg, table, [], strict_namespace=strict_namespace
            )
        if not schema_map and not listed:
            # Last chance: bare leaf name may live outside the connector schema
            # for *source* discovery only. Destination stays strict above.
            schema_map, schema_nulls, schema_keys = _introspect_table_schema_rich(
                fmt, cfg, table, [], strict_namespace=strict_namespace
            )
        if schema_map:
            out["columns"] = list(schema_map.keys())
            out["schema"] = schema_map
            out["schema_nullability"] = schema_nulls
            # Who fills a NOT NULL column when no mapping does — G14 refuses to
            # call a required column safe without this.
            out["schema_defaults"] = dict((schema_keys or {}).get("defaults") or {})
            out["identity_columns"] = list(
                (schema_keys or {}).get("identity_columns") or []
            )
            out["generated_columns"] = list(
                (schema_keys or {}).get("generated_columns") or []
            )
            out["primary_key_columns"] = list(
                (schema_keys or {}).get("primary_key_columns") or []
            )
            out["unique_keys"] = list((schema_keys or {}).get("unique_keys") or [])
            schema_warnings = [
                str(w) for w in ((schema_keys or {}).get("warnings") or []) if w
            ]
            if schema_warnings:
                # Preserve introspect honesty (BQ/Redshift/SF NOT ENFORCED) for
                # Validate — never drop advisory-key warnings at the adapter edge.
                existing = [str(w) for w in (out.get("warnings") or []) if w]
                for w in schema_warnings:
                    if w not in existing:
                        existing.append(w)
                out["warnings"] = existing
            out["table_exists"] = True
            if not listed:
                # Heal the objects list so Destination/Map pickers see the table.
                leaf = str(table).split(".")[-1]
                out["objects"] = [{"name": leaf, "type": "table"}] + [
                    o for o in (out.get("objects") or []) if isinstance(o, dict) and o.get("name") != leaf
                ]
            # Prefer advisory catalog message when present; else table-found note.
            if schema_warnings and not out.get("message"):
                out["message"] = schema_warnings[0]
            else:
                out["message"] = out.get("message") or f"Found existing table `{resolve_table}`"
            # Schema-only is not enough for Validate dry-run — fetch a bounded
            # sample so Transfer Studio can run transform integrity checks.
            _attach_sql_sample_rows(out, endpoint, cfg, fmt, resolve_table, sample_limit)
        elif listed or out.get("table_exists") is True:
            # Table is on the probe list but column metadata failed (permissions,
            # transient error, case fold). Do NOT tell the operator it is missing.
            out["table_exists"] = True
            out["columns"] = out.get("columns") or []
            out["schema"] = out.get("schema") or {}
            out["message"] = (
                f"Table `{resolve_table}` exists on the destination, but column "
                f"metadata could not be loaded — retrying via sample SELECT."
            )
            # Recover columns from a bounded SELECT — same path writers use.
            _attach_sql_sample_rows(out, endpoint, cfg, fmt, resolve_table, sample_limit)
            if out.get("columns"):
                out["message"] = (
                    f"Found existing table `{resolve_table}` "
                    f"({len(out['columns'])} columns via sample read)"
                )
            else:
                out["message"] = (
                    f"Table `{resolve_table}` exists on the destination, but column "
                    f"metadata could not be loaded — mapping will use source types until retry."
                )
        else:
            # Missing object: wording depends on whether this endpoint is a source
            # (Transfer Studio introspect) or a destination (create-on-write is OK).
            out["columns"] = []
            out["schema"] = {}
            out["table_exists"] = False
            purpose = str((endpoint.extra or {}).get("introspect_purpose") or "").lower()
            if purpose == "source":
                # Never imply the source will be written / auto-created.
                out["message"] = (
                    f"Table `{table}` was not found on this source. "
                    f"Check the name (and schema/database)."
                )
            else:
                # Destination (or legacy callers): CREATE IF NOT EXISTS on first write.
                out["auto_create"] = list(out.get("auto_create") or []) + [
                    f'CREATE TABLE IF NOT EXISTS "{table}" (from source schema on first write)'
                ]
                out["message"] = (
                    f"Table `{table}` not found — it will be created automatically on first write"
                )
    except Exception as e:
        # Soft-fail sample/schema read. Never wipe an explicit False (new table →
        # create-on-write) or True (listed) into null — that left Map stuck on
        # "Destination schema unavailable" for brand-new tables.
        if out.get("table_exists") not in (True, False):
            out["table_exists"] = None
        out["columns"] = out.get("columns") or []
        out["schema"] = out.get("schema") or {}
        out["message"] = f"{out.get('message', '')} · schema probe: {e}".strip(" ·")
        logger.warning(
            "schema probe failed for %s table=%s: %s", fmt, endpoint.table, e, exc_info=e
        )


#: SaaS drivers whose Describe is already modelled in ``schema_introspect``.
#: They declared ``introspect: True`` and answered "not yet implemented" here,
#: so a Salesforce or HubSpot *destination* could never prove its object exists
#: and every route into one failed G2 on unknown existence. The metadata was
#: written and reachable; only this dispatch was missing.
_SAAS_INTROSPECT_DRIVERS = frozenset({"salesforce", "hubspot"})


def _saas_introspect(
    out: dict, endpoint: EndpointConfig, cfg: dict, fmt: str
) -> dict:
    """Describe-backed introspect for SaaS objects, via the canonical helper."""
    from services.schema_introspect import introspect_schema

    obj = endpoint.table or endpoint.collection or endpoint.database or ""
    try:
        info = introspect_schema(
            fmt,
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 443),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            api_key=str(cfg.get("api_key") or ""),
            table=obj,
        )
    except Exception as exc:
        out["message"] = f"{fmt.title()} introspect failed: {exc}"
        return out

    objects = [str(t) for t in (info.get("tables") or []) if t]
    if not info.get("ok"):
        error = str(info.get("error") or f"{fmt.title()} introspect failed")
        # Describe failing because the object is absent is a different answer
        # from Describe failing because the session cannot read it. Only the
        # first is proof, and neither means "create it" — a SaaS object cannot
        # be created by a transfer.
        if objects and obj and obj not in set(objects):
            out["connected"] = True
            out["table_exists"] = False
            out["objects"] = [{"name": name, "type": "object"} for name in objects[:200]]
            out["message"] = (
                f"`{obj}` is not an object on this {fmt.title()} org. "
                "Check the API name — objects cannot be created by a transfer."
            )
            return out
        out["connected"] = bool(objects)
        out["message"] = error
        return out

    out["connected"] = True
    out["objects"] = [{"name": name, "type": "object"} for name in objects[:200]]
    out["message"] = f"{fmt.title()} connected — {len(objects)} object(s)"
    if not obj:
        return out

    columns = [c for c in (info.get("columns") or []) if c.get("name")]
    # Existence is decided by the object list, not by whether Describe returned
    # columns: an object that exists but describes empty is still not create-new.
    out["table_exists"] = bool(columns) or obj in set(objects)
    out["columns"] = [str(c["name"]) for c in columns]
    out["schema"] = {
        str(c["name"]): str(c.get("inferred_type") or "VARCHAR") for c in columns
    }
    out["column_nullability"] = {
        str(c["name"]): bool(c.get("nullable", True)) for c in columns
    }
    for key in ("primary_key_columns", "unique_keys"):
        if info.get(key):
            out[key] = info[key]
    return out


def _schema_from_batch(batch: Any) -> dict[str, str]:
    """Column types for a source that has no catalog to declare them.

    Object stores, Redis and index sinks answer "what columns are here" from the
    payload itself, so a bare ``string`` per header is a placeholder, not a
    declaration. It was treated as one downstream: ``endpoint_source_column_types``
    hands this to ``reconcile_source_types`` as the *declared* schema, which
    outranks the reader's own inference by design — right for a relational
    catalog, wrong here, and the result was that an S3 CSV landed three ``text``
    columns where the identical upload landed ``bigint``/``numeric``/``date``.

    Readers that can type their rows report it through ``meta['native_types']``.
    Where a reader cannot, the placeholder stands and nothing changes.
    """
    headers = list(getattr(batch, "headers", None) or [])
    meta = getattr(batch, "meta", None)
    native = meta.get("native_types") if isinstance(meta, dict) else None
    if isinstance(native, dict) and native:
        return {c: str(native.get(c) or "string") for c in headers}
    return {c: "string" for c in headers}


def _attach_batch_sample_rows(out: dict, batch: Any, *, preview: int = 100) -> None:
    """Attach JSON-safe sample rows from a ReadBatch for Validate dry-run.

    Default 100 matches Execute's preflight integrity window so encoding /
    format-control findings cannot hide behind a 10-row preview.
    """
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
        # Cap preview reads to the same window Execute uses for preflight integrity.
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
        for row in records[:limit]:
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
            from services.dialect_profiles import default_schema_for, uses_schema

            # MySQL/MariaDB: database is the namespace — never show empty `.jobs`.
            if uses_schema(db):
                sch = destination.schema or default_schema_for(db) or ""
                qual = f"{sch}.{target}" if sch else target
            else:
                db_name = destination.database or dest_info.get("schema") or ""
                qual = f"{db_name}.{target}" if db_name else target
            # Only advertise CREATE when the destination table is actually missing.
            if dest_info.get("table_exists"):
                plan["auto_create"].append(f"{db} table `{qual}` (existing — append/map to current columns)")
            else:
                plan["auto_create"].append(
                    f"{db} table `{qual}` with typed columns (CREATE IF NOT EXISTS)"
                )
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    if columns:
        try:
            from services.mapping_pipeline import run_mapping_pipeline

            dest_cols = plan.get("destination", {}).get("columns") or []
            from services.data_profiler import source_types_are_authoritative

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
                source_types_authoritative=source_types_are_authoritative(
                    source.kind or "", src_fmt or ""
                ),
            )
            plan["mapping_preview"] = preview["mappings"][:20]
            plan["mapping_agents"] = preview.get("agents_used", [])
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
