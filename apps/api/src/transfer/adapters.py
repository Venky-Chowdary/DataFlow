"""Read/write adapters for universal transfer — files, databases, warehouses."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Legacy connectors live under apps/api/
_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

try:
    from services.file_parser import FileParser
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.file_parser import FileParser

try:
    from services.mongodb_service import get_mongodb_service
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.mongodb_service import get_mongodb_service

try:
    from services.value_serializer import cell_to_string, json_default
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.value_serializer import cell_to_string, json_default

from connectors.sql_dsn import is_masked_secret, sync_credentials_into_connection_string

from .connector_registry import run_probe
from .models import EndpointConfig
from .type_mapper import ddl_carrier_type, ddl_type, normalize_inferred


class FileExportMapBlocked(ValueError):
    """Map/Risk Contract blocked file export — carry quarantine for DLQ persist."""

    def __init__(
        self,
        message: str,
        *,
        rejected_details: list[dict[str, Any]] | None = None,
        rejected_rows: int = 0,
    ) -> None:
        super().__init__(message)
        self.rejected_details = list(rejected_details or [])
        self.rejected_rows = int(
            rejected_rows if rejected_rows else len(self.rejected_details)
        )


class WriteBatchBlocked(RuntimeError):
    """Writer aborted a batch — carry full quarantine for DLQ before job fail.

    Stream/engine must persist ``rejected_details`` before treating the transfer
    as a bare RuntimeError (silent DLQ loss on FAIL_JOB / mid-write abort).
    """

    def __init__(
        self,
        message: str,
        *,
        rejected_details: list[dict[str, Any]] | None = None,
        rejected_rows: int = 0,
        rows_written: int = 0,
        dest_summary: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.rejected_details = list(rejected_details or [])
        self.rejected_rows = int(
            rejected_rows if rejected_rows else len(self.rejected_details)
        )
        self.rows_written = int(rows_written or 0)
        self.dest_summary = dict(dest_summary or {})


def resolve_dest_table(
    dest_type: str, destination: EndpointConfig, fallback_name: str = "import"
) -> str:
    """Resolve destination object name — table, key, index, or collection."""
    base = re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        (destination.table or destination.collection or fallback_name).lower(),
    )[:40]
    dt = dest_type.lower()
    if dt == "dynamodb":
        return (
            destination.table
            or destination.collection
            or destination.database
            or f"dt_{base}"
        )
    if dt in ("s3", "gcs"):
        return (
            destination.table
            or destination.collection
            or destination.schema
            or f"exports/dt_{base}.json"
        )
    if dt == "elasticsearch":
        # Elasticsearch uses an index name; prefer the table name (UI's destination name)
        # and fall back to the database field when it is intentionally supplied.
        return (
            destination.table
            or destination.collection
            or destination.database
            or f"dt_{base}"
        )
    if dt == "kafka":
        return (
            destination.table
            or destination.collection
            or destination.database
            or f"dataflow.{base}"
        )
    if dt in ("iceberg", "salesforce", "hubspot"):
        # Preserve case for CRM objects / lakehouse table identifiers.
        return destination.table or destination.collection or f"dt_{base}"
    return destination.table or destination.collection or f"dt_{base}"


def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    coerced = int(getattr(result, "coerced_null_rows", 0) or 0)
    skipped = int(getattr(result, "rows_skipped", 0) or 0)
    warnings = list(getattr(result, "warnings", []) or [])
    # GA: never truncate rejected_details before quarantine / proof harvest.
    rejected_details = list(getattr(result, "rejected_details", []) or [])
    out: dict[str, Any] = {
        "rejected_rows": rejected,
        "coerced_null_rows": coerced,
        "rows_skipped": skipped,
        "rejected_details": rejected_details,
        "rejected_details_sample": rejected_details[:200],
        "warnings": warnings[:10],
        "error_policy": "quarantine" if (rejected or coerced) else "none",
    }
    meta = getattr(result, "meta", None)
    if isinstance(meta, dict) and meta:
        # Promote Gate-8 sample / identity stamps into destination_summary.
        for key in ("reconcile_sample", "written_ids", "source_row_count"):
            if key in meta and meta[key] is not None:
                out[key] = meta[key]
    return out


def raise_writer_failure(result: Any, label: str) -> None:
    """Raise :class:`WriteBatchBlocked` so DLQ details survive job failure."""
    err = getattr(result, "error", None) or label
    written = int(getattr(result, "rows_written", 0) or 0)
    details = list(getattr(result, "rejected_details", []) or [])
    rejected_rows = int(getattr(result, "rejected_rows", 0) or 0) or len(details)
    summary = _writer_diagnostics(result)
    try:
        from connectors.write_resilience import is_connection_lost
    except ImportError:
        is_connection_lost = lambda _e: False  # noqa: E731
    if is_connection_lost(err):
        lost = ConnectionError(err)
        setattr(lost, "rejected_details", details)
        setattr(lost, "rejected_rows", rejected_rows)
        setattr(lost, "rows_written", written)
        setattr(lost, "dest_summary", summary)
        raise lost
    msg = (
        f"partial write ({written} rows committed before failure): {err}"
        if written > 0
        else str(err)
    )
    raise WriteBatchBlocked(
        msg,
        rejected_details=details,
        rejected_rows=rejected_rows,
        rows_written=written,
        dest_summary=summary,
    )


def _apply_vector_extra(common: dict[str, Any], endpoint: EndpointConfig) -> None:
    """Forward Studio Advanced vector fields from endpoint.extra into writer kwargs."""
    extra = endpoint.extra or {}
    common["content_column"] = extra.get("content_column")
    common["embedding_column"] = extra.get("embedding_column")
    common["metadata_columns"] = extra.get("metadata_columns")
    common["exclude_pii_columns"] = extra.get("exclude_pii_columns")
    common["embedding_model"] = extra.get("embedding_model")
    common["chunk_size"] = (
        int(extra.get("chunk_size", 512)) if extra.get("chunk_size") else 512
    )
    common["chunk_overlap"] = (
        int(extra.get("chunk_overlap", 50)) if extra.get("chunk_overlap") else 50
    )
    common["skip_chunking"] = bool(extra.get("skip_chunking"))
    if "durable_embedding_cache" in extra:
        common["durable_embedding_cache"] = bool(extra.get("durable_embedding_cache"))


def parse_file_content(
    content: bytes,
    filename: str,
    *,
    enable_ocr: bool = False,
) -> tuple[list[dict], list[str], dict[str, str]]:
    result = FileParser.parse(content, filename, enable_ocr=enable_ocr)
    if not result.success:
        raise ValueError(result.error or "File parse failed")
    schema = FileParser.infer_schema(result.data)
    return result.data, result.columns, schema


def parse_file_route_sample(
    content: bytes,
    filename: str,
    preview_rows: int = 200,
    *,
    enable_ocr: bool = False,
) -> tuple[list[str], dict[str, str], int]:
    """Headers + schema for route analysis without loading entire files."""
    ftype = FileParser.detect_file_type(filename, content)
    if ftype in ("csv", "tsv"):
        from services.csv_profiler import (
            count_csv_rows,
            detect_encoding,
            parse_csv_preview,
        )

        enc = detect_encoding(content)
        headers, rows, _enc, _delim = parse_csv_preview(
            content, encoding=enc, preview_rows=preview_rows
        )
        if not headers:
            raise ValueError("CSV/TSV has no header row")
        records = [
            {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
            for row in rows[:preview_rows]
        ]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {h: "string" for h in headers}
        )
        return headers, schema, count_csv_rows(content, enc)

    result = FileParser.parse(content, filename, enable_ocr=enable_ocr)
    if not result.success:
        raise ValueError(result.error or "File parse failed")
    sample = result.data[:preview_rows]
    schema = (
        FileParser.infer_schema(sample)
        if sample
        else {c: "string" for c in result.columns}
    )
    return result.columns, schema, result.row_count


def _matrix_cell(value: Any) -> Any:
    # Preserve SQL NULL / missing distinctly from the literal empty string so
    # downstream writers can tell the difference.
    from services.value_serializer import DF_MISSING_SENTINEL, is_missing_sentinel

    if value is None:
        return None
    # STOP_COLUMN / sparse CDC — never collapse DF_MISSING to "" via cell_to_string.
    if is_missing_sentinel(value):
        return DF_MISSING_SENTINEL
    return cell_to_string(value)


def records_to_matrix(
    records: list[dict], columns: list[str]
) -> tuple[list[str], list[list[Any]]]:
    from services.value_serializer import DF_MISSING_SENTINEL

    headers = columns or (list(records[0].keys()) if records else [])
    rows: list[list[Any]] = []
    for rec in records:
        row: list[Any] = []
        for h in headers:
            # Absent key = omit-from-SET (same contract as cdc_transfer._records_to_matrix).
            if h not in rec:
                row.append(DF_MISSING_SENTINEL)
            else:
                row.append(_matrix_cell(rec.get(h)))
        rows.append(row)
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

    from connectors.mongodb_common import _mongo_client

    connection_string = (cfg.get("connection_string") or "").strip()
    qs = parse_qs(urlparse(connection_string).query, keep_blank_values=True)
    url_auth_source = qs.get("authSource", qs.get("authsource", [""]))[0]
    database = (cfg.get("database") or "").strip()

    candidates: list[str] = []
    if cfg.get("auth_source"):
        candidates.append(str(cfg.get("auth_source")).strip())
    if url_auth_source:
        candidates.append(url_auth_source)
    # Prefer admin before the app database — managed Mongo (Railway/Atlas)
    # almost always defines the user in admin, while the app DB is the path.
    candidates.append("admin")
    if database and database != "admin":
        candidates.append(database)

    # Deduplicate while preserving order.
    seen: set[str] = set()
    ordered: list[str] = []
    for auth_source in candidates:
        key = (auth_source or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(key)

    last_error = ""
    for auth_source in ordered:
        try:
            conn_str = mongodb_connection_string({**cfg, "auth_source": auth_source})
            client = _mongo_client(conn_str)
            client.admin.command("ping")
            cfg["auth_source"] = auth_source
            return True, f"MongoDB reachable (authSource={auth_source})"
        except Exception as exc:
            last_error = str(exc)
    return False, last_error


def _find_implicit_connector_id(
    endpoint: EndpointConfig,
    cfg: dict[str, Any],
    driver: str,
    workspace_id: str | None = None,
) -> str | None:
    """When the endpoint has no explicit connector_id or credentials, resolve
    the single saved connector of the same type in the workspace.  This prevents
    silent fallbacks to ``localhost`` (e.g. SQLite writing a file named
    ``localhost``) and makes sandbox/test transfers that create one connector
    work without manually wiring ``connector_id``.
    """
    if endpoint.connector_id:
        return None
    has_creds = bool(
        cfg.get("host")
        or (cfg.get("database") or "").strip()
        or (cfg.get("connection_string") or "").strip()
    )
    if has_creds:
        return None
    try:
        from services.connector_store import list_connectors

        candidates = [
            c for c in list_connectors(workspace_id=workspace_id) if c.type == driver
        ]
        if len(candidates) == 1:
            return candidates[0].id
    except Exception as exc:
        logger.debug("implicit connector resolution failed: %s", exc, exc_info=exc)
    return None


def resolve_connector_config(
    endpoint: EndpointConfig, workspace_id: str | None = None
) -> dict[str, Any]:
    """Merge saved connector with inline overrides."""
    from .connector_capabilities import resolve_driver_type

    driver = resolve_driver_type(endpoint.format or "")
    fmt = driver
    default_port = (
        27017
        if fmt == "mongodb"
        else 3306
        if fmt == "mysql"
        else 1433
        if fmt == "sqlserver"
        else 1521
        if fmt == "oracle"
        else 9092
        if fmt == "kafka"
        else 6379
        if fmt == "redis"
        else 9200
        if fmt == "elasticsearch"
        else 5439
        if fmt == "redshift"
        else 0
        if fmt in ("sqlite", "generic_sql", "iceberg")
        else 22
        if fmt == "sftp"
        else 587
        if fmt == "email"
        else 6333
        if fmt == "qdrant"
        else 8080
        if fmt == "weaviate"
        else 19530
        if fmt == "milvus"
        else 443
        if fmt
        in (
            "snowflake",
            "bigquery",
            "dynamodb",
            "s3",
            "gcs",
            "adls",
            "salesforce",
            "hubspot",
            "stripe",
            "shopify",
            "zendesk",
            "notion",
            "airtable",
            "rest_api",
            "influxdb",
            "neo4j",
            "couchbase",
            "pinecone",
        )
        else 5432
    )
    from services.dialect_profiles import normalize_schema

    # Start with inline endpoint values only; driver defaults are applied after the
    # saved connector is merged so saved credentials can fill missing fields.
    cfg: dict[str, Any] = {
        "host": endpoint.host or "",
        "port": endpoint.port or 0,
        "database": endpoint.database or "",
        "schema": endpoint.schema or "",
        "table": endpoint.table or "",
        "table_name": endpoint.table or "",
        "username": endpoint.username,
        "password": endpoint.password,
        "connection_string": endpoint.connection_string or "",
        "warehouse": endpoint.warehouse or "",
        "ssl": endpoint.ssl,
        "type": endpoint.format,
        "auth_mode": endpoint.auth_mode or "",
        "auth_role": endpoint.auth_role or "",
        "auth_source": endpoint.auth_source or "",
        "api_key": endpoint.api_key or "",
        "service_account": endpoint.service_account or "",
        "private_key": endpoint.private_key or "",
        "endpoint_url": endpoint.endpoint_url or "",
        "path_style": endpoint.path_style,
        "region": endpoint.region or "",
    }
    # Keep "role" as the canonical key used by Snowflake connector functions.
    cfg["role"] = endpoint.auth_role or ""
    cfg.update(endpoint.extra)
    connector_id = endpoint.connector_id or _find_implicit_connector_id(
        endpoint, cfg, fmt, workspace_id=workspace_id
    )
    if connector_id:
        conn_dict = _lookup_saved_connector(connector_id, workspace_id=workspace_id)
        if not conn_dict:
            raise ValueError(f"Connector {connector_id} not found")
        from services.destination_identity import resolve_saved_vs_inline

        identity = resolve_saved_vs_inline(cfg, conn_dict, fmt=fmt)
        chosen_database = identity.database

        def _pick(inline: Any, saved: Any, sensitive: bool = False) -> Any:
            if sensitive and is_masked_secret(inline):
                return saved if saved is not None else ""
            return (
                inline
                if inline not in (None, "", 0)
                else (saved if saved is not None else "")
            )

        merged_cfg = {
            "host": _pick(cfg.get("host"), conn_dict.get("host")),
            "port": _pick(cfg.get("port"), conn_dict.get("port")),
            "database": chosen_database,
            "schema": _pick(cfg.get("schema"), conn_dict.get("schema")),
            "username": _pick(cfg.get("username"), conn_dict.get("username")),
            "password": _pick(
                cfg.get("password"), conn_dict.get("password"), sensitive=True
            ),
            "connection_string": _pick(
                cfg.get("connection_string"),
                conn_dict.get("connection_string"),
                sensitive=True,
            ),
            "warehouse": _pick(cfg.get("warehouse"), conn_dict.get("warehouse")),
            "ssl": conn_dict.get("ssl")
            if cfg.get("ssl") is None or cfg.get("ssl") is False
            else cfg.get("ssl"),
            "type": conn_dict.get("type") or endpoint.format or "",
            "auth_mode": _pick(cfg.get("auth_mode"), conn_dict.get("auth_mode")),
            "auth_role": _pick(cfg.get("auth_role"), conn_dict.get("auth_role")),
            "auth_source": _pick(cfg.get("auth_source"), conn_dict.get("auth_source")),
            "api_key": _pick(
                cfg.get("api_key"), conn_dict.get("api_key"), sensitive=True
            ),
            "service_account": _pick(
                cfg.get("service_account"),
                conn_dict.get("service_account"),
                sensitive=True,
            ),
            "private_key": _pick(
                cfg.get("private_key"), conn_dict.get("private_key"), sensitive=True
            ),
            "endpoint_url": _pick(
                cfg.get("endpoint_url"), conn_dict.get("endpoint_url")
            ),
            # EndpointConfig defaults path_style=False; do not clobber a saved
            # MinIO connector that requires path-style addressing.
            "path_style": (
                True if cfg.get("path_style") else bool(conn_dict.get("path_style"))
            ),
            "region": _pick(cfg.get("region"), conn_dict.get("region")),
            "role": _pick(cfg.get("role"), conn_dict.get("role")),
        }
        # Preserve any extra keys from the inline endpoint or saved connector.
        for key, value in {**cfg, **conn_dict}.items():
            if key not in merged_cfg:
                merged_cfg[key] = value
        merged_cfg["destination_identity"] = identity.as_dict()
        sync_credentials_into_connection_string(merged_cfg)
        cfg = merged_cfg
    # Stamp connector_id so CDC fingerprints / incremental snapshots match adapters.
    if connector_id:
        cfg["connector_id"] = connector_id
    # Apply driver defaults for fields that are still missing.
    # Never invent localhost when a connection string already carries host/auth —
    # that caused Validate to AUTH against the wrong endpoint while Connectors Test
    # still passed via the URI.
    if not (cfg.get("connection_string") or "").strip():
        cfg["host"] = cfg["host"] or "localhost"
    else:
        cfg["host"] = cfg.get("host") or ""
    cfg["port"] = cfg["port"] or default_port
    driver_type = (cfg.get("type") or fmt or "").lower()
    # Always resolve against the *merged* driver — never the pre-merge fmt default alone.
    cfg["schema"] = normalize_schema(
        driver_type,
        cfg.get("schema"),
        username=str(cfg.get("username") or "") or None,
    )
    if fmt == "mongodb" and not cfg.get("database"):
        from connectors.mongodb_common import mongodb_database_from_uri

        cfg["database"] = (
            mongodb_database_from_uri(cfg.get("connection_string", "")) or ""
        )
    # Ensure generic SQLAlchemy paths (introspection, schema drift, duplicate-key
    # probes) use the same credentials as the explicit user/pass fields.
    sync_credentials_into_connection_string(cfg)
    return cfg


def resolve_endpoint(
    endpoint: EndpointConfig, workspace_id: str | None = None
) -> EndpointConfig:
    """Return a new EndpointConfig with saved-connector fields resolved.

    This is the EndpointConfig equivalent of ``resolve_connector_config`` and is
    the single place where a ``connector_id`` is expanded into host/port/credentials.
    """
    from .models import endpoint_to_dict

    cfg = resolve_connector_config(endpoint, workspace_id=workspace_id)
    merged = endpoint_to_dict(endpoint)
    merged.update(cfg)
    # ``EndpointConfig.from_dict`` expects ``format``; ``resolve_connector_config``
    # uses ``type`` as the canonical driver key.  When a saved connector exists,
    # its stored driver type is authoritative; otherwise keep the inline format.
    merged["format"] = merged.get("type") or merged.get("format") or endpoint.format
    return EndpointConfig.from_dict(endpoint.kind, merged)


def _lookup_saved_connector(
    connector_id: str, workspace_id: str | None = None
) -> dict[str, Any] | None:
    """Find a saved connector in the file-backed store, falling back to MongoDB."""
    try:
        from services.connector_store import get_connector as fs_get

        conn = fs_get(connector_id, workspace_id=workspace_id)
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
                "private_key": getattr(conn, "private_key", ""),
                "endpoint_url": getattr(conn, "endpoint_url", ""),
                "path_style": getattr(conn, "path_style", False),
                "role": getattr(conn, "auth_role", ""),
            }
    except Exception as exc:
        logger.debug("connector store lookup failed: %s", exc, exc_info=exc)
    try:
        mongo = get_mongodb_service()
        return mongo.get_connector(connector_id)
    except Exception as exc:
        logger.debug("mongodb connector lookup failed: %s", exc, exc_info=exc)
        return None


def _columns_type_and_nullability(
    columns: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, bool]]:
    """Split introspect column dicts into type map + nullable map."""
    types, nulls, _defaults, _ident, _gen, _coll = _columns_schema_meta(columns)
    return types, nulls


def _columns_schema_meta(
    columns: list[dict[str, Any]],
) -> tuple[
    dict[str, str],
    dict[str, bool],
    dict[str, str],
    list[str],
    list[str],
    dict[str, str],
]:
    """Type / nullability / defaults / identity / generated / collation maps."""
    types: dict[str, str] = {}
    nulls: dict[str, bool] = {}
    defaults: dict[str, str] = {}
    identity: list[str] = []
    generated: list[str] = []
    collations: dict[str, str] = {}
    for col in columns:
        name = col.get("name")
        if not name:
            continue
        key = str(name)
        types[key] = str(col.get("inferred_type") or "TEXT")
        if "nullable" in col:
            nulls[key] = bool(col["nullable"])
        dflt = col.get("default")
        if dflt is not None and str(dflt).strip() != "":
            defaults[key] = str(dflt)
        if col.get("is_identity"):
            identity.append(key)
        inferred_u = str(col.get("inferred_type") or "").upper()
        if "GENERATED ALWAYS" in inferred_u or str(col.get("generation") or "").lower() == "always":
            if key not in generated:
                generated.append(key)
        coll = str(col.get("collation") or "").strip()
        if coll:
            collations[key] = coll
    return types, nulls, defaults, identity, generated, collations


def _introspect_table_schema_rich(
    db_type: str,
    cfg: dict[str, Any],
    table: str,
    headers: list[str],
    records: list[dict] | None = None,
    *,
    strict_namespace: bool = False,
) -> tuple[dict[str, str], dict[str, bool], dict[str, Any]]:
    """Load column types + nullability + unique keys from INFORMATION_SCHEMA.

    ``strict_namespace`` is required for destination probes so missing tables in
    the chosen DB/schema are not "healed" from another namespace on the host.
    Nullability feeds G3 NOT NULL contracts — never invent nullable=True when
    the catalog says otherwise.
    Third return value carries ``primary_key_columns`` / ``unique_keys`` when the
    catalog exposes them (PG/MySQL today), plus Property 6 defaults/identity.
    """
    empty_keys: dict[str, Any] = {
        "primary_key_columns": [],
        "unique_keys": [],
        "defaults": {},
        "identity_columns": [],
        "generated_columns": [],
        "collations": {},
        "charsets": {},
        "physical_storage": None,
        "check_constraints_meta": None,
        "indexes_meta": None,
        "warnings": [],
    }

    def _keys_from_info(
        payload: dict[str, Any],
        *,
        defaults: dict[str, str] | None = None,
        identity_columns: list[str] | None = None,
        generated_columns: list[str] | None = None,
        collations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        warnings = [str(w) for w in (payload.get("warnings") or []) if w]
        # Catalog message often carries the advisory-key honesty note when
        # ``warnings`` was folded into ``message`` by introspect.
        msg = str(payload.get("message") or "").strip()
        if msg and any(
            token in msg.lower()
            for token in ("not enforced", "informational", "advisory")
        ):
            if msg not in warnings:
                warnings.append(msg)
        charsets: dict[str, str] = {}
        for col in payload.get("columns") or []:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            cs = str(col.get("charset") or "").strip()
            if name and cs:
                charsets[name] = cs
        return {
            "primary_key_columns": list(payload.get("primary_key_columns") or []),
            "unique_keys": list(payload.get("unique_keys") or []),
            "defaults": dict(defaults or {}),
            "identity_columns": list(identity_columns or []),
            "generated_columns": list(generated_columns or []),
            "collations": dict(collations or {}),
            "charsets": charsets,
            "foreign_keys": list(payload.get("foreign_keys") or []),
            "check_constraints": list(payload.get("check_constraints") or []),
            # None when the dialect/probe never measured placement — the
            # certificate reports "unknown", never "no partitioning".
            "physical_storage": payload.get("physical_storage"),
            # None when the CHECK catalog was never read — the certificate says
            # "unmeasured", never "no CHECK constraints".
            "check_constraints_meta": payload.get("check_constraints_meta"),
            # None when the index catalog was never read — the certificate says
            # "unmeasured", never "no secondary indexes".
            "indexes_meta": payload.get("indexes_meta"),
            "warnings": warnings,
        }

    if db_type == "generic_sql":
        try:
            from connectors.generic_sql import introspect_table_schema

            info = introspect_table_schema(cfg, table)
            if info.get("ok") and info.get("columns"):
                types, nulls, defaults, ident, gen, coll = _columns_schema_meta(
                    info["columns"]
                )
                return types, nulls, _keys_from_info(
                    info,
                    defaults=defaults,
                    identity_columns=ident,
                    generated_columns=gen,
                    collations=coll,
                )
        except Exception as exc:
            logger.debug("table schema introspection failed: %s", exc, exc_info=exc)

    from services.dialect_profiles import schema_from_cfg
    from services.schema_introspect import introspect_schema

    info = introspect_schema(
        db_type,
        host=cfg.get("host", ""),
        port=int(
            cfg.get("port")
            or (
                3306
                if db_type == "mysql"
                else 1433
                if db_type == "sqlserver"
                else 1521
                if db_type == "oracle"
                else 5439
                if db_type == "redshift"
                else 5432
            )
        ),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        schema=schema_from_cfg(db_type, cfg),
        connection_string=cfg.get("connection_string", ""),
        # Prefer connector ssl exactly as list/probe use it. Do not default True
        # when the key is missing — that caused managed vs local mismatch where
        # SHOW TABLES succeeded (ssl=False) but INFORMATION_SCHEMA failed (ssl=True),
        # leaving Map with "destination schema unavailable" for an existing table.
        ssl=bool(cfg.get("ssl", False)),
        warehouse=cfg.get("warehouse", ""),
        table=table,
        catalog_type=cfg.get("type", ""),
        auth_source=cfg.get("auth_source", ""),
        api_key=cfg.get("api_key", ""),
        strict_namespace=strict_namespace,
    )
    if info.get("ok") and info.get("columns"):
        types, nulls, defaults, ident, gen, coll = _columns_schema_meta(info["columns"])
        return types, nulls, _keys_from_info(
            info,
            defaults=defaults,
            identity_columns=ident,
            generated_columns=gen,
            collations=coll,
        )

    # Retry once with flipped SSL when the first probe failed (common when the
    # connector ssl flag does not match the host's TLS requirement).
    if not info.get("ok") and db_type in (
        "postgresql",
        "redshift",
        "mysql",
        "sqlserver",
    ):
        flipped = not bool(cfg.get("ssl", False))
        info_retry = introspect_schema(
            db_type,
            host=cfg.get("host", ""),
            port=int(
                cfg.get("port")
                or (
                    3306
                    if db_type == "mysql"
                    else 1433
                    if db_type == "sqlserver"
                    else 1521
                    if db_type == "oracle"
                    else 5439
                    if db_type == "redshift"
                    else 5432
                )
            ),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=schema_from_cfg(db_type, cfg),
            connection_string=cfg.get("connection_string", ""),
            ssl=flipped,
            warehouse=cfg.get("warehouse", ""),
            table=table,
            catalog_type=cfg.get("type", ""),
            auth_source=cfg.get("auth_source", ""),
            api_key=cfg.get("api_key", ""),
            strict_namespace=strict_namespace,
        )
        if info_retry.get("ok") and info_retry.get("columns"):
            types, nulls, defaults, ident, gen, coll = _columns_schema_meta(
                info_retry["columns"]
            )
            return types, nulls, _keys_from_info(
                info_retry,
                defaults=defaults,
                identity_columns=ident,
                generated_columns=gen,
                collations=coll,
            )

    # Fallback: infer logical types from the sample records we already have in hand.
    # This is essential for schemaless sources (MongoDB, DynamoDB, Redis) whose
    # stored values may be strings but whose content is numeric, boolean, JSON, etc.
    if records:
        try:
            from services.file_parser import FileParser

            inferred = FileParser.infer_schema(records)
            if inferred:
                return {h: inferred.get(h, "TEXT") for h in headers}, {}, empty_keys
        except Exception as exc:
            logger.debug("record schema inference failed: %s", exc, exc_info=exc)
    return {h: "TEXT" for h in headers}, {}, empty_keys


def _introspect_table_schema(
    db_type: str,
    cfg: dict[str, Any],
    table: str,
    headers: list[str],
    records: list[dict] | None = None,
    *,
    strict_namespace: bool = False,
) -> dict[str, str]:
    """Load column types from INFORMATION_SCHEMA or infer from sample records."""
    types, _nulls, _keys = _introspect_table_schema_rich(
        db_type,
        cfg,
        table,
        headers,
        records=records,
        strict_namespace=strict_namespace,
    )
    return types


_NON_STREAMING_ROW_LIMIT = 100_000


def _guard_truncated_read(batch, db_type: str, name: str) -> None:
    """Fail closed when a non-streaming read would silently drop rows."""
    total_rows = batch.total_rows or 0
    if total_rows > len(batch.rows):
        raise ValueError(
            f"Source {db_type}.{name} has {total_rows:,} rows but non-streaming reads "
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
    # Prefer the saved connector's driver type over any inline format string.
    db_type = resolve_driver_type(cfg.get("type") or endpoint.format or "")

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
        schema = _introspect_table_schema(
            "postgresql", cfg, table, batch.headers, records=records
        )
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
        schema = _introspect_table_schema(
            "mongodb", cfg, coll_name, batch.headers, records=records
        )
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
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
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
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
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
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
        return records, batch.headers, schema

    if db_type == "gcs":
        from connectors.gcs_reader import read_object

        bucket = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not bucket or not key:
            raise ValueError(
                "GCS source requires bucket (database) and object key (table/collection)"
            )
        batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type == "adls":
        from connectors.adls_reader import read_object

        container = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not container or not key:
            raise ValueError(
                "Azure Blob source requires container (database) and blob key (table/collection)"
            )
        batch = read_object(cfg=cfg, bucket=container, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type == "s3":
        from connectors.s3_reader import read_object

        bucket = cfg["database"]
        key = endpoint.table or endpoint.collection or endpoint.schema or ""
        if not bucket or not key:
            raise ValueError(
                "S3 source requires bucket (database) and object key (table/collection)"
            )
        batch = read_object(cfg=cfg, bucket=bucket, key=key, offset=0, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, key)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
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
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type == "elasticsearch":
        from connectors.elasticsearch_reader import read_index_batch

        index = endpoint.table or endpoint.database or endpoint.collection
        if not index:
            raise ValueError(
                "Elasticsearch index name required (table or database field)"
            )
        batch, _ = read_index_batch(cfg=cfg, index=index, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, index)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type == "redis":
        from connectors.redis_reader import read_keys_batch, resolve_key_pattern

        pattern = resolve_key_pattern(
            endpoint.table or endpoint.collection or endpoint.schema
        )
        batch, _ = read_keys_batch(cfg=cfg, pattern=pattern, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, pattern)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
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
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
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
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
        return records, batch.headers, schema

    if db_type in ("sqlserver", "oracle"):
        from .connector_dispatch import read_via_registry

        table = endpoint.table
        if not table:
            raise ValueError(f"Source {db_type} table name required")
        batch = read_via_registry(db_type, cfg=cfg, table=table, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
        return records, batch.headers, schema

    if db_type == "iceberg":
        from .connector_dispatch import read_via_registry

        table = endpoint.table
        if not table:
            raise ValueError("Source Iceberg table name required")
        batch = read_via_registry("iceberg", cfg=cfg, table=table, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, table)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = _introspect_table_schema(
            db_type, cfg, table, batch.headers, records=records
        )
        return records, batch.headers, schema

    if db_type == "sftp":
        from connectors.sftp_reader import read_object

        if (
            not endpoint.table
            and not endpoint.connection_string
            and not endpoint.database
        ):
            raise ValueError(
                "SFTP source requires a remote file path (connection_string, database, or table field)"
            )
        batch = read_object(
            cfg=cfg,
            bucket=endpoint.database,
            key=endpoint.table,
            offset=0,
            limit=limit,
        )
        if raise_on_truncate:
            _guard_truncated_read(
                batch,
                db_type,
                endpoint.table or endpoint.database or endpoint.connection_string,
            )
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type in (
        "salesforce",
        "hubspot",
        "stripe",
        "shopify",
        "zendesk",
        "notion",
        "airtable",
        "rest_api",
        "influxdb",
        "neo4j",
        "couchbase",
    ):
        from connectors.saas_common import ReadBatch

        mod = __import__(f"connectors.{db_type}", fromlist=["read_object"])
        read_fn = getattr(mod, "read_object")
        sobject = endpoint.table or endpoint.database or endpoint.collection or ""
        batch: ReadBatch = read_fn(cfg=cfg, object=sobject, limit=limit)
        if raise_on_truncate:
            _guard_truncated_read(batch, db_type, sobject or db_type)
        records = [dict(zip(batch.headers, row)) for row in batch.rows]
        schema = (
            FileParser.infer_schema(records)
            if records
            else {c: "string" for c in batch.headers}
        )
        return records, batch.headers, schema

    if db_type == "singer_tap":
        from connectors.sdk import sdk_read_as_matrix

        stream = endpoint.table or endpoint.collection or "stream"
        result = sdk_read_as_matrix(
            "singer_tap",
            cfg,
            stream,
            offset=0,
            limit=limit or 1000,
        )
        headers, rows, schema = result[0], result[1], result[2]
        records = [dict(zip(headers, row)) for row in rows]
        return records, headers, schema or {c: "string" for c in headers}

    if db_type == "email":
        raise ValueError(
            "Email cannot be a transfer source; configure it as a destination only."
        )

    raise ValueError(f"Database source '{db_type}' read not implemented")


def write_destination_database(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    mappings: list[dict],
    **options: Any,
) -> tuple[int, list[str], dict]:
    """Write to a database destination, stamping the pre-write row count.

    Gate-8 cannot prove an append into a non-empty table from the final count
    alone, so the count is taken here — before the writer runs — and carried on
    the destination summary for ``reconcile()`` to check the delta against.
    Keyword options are forwarded verbatim to ``_write_destination_database``.
    """
    from services.dest_precount import PRECOUNT_KEY, precount_destination
    from services.dialect_profiles import schema_from_cfg
    from services.row_conservation import CENSUS_KEY, prepare_keyed_upsert
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    rows_before = precount_destination(endpoint, cfg)
    write_mode = str(options.get("write_mode") or "")
    conflict_columns = list(options.get("conflict_columns") or [])
    census_payload = None
    if write_mode.lower() == "upsert" and conflict_columns:
        db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
        records, census_payload = prepare_keyed_upsert(
            records,
            key_columns=conflict_columns,
            mappings=mappings,
            db_type=db_type,
            cfg=cfg,
            schema=schema_from_cfg(db_type, cfg),
            table_name=resolve_dest_table(db_type, endpoint, "dt_import"),
            dest_nonempty=bool(rows_before),
        )
    rows_written, ddl_log, summary = _write_destination_database(
        endpoint, records, columns, schema, mappings, **options
    )
    if rows_before is not None and isinstance(summary, dict):
        summary.setdefault(PRECOUNT_KEY, int(rows_before))
    if census_payload is not None and isinstance(summary, dict):
        summary[CENSUS_KEY] = census_payload
    return rows_written, ddl_log, summary


def _write_destination_database(
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
    job_id: str | None = None,
    skip_preflight: bool = False,
    error_policy: str | None = None,
    sync_mode: str = "",
) -> tuple[int, list[str], dict]:
    """Write records to a SQL/NoSQL destination.

    ``error_policy`` overrides the policy normally derived from
    ``validation_mode``. It exists for write targets whose contract differs from
    the user's validation intent — notably the pre-ingestion staging landing
    zone, which must land every source row for inspection even when the job is
    strict. Callers that omit it keep the validation-mode-derived policy.
    """
    from .connector_capabilities import resolve_driver_type
    from connectors.write_resilience import build_write_batch_key

    cfg = resolve_connector_config(endpoint)
    # Prefer the saved connector's driver type over any inline format string.
    db_type = resolve_driver_type(cfg.get("type") or endpoint.format or "")
    ddl_log: list[str] = []

    from connectors.writer_common import (
        transform_error_policy,
        transform_error_policy_for_validation_mode,
    )

    error_policy = (
        transform_error_policy(error_policy)
        if error_policy
        else transform_error_policy_for_validation_mode(validation_mode)
    )

    headers, data_rows = records_to_matrix(records, columns)
    column_types = {c: ddl_carrier_type(schema.get(c, "string")) for c in columns}
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    table_name = resolve_dest_table(db_type, endpoint, "dt_import")

    from services.dialect_profiles import schema_from_cfg

    common = {
        "host": cfg["host"],
        "port": cfg["port"]
        or (
            5439
            if db_type == "redshift"
            else 5432
            if db_type == "postgresql"
            else 3306
            if db_type == "mysql"
            else 1433
            if db_type == "sqlserver"
            else 1521
            if db_type == "oracle"
            else 9092
            if db_type == "kafka"
            else 6333
            if db_type == "qdrant"
            else 8080
            if db_type == "weaviate"
            else 19530
            if db_type == "milvus"
            else 22
            if db_type == "sftp"
            else 587
            if db_type == "email"
            else 0
            if db_type in ("generic_sql", "iceberg", "sqlite")
            else 443
        ),
        "database": cfg["database"],
        "username": cfg.get("username", ""),
        "password": cfg.get("password", ""),
        "schema": schema_from_cfg(db_type, cfg),
        "connection_string": cfg.get("connection_string", ""),
        "ssl": cfg.get("ssl", False),
        "auth_source": cfg.get("auth_source", ""),
        "service_account": cfg.get("service_account", ""),
        "api_key": cfg.get("api_key", ""),
        "endpoint_url": cfg.get("endpoint_url", ""),
        "path_style": cfg.get("path_style", False),
        "role": cfg.get("role", ""),
        "table_name": table_name,
        "headers": headers,
        "data_rows": data_rows,
        "mappings": mappings,
        "column_types": column_types,
        "on_checkpoint": on_checkpoint,
        "error_policy": error_policy,
        "backfill_new_fields": backfill_new_fields,
        "job_id": job_id,
        "write_batch_key": build_write_batch_key(
            table_name=table_name, file_batch_idx=0
        ),
        "file_batch_idx": 0,
        "skip_preflight": skip_preflight,
        "sync_mode": sync_mode,
        # Live dest nullability for write-time NOT NULL escalate (G3 parity).
        "destination_column_nullability": dict(
            (cfg.get("extra") or {}).get("schema_nullability")
            or (getattr(endpoint, "extra", None) or {}).get("schema_nullability")
            or {}
        ),
        # Live dest DDL from Studio probe — writers prefer this over Map stamps
        # for transform/quarantine before physical introspect (invent cliff).
        "destination_column_types": dict(
            (cfg.get("extra") or {}).get("schema_types")
            or (getattr(endpoint, "extra", None) or {}).get("schema_types")
            or (cfg.get("extra") or {}).get("destination_column_types")
            or (getattr(endpoint, "extra", None) or {}).get("destination_column_types")
            or {}
        ),
        "source_schema_catalog": (cfg.get("extra") or {}).get("source_schema_catalog"),
    }

    if db_type == "snowflake":
        from connectors.snowflake_writer import write_mapped_rows

        common["schema"] = schema_from_cfg("snowflake", cfg)
        common["warehouse"] = cfg.get("warehouse", "")
        for col in columns:
            ddl_log.append(
                f"SNOWFLAKE COLUMN {col} {ddl_type('snowflake', schema.get(col, 'string'))}"
            )
        result = write_mapped_rows(
            **common, write_mode=write_mode, conflict_columns=conflict_columns or []
        )
        if not result.ok:
            raise_writer_failure(result, "Snowflake write failed")
        ddl_log.insert(
            0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}"
        )
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "snowflake",
                "schema": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "postgresql" or db_type == "redshift":
        from connectors.postgresql_writer import write_mapped_rows

        common["schema"] = schema_from_cfg(db_type, cfg)
        if db_type in {"redshift", "amazon_redshift", "redshift_serverless"}:
            common["port"] = cfg["port"] or 5439
            common["dest_extra"] = dict(cfg.get("extra") or {})
        for col in columns:
            ddl_log.append(
                f"{db_type.upper()} COLUMN {col} {ddl_type(db_type, schema.get(col, 'string'))}"
            )
        result = write_mapped_rows(
            **common,
            write_mode=write_mode,
            conflict_columns=conflict_columns or [],
            engine=db_type,
        )
        if not result.ok:
            raise_writer_failure(result, f"{db_type} write failed")
        ddl_log.insert(
            0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}"
        )
        return (
            result.rows_written,
            ddl_log,
            {
                "type": db_type,
                "schema": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "mysql":
        from connectors.mysql_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(
                f"MYSQL COLUMN {col} {ddl_type('mysql', schema.get(col, 'string'))}"
            )
        result = write_mapped_rows(
            **common, write_mode=write_mode, conflict_columns=conflict_columns or []
        )
        if not result.ok:
            raise_writer_failure(result, "MySQL write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "mysql",
                "database": cfg["database"],
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "bigquery":
        from connectors.bigquery_writer import write_mapped_rows

        common["schema"] = cfg.get("schema", "dataflow")
        for col in columns:
            ddl_log.append(
                f"BQ COLUMN {col} {ddl_type('bigquery', schema.get(col, 'string'))}"
            )
        result = write_mapped_rows(
            **common,
            warehouse=cfg.get("warehouse", ""),
            write_mode=write_mode,
            conflict_columns=conflict_columns or [],
        )
        if not result.ok:
            raise_writer_failure(result, "BigQuery write failed")
        ddl_log.insert(
            0,
            f"CREATE TABLE IF NOT EXISTS {cfg['database']}.{result.target_schema}.{result.table_name}",
        )
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "bigquery",
                "project": cfg["database"],
                "dataset": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

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
        result = write_mapped_rows(
            **common, write_mode=write_mode, conflict_columns=conflict_columns or []
        )
        if not result.ok:
            raise_writer_failure(result, "MongoDB write failed")
        ddl_log.insert(
            0,
            f"CREATE COLLECTION IF NOT EXISTS {result.target_schema}.{result.table_name}",
        )
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "mongodb",
                "database": result.target_schema,
                "collection": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "gcs":
        from connectors.gcs_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"GCS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "GCS write failed")
        ddl_log.insert(0, f"PUT gs://{cfg['database']}/{result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "gcs",
                "bucket": cfg["database"],
                "key": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "adls":
        from connectors.adls_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"ADLS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Azure Blob write failed")
        ddl_log.insert(0, f"PUT abfs://{cfg['database']}/{result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "adls",
                "container": cfg["database"],
                "key": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "s3":
        from connectors.s3_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"S3 FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "S3 write failed")
        ddl_log.insert(0, f"PUT s3://{cfg['database']}/{result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "s3",
                "bucket": cfg["database"],
                "key": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "dynamodb":
        from connectors.dynamodb_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"DYNAMODB ATTR {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "DynamoDB write failed")
        ddl_log.insert(0, f"BATCH WRITE DynamoDB table {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "dynamodb",
                "table": result.table_name,
                "region": result.target_schema,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "elasticsearch":
        from connectors.elasticsearch_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"ES FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Elasticsearch write failed")
        ddl_log.insert(0, f"BULK INDEX {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "elasticsearch",
                "index": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "redis":
        from connectors.redis_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(f"REDIS FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Redis write failed")
        ddl_log.insert(0, f"SET keys under prefix {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "redis",
                "prefix": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "sqlite":
        from connectors.sqlite_writer import write_mapped_rows

        for col in columns:
            ddl_log.append(
                f"SQLITE COLUMN {col} {ddl_type('sqlite', schema.get(col, 'string'))}"
            )
        result = write_mapped_rows(
            **common, write_mode=write_mode, conflict_columns=conflict_columns or []
        )
        if not result.ok:
            raise_writer_failure(result, "SQLite write failed")
        ddl_log.insert(0, f"CREATE TABLE IF NOT EXISTS {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "sqlite",
                "database": cfg["database"],
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "generic_sql":
        from connectors.generic_sql import write_mapped_rows

        for col in columns:
            ddl_log.append(
                f"GENERIC_SQL COLUMN {col} {ddl_type('generic_sql', schema.get(col, 'string'))}"
            )
        common["type"] = cfg.get("type", "")
        result = write_mapped_rows(
            **common, write_mode=write_mode, conflict_columns=conflict_columns or []
        )
        if not result.ok:
            raise_writer_failure(result, "Generic SQL write failed")
        ddl_log.insert(
            0, f"CREATE TABLE IF NOT EXISTS {result.target_schema}.{result.table_name}"
        )
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "generic_sql",
                "driver": result.driver,
                "schema": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "sftp":
        from connectors.sftp_common import host_key_settings
        from connectors.sftp_writer import write_mapped_rows

        common.update(host_key_settings(cfg))
        for col in columns:
            ddl_log.append(f"SFTP FIELD {col}")
        if (
            not common.get("table_name")
            and not endpoint.connection_string
            and not endpoint.database
        ):
            raise ValueError(
                "SFTP destination requires a remote file path (connection_string, database, or table field)"
            )
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "SFTP write failed")
        ddl_log.insert(0, f"PUT sftp://{cfg.get('host', '')}/{result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "sftp",
                "host": cfg.get("host", ""),
                "path": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "email":
        from connectors.email import write_mapped_rows

        for col in columns:
            ddl_log.append(f"EMAIL FIELD {col}")
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Email send failed")
        ddl_log.insert(0, f"EMAIL {result.table_name} via {cfg.get('host', '')}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "email",
                "host": cfg.get("host", ""),
                "subject": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    if db_type == "pgvector":
        from connectors.pgvector_writer import write_mapped_rows

        _apply_vector_extra(common, endpoint)
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "pgvector write failed")
        ddl_log.insert(0, f"UPSERT pgvector {result.target_schema}.{result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "pgvector",
                "schema": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                "load_method": getattr(result, "load_method", None)
                or "pgvector_upsert",
                **_writer_diagnostics(result),
            },
        )

    if db_type == "qdrant":
        from connectors.qdrant_writer import write_mapped_rows

        _apply_vector_extra(common, endpoint)
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Qdrant write failed")
        ddl_log.insert(0, f"UPSERT qdrant collection {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "qdrant",
                "collection": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                "load_method": getattr(result, "load_method", None) or "qdrant_upsert",
                **_writer_diagnostics(result),
            },
        )

    if db_type == "weaviate":
        from connectors.weaviate_writer import write_mapped_rows

        _apply_vector_extra(common, endpoint)
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Weaviate write failed")
        ddl_log.insert(0, f"UPSERT weaviate class {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "weaviate",
                "collection": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                "load_method": getattr(result, "load_method", None)
                or "weaviate_upsert",
                **_writer_diagnostics(result),
            },
        )

    if db_type == "pinecone":
        from connectors.pinecone_writer import write_mapped_rows

        _apply_vector_extra(common, endpoint)
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Pinecone write failed")
        ddl_log.insert(0, f"UPSERT pinecone namespace {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "pinecone",
                "collection": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                "load_method": getattr(result, "load_method", None)
                or "pinecone_upsert",
                **_writer_diagnostics(result),
            },
        )

    if db_type == "milvus":
        from connectors.milvus_writer import write_mapped_rows

        _apply_vector_extra(common, endpoint)
        result = write_mapped_rows(**common)
        if not result.ok:
            raise_writer_failure(result, "Milvus write failed")
        ddl_log.insert(0, f"UPSERT milvus collection {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": "milvus",
                "collection": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                "load_method": getattr(result, "load_method", None) or "milvus_upsert",
                **_writer_diagnostics(result),
            },
        )

    # Registry-driven path: sqlserver, oracle, iceberg, kafka, salesforce, hubspot, …
    from .connector_dispatch import has_writer, write_via_registry

    if has_writer(db_type):
        extra: dict[str, Any] = {}
        if db_type == "kafka":
            extra["schema_registry_url"] = str(
                (endpoint.extra or {}).get("schema_registry_url")
                or cfg.get("schema_registry_url")
                or ""
            )
        if db_type in ("salesforce", "hubspot"):
            # Prefer upsert for activation / reverse-ETL destinations.
            if (write_mode or "insert").lower() == "insert":
                write_mode = "upsert"
            if (endpoint.extra or {}).get("activation_batch_size"):
                extra["batch_size"] = int(endpoint.extra["activation_batch_size"])
        for col in columns:
            ddl_log.append(f"{db_type.upper()} FIELD {col}")
        result = write_via_registry(
            db_type,
            common=common,
            write_mode=write_mode,
            conflict_columns=conflict_columns or [],
            extra=extra or None,
        )
        if not result.ok:
            raise_writer_failure(result, f"{db_type} write failed")
        ddl_log.insert(0, f"WRITE {db_type} → {result.table_name}")
        return (
            result.rows_written,
            ddl_log,
            {
                "type": db_type,
                "schema": result.target_schema,
                "table": result.table_name,
                "checksum": result.checksum,
                "driver": result.driver,
                **_writer_diagnostics(result),
            },
        )

    raise ValueError(f"Database destination '{db_type}' write not implemented")


def write_destination_file(
    endpoint: EndpointConfig,
    records: list[dict],
    columns: list[str],
    *,
    source_format: str | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    validation_mode: str = "strict",
) -> tuple[bytes, str, dict]:
    """Write records to CSV, JSON, JSONL, or TSV using unified format converter."""
    import sys
    from pathlib import Path

    _api_root = Path(__file__).resolve().parents[2]
    if str(_api_root) not in sys.path:
        sys.path.insert(0, str(_api_root))
    from connectors.writer_common import (
        resolve_target_columns,
        transform_error_policy_for_validation_mode,
    )
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
    rejected_details: list[dict[str, Any]] = []

    if mappings:
        from connectors.writer_common import (
            apply_write_quarantine_matrix,
            build_mapped_rows_with_details,
            reject_on_strict_policy,
            transform_error_policy,
        )
        from services.value_serializer import (
            DF_MISSING_SENTINEL,
            is_missing_sentinel,
        )

        headers = columns
        data_rows: list[list[Any]] = []
        for rec in records:
            row: list[Any] = []
            for col in headers:
                if col not in rec:
                    # Absent key ≠ empty string — preserve omit semantics for Map.
                    row.append(DF_MISSING_SENTINEL)
                    continue
                val = rec[col]
                if is_missing_sentinel(val):
                    row.append(val)
                elif val is None:
                    row.append(None)
                else:
                    row.append(cell_to_string(val))
            data_rows.append(row)
        target_cols, _ = resolve_target_columns(mappings, types)
        error_policy = transform_error_policy_for_validation_mode(validation_mode)
        mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=types,
            error_policy=error_policy,
            dest_types={
                str(m.get("target") or ""): str(
                    m.get("target_type") or m.get("dest_type") or types.get(m.get("source") or "", "")
                )
                for m in mappings
                if str(m.get("target") or "").strip()
            },
        )
        # Same typed quarantine matrix as SQL/object writers — never export oversize/
        # unfit cells that transforms left as strings.
        dest_type_list = [
            str(
                next(
                    (
                        m.get("target_type") or m.get("dest_type") or types.get(m.get("source") or "", "")
                        for m in mappings
                        if str(m.get("target") or "") == col
                    ),
                    types.get(col, ""),
                )
                or ""
            )
            for col in target_cols
        ]
        if mapped_rows and any(str(t).strip() for t in dest_type_list):
            policy = transform_error_policy(error_policy)
            mapped_rows = apply_write_quarantine_matrix(
                mapped_rows,
                target_cols,
                dest_type_list,
                rejected_details,
                policy,
                dialect_label="file_export",
                mappings=list(mappings) or None,
            )
        # Keep DF_MISSING through export — JSON/JSONL omit keys; dense CSV/grid
        # render empty via cell_to_string. Never force-null invent before serialize.
        abort = reject_on_strict_policy(error_policy, rejected_details, "file_export")
        if abort:
            raise FileExportMapBlocked(
                abort,
                rejected_details=rejected_details,
                rejected_rows=len(rejected_details),
            )
        export_columns = target_cols
        export_records = [dict(zip(target_cols, row)) for row in mapped_rows]

    def _export_summary(
        filename: str,
        *,
        mime: str | None = None,
        converted_from: str | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {
            "format": fmt,
            "filename": filename,
            "rows": len(export_records),
            "transform_errors": transform_errors[:10],
            "mapped": bool(mappings),
            "rejected_details": list(rejected_details),
            "rejected_rows": len(rejected_details),
        }
        if mime is not None:
            out["mime"] = mime
        if converted_from is not None:
            out["converted_from"] = converted_from
        return out

    grid = [
        [cell_to_string(rec.get(col, "")) for col in export_columns]
        for rec in export_records
    ]

    # JSON/JSONL must use omit-aware serialization below — the grid convert path
    # runs cell_to_string which collapses DF_MISSING to "" (false invent).
    if fmt not in {"json", "jsonl"} and can_convert(src_fmt, fmt) and grid:
        content, mime = convert_rows(
            export_columns, grid, source_format=src_fmt, target_format=fmt
        )
        ext = (
            "tsv"
            if fmt == "tsv"
            else "xlsx"
            if fmt == "excel"
            else "parquet"
            if fmt == "parquet"
            else "avro"
            if fmt == "avro"
            else "orc"
            if fmt == "orc"
            else "xml"
            if fmt == "xml"
            else fmt
            if fmt in ("csv", "jsonl")
            else "json"
        )
        filename = f"export.{ext}"
        return (
            content,
            filename,
            _export_summary(
                filename=filename,
                mime=mime,
                converted_from=src_fmt if src_fmt != fmt else None,
            ),
        )

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
                    def _reject(name: str) -> None:
                        raise ValueError(f"non-finite JSON constant: {name}")

                    return json.loads(
                        text, parse_float=Decimal, parse_constant=_reject
                    )
                except (json.JSONDecodeError, ValueError):
                    return value
            if ctype in {
                "text",
                "string",
                "varchar",
                "uuid",
                "binary",
                "date",
                "datetime",
                "time",
            }:
                return value
            try:
                def _reject(name: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {name}")

                return json.loads(
                    text, parse_float=Decimal, parse_constant=_reject
                )
            except (json.JSONDecodeError, ValueError):
                return value
        return value

    def _json_export_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from services.value_serializer import is_missing_sentinel

        # Kafka/object-store class: omit STOP_COLUMN / sparse CDC keys entirely.
        return [
            {
                c: _to_json_value(v, c)
                for c, v in r.items()
                if not is_missing_sentinel(v)
            }
            for r in rows
        ]

    export_mime = ""
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=export_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(
            [{c: cell_to_string(v) for c, v in r.items()} for r in export_records]
        )
        content = buf.getvalue().encode("utf-8")
        filename = "export.csv"
        export_mime = "text/csv"
    elif fmt == "tsv":
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=export_columns, delimiter="\t", extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(
            [{c: cell_to_string(v) for c, v in r.items()} for r in export_records]
        )
        content = buf.getvalue().encode("utf-8")
        filename = "export.tsv"
        export_mime = "text/tab-separated-values"
    elif fmt == "jsonl":
        records = _json_export_records(export_records)
        lines = [
            json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False)
            for r in records
        ]
        content = "\n".join(lines).encode("utf-8")
        filename = "export.jsonl"
        export_mime = "application/x-ndjson"
    elif fmt == "excel":
        content, export_mime = convert_rows(
            export_columns, grid, source_format="csv", target_format=fmt
        )
        filename = "export.xlsx"
    elif fmt == "parquet":
        content, export_mime = convert_rows(
            export_columns, grid, source_format="csv", target_format=fmt
        )
        filename = "export.parquet"
    else:
        records = _json_export_records(export_records)
        content = json.dumps(
            records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        filename = "export.json"
        export_mime = "application/json"
    return (
        content,
        filename,
        _export_summary(filename, mime=export_mime or None),
    )


def resolve_endpoint_dict(
    endpoint_dict: dict[str, Any], workspace_id: str | None = None
) -> dict[str, Any]:
    """Resolve a saved connector into a plain endpoint dict (format, credentials, etc.)."""
    from .models import EndpointConfig, endpoint_to_dict

    kind = endpoint_dict.get("kind") or "database"
    ep = EndpointConfig.from_dict(kind, endpoint_dict)
    resolved = resolve_endpoint(ep, workspace_id=workspace_id)
    out = endpoint_to_dict(resolved)
    # Preserve keys the UI may rely on that are not in endpoint_to_dict.
    for key, value in endpoint_dict.items():
        if key not in out and value is not None:
            out[key] = value
    return out
