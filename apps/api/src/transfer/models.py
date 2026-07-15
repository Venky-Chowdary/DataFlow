"""Universal transfer request/result models."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class EndpointConfig:
    kind: str = "database"  # file, database, file_export
    format: str = ""  # csv, json, postgresql, mongodb, snowflake
    connector_id: Optional[str] = None
    host: str = ""
    port: int = 0
    database: str = ""
    schema: str = ""
    table: str = ""
    collection: str = ""
    username: str = ""
    password: str = ""
    connection_string: str = ""
    warehouse: str = ""
    # False => sslmode "prefer": negotiate TLS when available, fall back for local DBs
    ssl: bool = False
    auth_mode: str = ""
    auth_role: str = ""
    auth_source: str = ""
    api_key: str = ""
    service_account: str = ""
    # For file_export destinations: a server-local path to write the export file.
    # If empty, the file is written to the configured exports directory and a
    # download URL is returned.
    output_path: str = ""
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, kind: str, data: dict | None) -> "EndpointConfig":
        d = data or {}
        return cls(
            kind=kind,
            format=d.get("format", d.get("type", d.get("db_type", ""))),
            connector_id=d.get("connector_id"),
            host=d.get("host", "localhost"),
            port=int(d.get("port", 0) or 0),
            database=d.get("database", ""),
            schema=d.get("schema", ""),
            table=d.get("table", d.get("table_name", "")),
            collection=d.get("collection", d.get("collection_name", "")),
            username=d.get("username", ""),
            password=d.get("password", ""),
            connection_string=d.get("connection_string", ""),
            warehouse=d.get("warehouse", ""),
            ssl=d.get("ssl", False),
            auth_mode=d.get("auth_mode", ""),
            auth_role=d.get("auth_role", ""),
            auth_source=d.get("auth_source", ""),
            api_key=d.get("api_key", ""),
            service_account=d.get("service_account", ""),
            output_path=(d.get("output_path") or "").strip(),
            extra={k: v for k, v in d.items() if k not in {
                "format", "type", "db_type", "connector_id", "host", "port",
                "database", "schema", "table", "table_name", "collection",
                "collection_name", "username", "password", "connection_string",
                "warehouse", "ssl", "auth_mode", "auth_role", "auth_source", "api_key", "service_account",
                "output_path",
            }},
        )


@dataclass
class TransferRequest:
    source: EndpointConfig
    destination: EndpointConfig
    mappings: list[dict] = field(default_factory=list)
    column_types: dict[str, str] = field(default_factory=dict)
    skip_preflight: bool = False
    source_filename: str = ""
    source_content: bytes = b""
    # Optional on-disk path for the source file.  Used for billion-row streaming
    # when loading the whole file into memory would exhaust RAM.
    source_path: str = ""
    sync_mode: str = "full_refresh_overwrite"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    # Optional row-level source filter (column predicates, and/or composition).
    source_filter: dict = field(default_factory=dict)
    # Priority-first sync: sort source rows by this column before writing.
    priority_column: str = ""
    priority_direction: str = "desc"  # "asc" or "desc"
    limit: int = 0  # 0 means no limit
    # Workspace isolation: transfers and their jobs can be scoped to a workspace.
    workspace_id: str = ""
    stream_contracts: list[dict] = field(default_factory=list)
    contract_id: str = ""
    enforce_contract: bool = True

    @property
    def operation(self) -> str:
        sk, dk = self.source.kind, self.destination.kind
        if sk == "file" and dk == "database":
            return "upload"
        if sk == "database" and dk == "database":
            return "migration"
        if sk == "file" and dk == "file_export":
            return "convert"
        if sk == "database" and dk == "file_export":
            return "dump"
        return "transfer"


def endpoint_to_dict(ep: EndpointConfig) -> dict:
    return {
        "kind": ep.kind,
        "format": ep.format,
        "connector_id": ep.connector_id,
        "host": ep.host,
        "port": ep.port,
        "database": ep.database,
        "schema": ep.schema,
        "table": ep.table,
        "collection": ep.collection,
        "username": ep.username,
        "password": ep.password,
        "connection_string": ep.connection_string,
        "warehouse": ep.warehouse,
        "ssl": ep.ssl,
        "auth_mode": ep.auth_mode,
        "auth_role": ep.auth_role,
        "auth_source": ep.auth_source,
        "api_key": ep.api_key,
        "service_account": ep.service_account,
        "output_path": ep.output_path,
    }


def transfer_request_to_dict(request: TransferRequest) -> dict:
    return {
        "source": endpoint_to_dict(request.source),
        "destination": endpoint_to_dict(request.destination),
        "mappings": request.mappings,
        "column_types": request.column_types,
        "skip_preflight": request.skip_preflight,
        "source_filename": request.source_filename,
        "sync_mode": request.sync_mode,
        "schema_policy": request.schema_policy,
        "validation_mode": request.validation_mode,
        "backfill_new_fields": request.backfill_new_fields,
        "source_filter": request.source_filter,
        "priority_column": request.priority_column,
        "priority_direction": request.priority_direction,
        "limit": request.limit,
        "workspace_id": request.workspace_id,
        "stream_contracts": request.stream_contracts,
        "contract_id": request.contract_id,
        "enforce_contract": request.enforce_contract,
        "requires_file_reupload": request.source.kind == "file" and bool(request.source_content),
    }


def transfer_request_from_dict(data: dict) -> TransferRequest:
    src = data.get("source") or {}
    dst = data.get("destination") or {}
    return TransferRequest(
        source=EndpointConfig.from_dict(src.get("kind", "database"), src),
        destination=EndpointConfig.from_dict(dst.get("kind", "database"), dst),
        mappings=data.get("mappings") or [],
        column_types=data.get("column_types") or {},
        skip_preflight=bool(data.get("skip_preflight")),
        source_filename=data.get("source_filename") or "",
        source_content=b"",
        sync_mode=data.get("sync_mode") or "full_refresh_overwrite",
        schema_policy=data.get("schema_policy") or "manual_review",
        validation_mode=data.get("validation_mode") or "strict",
        backfill_new_fields=bool(data.get("backfill_new_fields")),
        source_filter=data.get("source_filter") or {},
        priority_column=(data.get("priority_column") or "").strip(),
        priority_direction=(data.get("priority_direction") or "desc").lower(),
        limit=int(data.get("limit") or 0),
        workspace_id=(data.get("workspace_id") or "").strip(),
        stream_contracts=data.get("stream_contracts") or [],
        contract_id=data.get("contract_id") or "",
        enforce_contract=bool(data.get("enforce_contract", True)),
    )


@dataclass
class TransferResult:
    success: bool
    job_id: str = ""
    records_transferred: int = 0
    operation: str = ""
    source_summary: dict = field(default_factory=dict)
    destination_summary: dict = field(default_factory=dict)
    ddl_executed: list[str] = field(default_factory=list)
    error: str = ""
    error_details: dict = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    reconciliation: dict = field(default_factory=dict)
    validation_plan: dict = field(default_factory=dict)
    payload_shape: dict = field(default_factory=dict)
    contract_id: str = ""
    explanation: str = ""
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    peak_memory_bytes: int = 0


@dataclass
class TransferCapabilities:
    live_combinations: list[dict]
    source_formats: list[str]
    destination_types: list[str]
    operations: list[str]
