"""Universal transfer request/result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


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
    # SSH/SFTP private key content, PEM text, or base64-encoded key.
    private_key: str = ""
    # For file_export destinations: a server-local path to write the export file.
    # If empty, the file is written to the configured exports directory and a
    # download URL is returned.
    output_path: str = ""
    # Cloud/data region for the endpoint (e.g. us-east-1, eu-west-1).
    region: str = ""
    # S3 / S3-compatible custom endpoint, e.g. http://minio:9000.
    endpoint_url: str = ""
    # Force path-style addressing for S3-compatible stores (MinIO, LocalStack, Wasabi).
    path_style: bool = False
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, kind: str, data: dict | None) -> "EndpointConfig":
        d = data or {}
        known = {
            "kind", "format", "type", "db_type", "connector_id", "host", "port",
            "database", "schema", "table", "table_name", "collection",
            "collection_name", "username", "password", "connection_string",
            "warehouse", "ssl", "auth_mode", "auth_role", "auth_source", "api_key", "service_account",
            "private_key", "ssh_private_key", "output_path", "region", "endpoint_url", "path_style",
            "extra",
        }
        nested = d.get("extra") if isinstance(d.get("extra"), dict) else {}
        flat_extra = {k: v for k, v in d.items() if k not in known}
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
            private_key=d.get("private_key", d.get("ssh_private_key", "")),
            output_path=(d.get("output_path") or "").strip(),
            region=d.get("region", ""),
            endpoint_url=(d.get("endpoint_url") or "").strip(),
            path_style=bool(d.get("path_style", False)),
            extra={**nested, **flat_extra},
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
    sync_mode: str = "full_refresh_append"
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    backfill_new_fields: bool = False
    # Load into {table}_df_staging first; promote only clean rows to primary.
    write_via_staging: bool = False
    # Optional row-level source filter (column predicates, and/or composition).
    source_filter: dict = field(default_factory=dict)
    # Priority-first sync: sort source rows by this column before writing.
    priority_column: str = ""
    priority_direction: str = "desc"  # "asc" or "desc"
    limit: int = 0  # 0 means no limit
    # Workspace isolation: transfers and their jobs can be scoped to a workspace.
    workspace_id: str = ""
    # Data residency / region tag for the job.
    data_region: str = ""
    stream_contracts: list[dict] = field(default_factory=list)
    contract_id: str = ""
    enforce_contract: bool = True
    # When True with contract_id, transfer fails unless the contract is SIGNED.
    require_signed_contract: bool = False
    # Operator identity for Jobs audit (email / subject).
    triggered_by: str = ""

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
        "private_key": ep.private_key,
        "output_path": ep.output_path,
        "region": ep.region,
        "endpoint_url": ep.endpoint_url,
        "path_style": ep.path_style,
        "extra": dict(ep.extra or {}),
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
        "write_via_staging": request.write_via_staging,
        "source_filter": request.source_filter,
        "priority_column": request.priority_column,
        "priority_direction": request.priority_direction,
        "limit": request.limit,
        "workspace_id": request.workspace_id,
        "data_region": request.data_region,
        "stream_contracts": request.stream_contracts,
        "contract_id": request.contract_id,
        "enforce_contract": request.enforce_contract,
        "require_signed_contract": request.require_signed_contract,
        "triggered_by": request.triggered_by,
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
        sync_mode=data.get("sync_mode") or "full_refresh_append",
        schema_policy=data.get("schema_policy") or "manual_review",
        validation_mode=data.get("validation_mode") or "strict",
        backfill_new_fields=bool(data.get("backfill_new_fields")),
        write_via_staging=bool(data.get("write_via_staging")),
        source_filter=data.get("source_filter") or {},
        priority_column=(data.get("priority_column") or "").strip(),
        priority_direction=(data.get("priority_direction") or "desc").lower(),
        limit=int(data.get("limit") or 0),
        workspace_id=(data.get("workspace_id") or "").strip(),
        data_region=(data.get("data_region") or "").strip(),
        stream_contracts=data.get("stream_contracts") or [],
        contract_id=data.get("contract_id") or "",
        enforce_contract=bool(data.get("enforce_contract", True)),
        require_signed_contract=bool(data.get("require_signed_contract", False)),
        triggered_by=(data.get("triggered_by") or "").strip(),
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
