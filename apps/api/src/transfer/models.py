"""Universal transfer request/result models."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class EndpointConfig:
    kind: str  # file, database, file_export
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
    ssl: bool = True
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
            schema=d.get("schema", "public"),
            table=d.get("table", d.get("table_name", "")),
            collection=d.get("collection", d.get("collection_name", "")),
            username=d.get("username", ""),
            password=d.get("password", ""),
            connection_string=d.get("connection_string", ""),
            warehouse=d.get("warehouse", ""),
            ssl=d.get("ssl", True),
            extra={k: v for k, v in d.items() if k not in {
                "format", "type", "db_type", "connector_id", "host", "port",
                "database", "schema", "table", "table_name", "collection",
                "collection_name", "username", "password", "connection_string",
                "warehouse", "ssl",
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
    columns: list[str] = field(default_factory=list)


@dataclass
class TransferCapabilities:
    live_combinations: list[dict]
    source_formats: list[str]
    destination_types: list[str]
    operations: list[str]
