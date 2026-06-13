"""Supported connectors and data operations — universal platform registry."""

from __future__ import annotations

from enum import Enum


class DataOperation(str, Enum):
    UPLOAD = "upload"  # file → db
    DUMP = "dump"  # db → file
    TRANSFER = "transfer"  # generic
    MIGRATION = "migration"  # db → db full
    CONVERT = "convert"  # file → file


class EndpointKind(str, Enum):
    FILE = "file"
    DATABASE = "database"
    API = "api"
    FILE_EXPORT = "file_export"


DATABASE_TYPES = [
    "postgresql",
    "sqlserver",
    "mysql",
    "oracle",
    "mongodb",
    "snowflake",
    "bigquery",
    "redis",
    "databricks",
]

FILE_FORMATS = [
    "csv",
    "excel",
    "json",
    "parquet",
    "avro",
    "fixed_width",
    "pdf",
    "word",
    "xml",
    "sql",
]

# Pairs we will implement in connector plugins (not payment-only)
SUPPORTED_OPERATION_MATRIX: dict[tuple[str, str], DataOperation] = {
    ("file", "database"): DataOperation.UPLOAD,
    ("file", "file_export"): DataOperation.CONVERT,
    ("database", "database"): DataOperation.MIGRATION,
    ("database", "file_export"): DataOperation.DUMP,
    ("api", "database"): DataOperation.TRANSFER,
    ("database", "file"): DataOperation.DUMP,
    ("file", "file"): DataOperation.CONVERT,
}


def infer_operation(source_kind: str, dest_kind: str) -> DataOperation:
    key = (source_kind, dest_kind if dest_kind != "file" else "file_export")
    if source_kind == "file" and dest_kind == "file":
        return DataOperation.CONVERT
    return SUPPORTED_OPERATION_MATRIX.get(key, DataOperation.TRANSFER)
