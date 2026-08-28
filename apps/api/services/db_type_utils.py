"""Shared helpers for normalizing database type names and looking up schema keys."""

from __future__ import annotations

_DB_TYPE_ALIASES = {
    "mongo": "mongodb",
    "mongodb+srv": "mongodb",
    "mongodb_atlas": "mongodb",
    "atlas": "mongodb",
    "cosmos-mongodb": "mongodb",
    "cosmos_mongodb": "mongodb",
    "documentdb": "mongodb",
    "aws_documentdb": "mongodb",
    "dynamo": "dynamodb",
    "redis-kv": "redis",
    "redis_kv": "redis",
}

SCHEMALESS_DESTS = {"mongodb", "dynamodb", "redis"}

# Object stores / file sinks have no CREATE TABLE contract — "table_exists"
# probes are N/A. Do not fail-closed Validate on sticky None for these kinds.
NO_RELATIONAL_DDL_DESTS = frozenset({
    "s3",
    "gcs",
    "adls",
    "minio",
    "azure_blob",
    "azure_data_lake",
    "file",
    "file_export",
    "csv",
    "tsv",
    "json",
    "jsonl",
    "ndjson",
    "excel",
    "parquet",
    "avro",
    "orc",
    "xml",
})


def normalize_dest_kind(dest_db_type: str | None, default: str = "") -> str:
    """Normalize a destination database type string to a canonical driver name."""
    raw = (dest_db_type or default).strip().lower().replace(" ", "_")
    if not raw:
        return ""
    if raw in _DB_TYPE_ALIASES:
        return _DB_TYPE_ALIASES[raw]
    if raw.startswith("mongodb"):
        return "mongodb"
    if raw.startswith("dynamodb"):
        return "dynamodb"
    if raw.startswith("redis"):
        return "redis"
    return raw


def ci_get(schema: dict[str, str], key: str) -> str | None:
    """Case-insensitive key lookup in a schema dict."""
    key_l = key.lower()
    for existing_key, value in schema.items():
        if existing_key.lower() == key_l:
            return value
    return None
