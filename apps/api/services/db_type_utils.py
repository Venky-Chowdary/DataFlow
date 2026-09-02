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
    "kafka",
    "pinecone",
    "milvus",
    "qdrant",
    "weaviate",
})

# Reverse-ETL / dest-only objects are not DROP TABLE + CREATE. Overwrite
# upserts records against live Describe (Salesforce/HubSpot) or create-on-write
# (Kafka / vector). Treating them as dest_recreated invented TEXT recreate
# stamps and G19 blocked DECIMAL → TEXT on every CRM SKU route.
DESTS_WITHOUT_SCHEMA_RECREATE = frozenset({
    "salesforce",
    "hubspot",
    "stripe",
    "rest_api",
    "kafka",
    "pinecone",
    "milvus",
    "qdrant",
    "weaviate",
    "iceberg",
    "apache_iceberg",
})


def dest_schema_is_recreated_on_overwrite(dest_db_type: str | None) -> bool:
    """True when overwrite actually drops and recreates destination DDL."""
    kind = normalize_dest_kind(dest_db_type)
    if not kind:
        return True
    if kind in DESTS_WITHOUT_SCHEMA_RECREATE:
        return False
    if kind in SCHEMALESS_DESTS or kind in NO_RELATIONAL_DDL_DESTS:
        return False
    return True


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


_NO_DDL_KIND_TOKENS = (
    "s3",
    "gcs",
    "google_cloud_storage",
    "adls",
    "azure_blob",
    "azure_data_lake",
    "minio",
    "object_store",
)


def dest_declares_column_ddl(dest_db_type: str | None) -> bool:
    """True when the destination's live column types come from real DDL.

    A relational/warehouse destination hands back the catalog: ``DECIMAL(12,2)``
    is a declared capacity the writer must respect. A document store, a KV store
    and an object-store prefix hand back a *profile* of whatever values happened
    to be sampled, so the same column reads as ``DECIMAL(2,2)`` on one pass and
    ``DECIMAL(6,2)`` on the next. Treating that as declared DDL is what made a
    route refuse its own second run for a "narrow_type" collapse onto a sink
    that has no column types at all.

    Alias-tolerant on purpose: ``amazon_s3`` / ``google_cloud_storage`` reach
    this helper unnormalized from catalog ids.
    """
    kind = normalize_dest_kind(dest_db_type)
    if not kind:
        return True
    if kind in SCHEMALESS_DESTS or kind in NO_RELATIONAL_DDL_DESTS:
        return False
    return not any(token in kind for token in _NO_DDL_KIND_TOKENS)


def sample_inferred_carrier(inferred: str | None) -> str:
    """A profiled carrier with its fabricated capacity dropped.

    ``DECIMAL(2,2)`` inferred from 200 sampled CSV rows states a precision the
    destination never declared. Keeping the family and dropping the parameters
    is the honest reading: the sink stores what the source carries.
    """
    text = (inferred or "").strip()
    if "(" not in text:
        return text
    head, _, rest = text.partition("(")
    tail = rest.partition(")")[2].strip()
    return f"{head.strip()} {tail}".strip() if tail else head.strip()


def ci_get(schema: dict[str, str], key: str) -> str | None:
    """Case-insensitive key lookup in a schema dict."""
    key_l = key.lower()
    for existing_key, value in schema.items():
        if existing_key.lower() == key_l:
            return value
    return None
