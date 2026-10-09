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


def overwrite_replaces_rows(dest_db_type: str | None) -> bool:
    """True when an overwrite replaces every row, so unwritten columns lose values.

    SaaS objects, REST, Kafka and vector stores upsert on "overwrite": a
    property the run does not write keeps its value. Iceberg keeps its schema
    but its overwrite does replace the data files.
    """
    kind = normalize_dest_kind(dest_db_type)
    if kind in NO_RELATIONAL_DDL_DESTS:
        return False
    return kind in {"iceberg", "apache_iceberg"} or kind not in DESTS_WITHOUT_SCHEMA_RECREATE


# Overwrite of an existing relational table empties rows in place. DROP+CREATE
# threw away primary key, unique, NOT NULL, check, foreign key and identity
# (DEF-V7-E1-021). Warehouses that still DROP stay on the recreate path.
_OVERWRITE_KEEPS_EXISTING_TABLE = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlite",
    "oracle",
    "sqlserver",
    "generic_sql",
    "cockroachdb",
    "greenplum",
    "timescaledb",
    "db2",
    "teradata",
})


_SQLSERVER_OVERWRITE_ALIASES = frozenset({
    "mssql",
    "sql_server",
    "microsoft_sql_server",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
    "google_cloud_sql_sql_server",
    "synapse",
    "synapse_analytics",
    "azure_synapse_dedicated",
    "azure_synapse_serverless",
})


def overwrite_engine_kind(dest_db_type: str | None) -> str:
    """Canonical engine for the overwrite-keeps-table decision.

    ``mssql`` and ``sql_server`` are the same carrier. Leaving them off the
    keep-set made an overwrite DROP the table and lose its constraints.
    """
    kind = normalize_dest_kind(dest_db_type)
    if kind in _SQLSERVER_OVERWRITE_ALIASES or kind.startswith("mssql"):
        return "sqlserver"
    if kind in {"postgres", "pg"}:
        return "postgresql"
    return kind


def dest_schema_is_recreated_on_overwrite(dest_db_type: str | None) -> bool:
    """True when overwrite drops the object and creates it again.

    Relational engines empty an existing table instead, so the operator's
    constraints stay and the live column types are this run's contract.
    """
    kind = overwrite_engine_kind(dest_db_type)
    if not kind:
        return True
    if kind in _OVERWRITE_KEEPS_EXISTING_TABLE:
        return False
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
