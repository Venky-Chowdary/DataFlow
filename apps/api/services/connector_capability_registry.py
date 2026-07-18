"""Connector capability registry.

A structured sidecar of connector capabilities, tier priorities, rate limits,
CDC prerequisites, write semantics, and idempotency properties.  Used by the
universal orchestrator, the catalog service, and the preflight engine to make
connector-aware decisions rather than assume a one-size-fits-all path.
"""

from __future__ import annotations

from typing import Any

# ═══════════════════════════════════════════════════════════════════════════
# Tier taxonomy from the universal transfer prompt
# ═══════════════════════════════════════════════════════════════════════════
TIER_HIGHEST = "highest"
TIER_HIGH = "high"
TIER_MEDIUM = "medium"
TIER_STRATEGIC = "strategic"

TIER_ORDER = [TIER_HIGHEST, TIER_HIGH, TIER_MEDIUM, TIER_STRATEGIC]

# ═══════════════════════════════════════════════════════════════════════════
# Connector capability registry
# ═══════════════════════════════════════════════════════════════════════════
DEFAULT_CAPABILITY: dict[str, Any] = {
    "transfer_ready": False,
    "tier": TIER_STRATEGIC,
    "pattern": "batch",
    "supports_cdc": False,
    "supports_streaming": False,
    "supports_upsert": True,
    "supports_append": True,
    "supports_overwrite": True,
    "supports_merge": False,
    "requires_schema": False,
    "supports_binary": False,
    "supports_unstructured": False,
    "pagination": "none",
    "rate_limit_notes": "",
    "cdc_prerequisites": "",
    "auth_notes": "",
    "common_issues": [],
    "recommended_batch_size": 1000,
    "retry_transient_http_codes": [408, 429, 500, 502, 503, 504],
    "docs_url": "",
}

CAPABILITY_REGISTRY: dict[str, dict[str, Any]] = {
    # Relational / OLTP
    "postgresql": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": True,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "cdc_prerequisites": "Query-based CDC requires a monotonic cursor column. Log-based CDC uses logical decoding with the test_decoding output plugin; requires wal_level=logical and REPLICATION privileges.",
        "auth_notes": "Standard host/port/user/password. Use SSL in production.",
        "common_issues": ["The destination table must exist or 'create table' must be enabled.", "Integer overflow when target column is smaller than source values."],
        "recommended_batch_size": 1000,
    },
    "mysql": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": True,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": True,
        "supports_binary": True,
        "cdc_prerequisites": "Query-based CDC requires a monotonic cursor column. Log-based CDC reads MySQL binlog (ROW format) using python-mysql-replication; requires REPLICATION SLAVE and REPLICATION CLIENT privileges.",
        "auth_notes": "Standard host/port/user/password. Verify the user has REPLICATION CLIENT for CDC.",
        "common_issues": ["ZeroDateTime values can cause parsing errors; prefer NULL or a valid date.", "MySQL 8 caching_sha2_password may require a recent client."],
        "recommended_batch_size": 1000,
    },
    "sqlserver": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": True,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "cdc_prerequisites": "Query-based CDC: the source table must have a monotonic cursor column. Log-based CDC (SQL Server CDC capture jobs) is not implemented.",
        "common_issues": ["MERGE requires a unique key on the target.", "IDENTITY inserts may need SET IDENTITY_INSERT."],
        "recommended_batch_size": 2000,
    },
    "oracle": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": True,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "cdc_prerequisites": "Query-based CDC: the source table must have a monotonic cursor column. Log-based CDC (GoldenGate/LogMiner) is not implemented.",
        "common_issues": ["Table names may be case-sensitive. Use uppercase unless quoted.", "NUMBER with no precision maps to a wide decimal."],
        "recommended_batch_size": 2000,
    },
    "sqlite": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["SQLite has a single writer lock; large batch inserts should be committed in transactions."],
        "recommended_batch_size": 1000,
    },
    "duckdb": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["DuckDB is in-process; multiple writers to the same file can conflict."],
        "recommended_batch_size": 10000,
    },
    # Warehouses / lakes
    "snowflake": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "supports_unstructured": True,
        "cdc_prerequisites": "Query-based CDC only: the source table must have a monotonic cursor column. Snowpipe Streaming/log-based CDC is not implemented.",
        "common_issues": ["Large TIMESTAMP values can exceed Snowflake range.", "VARIANT is preferred for semi-structured JSON."],
        "recommended_batch_size": 10000,
    },
    "bigquery": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "supports_unstructured": True,
        "cdc_prerequisites": "Query-based CDC only: the source table must have a monotonic cursor column. BigQuery Storage Write CDC is not implemented.",
        "common_issues": ["Object tables are required for unstructured binary payloads.", "Partitioning can only be set at table creation."],
        "recommended_batch_size": 10000,
    },
    "redshift": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["VARCHAR columns without length may truncate. Use VARCHAR(max) or SUPER for JSON."],
        "recommended_batch_size": 5000,
    },
    "databricks": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": ["Binary ingestion may need a separate object metadata table.", "Delta schema enforcement can reject type changes."],
        "recommended_batch_size": 10000,
    },
    "synapse": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
    },
    # Document / NoSQL
    "mongodb": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": True,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "cdc_prerequisites": "Query-based CDC requires a monotonic cursor field. Log-based CDC uses MongoDB Change Streams; requires a replica set or MongoDB Atlas cluster.",
        "common_issues": [
            "Only the '_id' field is a hard primary key; other identifier fields may repeat.",
            "Decimal128 should be preserved for money; do not cast to float.",
            "ObjectId strings may be mapped as plain strings unless explicitly cast.",
        ],
        "recommended_batch_size": 2000,
    },
    "dynamodb": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": [
            "The partition key must have a consistent type (S/N/B). DataFlow infers the key type from the source sample.",
            "DynamoDB items cannot exceed 400 KB including attribute names.",
        ],
        "recommended_batch_size": 25,
    },
    "redis": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Key names should be deterministic. Composite keys are recommended for multi-column records."],
        "recommended_batch_size": 1000,
    },
    "elasticsearch": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Mappings are immutable for existing fields; reindexing is required for type changes."],
        "recommended_batch_size": 1000,
    },
    # Object stores
    "s3": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": [
            "Use the region-specific endpoint and verify bucket permissions.",
            "Large objects should be split into parts. S3 has no native UPDATE.",
        ],
        "recommended_batch_size": 1000,
    },
    "gcs": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": ["Use HMAC keys or service-account JSON for authentication.", "Bucket names are global and unique."],
        "recommended_batch_size": 1000,
    },
    "adls": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": ["Use Azure Storage Account key or service principal.", "Append to blobs requires read-modify-write."],
        "recommended_batch_size": 1000,
    },
    "minio": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": ["MinIO is S3-compatible; use the MinIO endpoint and region.", "Bucket policies may differ from AWS S3."],
        "recommended_batch_size": 1000,
    },
    # File formats
    "csv": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": False,
        "supports_unstructured": False,
        "common_issues": [
            "CSV has no type metadata; DataFlow infers types from sample values.",
            "Malformed quoting or embedded newlines can break parsing.",
            "Different locale decimal separators may misclassify numbers.",
        ],
        "recommended_batch_size": 10000,
    },
    "json": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": False,
        "supports_unstructured": True,
        "common_issues": ["JSON Lines files are preferred for streaming large JSON."],
        "recommended_batch_size": 10000,
    },
    "parquet": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["Parquet preserves types and nullability; ensure the writer schema matches the source."],
        "recommended_batch_size": 10000,
    },
    "avro": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["Schema evolution is limited; new fields must have defaults."],
        "recommended_batch_size": 10000,
    },
    "xml": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": False,
        "common_issues": ["XML attributes and namespaces are flattened to a path notation."],
        "recommended_batch_size": 1000,
    },
    "excel": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Excel stores dates as serial numbers; DataFlow normalizes them to ISO dates."],
        "recommended_batch_size": 5000,
    },
    # Streaming / messaging
    "kafka": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "streaming",
        "supports_cdc": True,
        "supports_streaming": True,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "cdc_prerequisites": "Query-based CDC only. Kafka Connect/Debezium log-based CDC is not implemented.",
        "common_issues": [
            "Exactly-once requires idempotent producers and transactional IDs.",
            "Schema Registry is recommended for Avro/Protobuf/JSON Schema.",
        ],
        "recommended_batch_size": 1000,
    },
    "kinesis": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "streaming",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Shards control throughput; scale shards before peak load."],
        "recommended_batch_size": 500,
    },
    "pubsub": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "streaming",
        "supports_cdc": False,
        "supports_streaming": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Pub/Sub is at-least-once by default; use ordering keys for ordered delivery."],
        "recommended_batch_size": 1000,
    },
    # SaaS / APIs
    "salesforce": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": True,
        "requires_schema": False,
        "supports_binary": False,
        "pagination": "bulk_api",
        "rate_limit_notes": "Bulk API 2.0 breaks large ingest into batches. Monitor daily API limits.",
        "common_issues": ["Long-running bulk jobs can fail if the daily limit is exceeded.", "External IDs are required for upsert."],
        "recommended_batch_size": 10000,
    },
    "servicenow": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "pagination": "sysparm_limit",
        "rate_limit_notes": "ServiceNow inbound REST API has rate limits; use pagination and avoid full scans.",
        "common_issues": ["Query length limits can truncate large filters."],
        "recommended_batch_size": 1000,
    },
    "msgraph": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "pagination": "odata_nextLink",
        "rate_limit_notes": "Microsoft Graph throttling uses Retry-After. Reduce concurrency on write-heavy workloads.",
        "common_issues": ["Throttling can pause large mailbox or SharePoint transfers."],
        "recommended_batch_size": 500,
    },
    "sharepoint": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "pagination": "odata_nextLink",
        "rate_limit_notes": "SharePoint REST API has throttling; respect Retry-After headers.",
        "common_issues": ["Large files should be uploaded in chunks."],
        "recommended_batch_size": 500,
    },
    "hubspot": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "pagination": "cursor",
        "rate_limit_notes": "HubSpot uses daily and burst rate limits per API.",
        "common_issues": ["Custom object properties may vary by account."],
        "recommended_batch_size": 100,
    },
    "zendesk": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "pagination": "cursor",
        "rate_limit_notes": "Zendesk has rate limits; use cursor pagination.",
        "recommended_batch_size": 100,
    },
    "shopify": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "pagination": "link_header",
        "rate_limit_notes": "Shopify has leaky-bucket API limits.",
        "common_issues": ["GraphQL pagination uses cursors; REST uses page_info."],
        "recommended_batch_size": 250,
    },
    "sftp": {
        "transfer_ready": True,
        "tier": TIER_HIGH,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "common_issues": ["Files are immutable; use a unique filename or append-only mode."],
        "recommended_batch_size": 1000,
    },
    # ERP / strategic
    "sap": {
        "transfer_ready": True,
        "tier": TIER_MEDIUM,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": True,
        "common_issues": ["SAP CDC in Azure uses ODP-based extraction. Choose the right extraction strategy."],
        "recommended_batch_size": 1000,
    },
    "workday": {
        "transfer_ready": True,
        "tier": TIER_MEDIUM,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "common_issues": ["Workday requires connector-specific models and partner adapters."],
        "recommended_batch_size": 1000,
    },
    "netsuite": {
        "transfer_ready": True,
        "tier": TIER_MEDIUM,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "common_issues": ["Use token-based auth and SuiteQL for large reads."],
        "recommended_batch_size": 1000,
    },
    "dynamics365": {
        "transfer_ready": True,
        "tier": TIER_MEDIUM,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "common_issues": ["Dynamics requires OAuth2 and may expose only selected entities."],
        "recommended_batch_size": 1000,
    },
    "google_workspace": {
        "transfer_ready": True,
        "tier": TIER_MEDIUM,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "common_issues": ["Google Workspace APIs are user-scoped and have quota limits."],
        "recommended_batch_size": 500,
    },
    # Open table formats
    "iceberg": {
        "transfer_ready": True,
        "tier": TIER_STRATEGIC,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["Schema evolution is supported but should be declared explicitly."],
        "recommended_batch_size": 10000,
    },
    "delta": {
        "transfer_ready": True,
        "tier": TIER_STRATEGIC,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["Delta schema enforcement can reject type changes; enable schema evolution if needed."],
        "recommended_batch_size": 10000,
    },
    "hudi": {
        "transfer_ready": True,
        "tier": TIER_STRATEGIC,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": ["Hudi requires a record key and pre-combine field for upserts."],
        "recommended_batch_size": 10000,
    },
    "generic_sql": {
        "transfer_ready": True,
        "tier": TIER_HIGHEST,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": True,
        "supports_append": True,
        "supports_overwrite": True,
        "supports_merge": True,
        "requires_schema": True,
        "supports_binary": True,
        "common_issues": [
            "Generic SQL uses SQLAlchemy. Provide the exact driver name and DSN.",
            "Not every SQL dialect supports all features; use the catalog for native dialects when available.",
        ],
        "recommended_batch_size": 1000,
    },
    "rest_api": {
        "transfer_ready": True,
        "tier": TIER_STRATEGIC,
        "pattern": "batch",
        "supports_cdc": False,
        "supports_streaming": False,
        "supports_upsert": False,
        "supports_append": True,
        "supports_overwrite": False,
        "supports_merge": False,
        "requires_schema": False,
        "supports_binary": True,
        "supports_unstructured": True,
        "pagination": "custom",
        "rate_limit_notes": "Generic REST connector requires explicit pagination and rate-limit handling.",
        "common_issues": ["Custom connectors need a spec or SDK. Document pagination, auth, and rate limits."],
        "recommended_batch_size": 100,
    },
}


# Aliases used by the UI, connectors, and driver layer.
CONNECTOR_ALIASES: dict[str, str] = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "mysql2": "mysql",
    "mariadb": "mysql",
    "mssql": "sqlserver",
    "sql_server": "sqlserver",
    "ms_sql": "sqlserver",
    "oracle_db": "oracle",
    "mongo": "mongodb",
    "documentdb": "mongodb",
    "document_db": "mongodb",
    "cosmos": "mongodb",
    "cosmos-mongodb": "mongodb",
    "cosmos_mongodb": "mongodb",
    "dynamo": "dynamodb",
    "dyn": "dynamodb",
    "elastic": "elasticsearch",
    "es": "elasticsearch",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "big_query": "bigquery",
    "bq": "bigquery",
    "gcp_bigquery": "bigquery",
    "azure_synapse": "synapse",
    "synapse_sql": "synapse",
    "s3_compatible": "minio",
    "gcp_storage": "gcs",
    "azure_blob": "adls",
    "azure_data_lake": "adls",
    "kafka_connect": "kafka",
    "confluent": "kafka",
    "salesforce_bulk": "salesforce",
    "sf": "salesforce",
    "sharepoint_online": "sharepoint",
    "onedrive": "sharepoint",
    "gsheets": "google_workspace",
    "gmail": "google_workspace",
    "rest": "rest_api",
    "api": "rest_api",
}


def _normalize_connector_id(key: str) -> str:
    k = (key or "").lower().strip()
    return CONNECTOR_ALIASES.get(k, k)


def _align_registry_honesty() -> None:
    """Demote transfer_ready on registry entries that have no real driver modules."""
    fiction = {
        "kafka", "kinesis", "pubsub", "iceberg", "delta", "hudi",
        "databricks", "synapse", "sap", "workday", "netsuite", "servicenow",
        "dynamics365", "msgraph", "google_workspace", "sharepoint",
        "shopify", "zendesk", "oracle", "sqlserver", "duckdb",
    }
    for key in fiction:
        if key in CAPABILITY_REGISTRY:
            CAPABILITY_REGISTRY[key]["transfer_ready"] = False


_align_registry_honesty()


def get_connector_capability(key: str) -> dict[str, Any]:
    """Return the capability record for a connector, with alias resolution.

    ``transfer_ready`` is forced to match the real driver capability table
    (``connector_capabilities._DRIVER_CAPS`` / file caps), not marketing catalog
    entries for kafka/iceberg/etc. that have no implemented modules.
    """
    normalized = _normalize_connector_id(key)
    cap = CAPABILITY_REGISTRY.get(normalized, DEFAULT_CAPABILITY).copy()
    cap["requested_key"] = key
    cap["normalized_key"] = normalized
    _FICTION = {
        "kafka", "kinesis", "pubsub", "iceberg", "delta", "hudi",
        "databricks", "synapse", "sap", "workday", "netsuite", "servicenow",
        "dynamics365", "msgraph", "google_workspace", "sharepoint",
        "shopify", "zendesk", "oracle", "sqlserver", "duckdb",
    }
    try:
        from src.transfer.connector_capabilities import (
            _DRIVER_CAPS,
            _FILE_CAPS,
            _source_only_ready,
            get_capabilities,
            resolve_driver_type,
            transfer_ready,
        )

        driver = resolve_driver_type(normalized)
        caps = get_capabilities(driver, normalized)
        # Honest: ready only when this key is a first-class driver/file, not a
        # SaaS catch-all (rest_api) mapping for a marketing catalog id.
        first_class = (
            normalized in _DRIVER_CAPS
            or normalized in _FILE_CAPS
            or normalized == "generic_sql"
            or driver in _FILE_CAPS
            and normalized in _FILE_CAPS
        )
        # Aliases that intentionally point at real drivers (e.g. minio→s3) are OK
        # when the registry key itself is not fiction.
        if normalized in _FICTION:
            cap["transfer_ready"] = False
        elif first_class or (driver in _DRIVER_CAPS and normalized == driver):
            cap["transfer_ready"] = bool(transfer_ready(caps) or _source_only_ready(caps))
        elif driver in _DRIVER_CAPS and normalized not in _FICTION and driver != "rest_api":
            cap["transfer_ready"] = bool(transfer_ready(caps) or _source_only_ready(caps))
        else:
            # Catch-all rest_api / unknown → not transfer_ready under this brand name
            cap["transfer_ready"] = False
        cap["driver_type"] = driver
        cap["driver_capabilities"] = caps
    except Exception:
        if normalized in _FICTION:
            cap["transfer_ready"] = False
    return cap


def list_connectors_by_tier() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {tier: [] for tier in TIER_ORDER}
    for key, cap in CAPABILITY_REGISTRY.items():
        out.setdefault(cap.get("tier", TIER_STRATEGIC), []).append(key)
    return out


def supports_streaming(key: str) -> bool:
    return bool(get_connector_capability(key).get("supports_streaming"))


def supports_cdc(key: str) -> bool:
    return bool(get_connector_capability(key).get("supports_cdc"))


def is_schemaless(key: str) -> bool:
    return not get_connector_capability(key).get("requires_schema", True)


def recommended_batch_size(key: str) -> int:
    return int(get_connector_capability(key).get("recommended_batch_size", 1000))


STRUCTURED_FILE_FORMATS: set[str] = {
    "csv", "tsv", "parquet", "avro", "excel", "xlsx", "xls", "ods", "feather", "arrow", "ipc", "orc"
}

SEMI_STRUCTURED_FILE_FORMATS: set[str] = {
    "json", "jsonl", "ndjson", "xml", "yaml", "yml", "toml", "bson", "msgpack", "protobuf"
}


def classify_payload(
    *,
    source_format: str = "",
    target_format: str = "",
    has_binary: bool = False,
    has_unstructured: bool = False,
    is_streaming: bool = False,
) -> dict[str, str]:
    """Classify the payload as structured, semi-structured, unstructured, binary, streaming, or mixed."""
    src = get_connector_capability(source_format)
    tgt = get_connector_capability(target_format)
    if is_streaming or src.get("pattern") == "streaming" or tgt.get("pattern") == "streaming":
        return {"shape": "streaming", "note": "Streaming/messaging payload"}

    if has_binary and has_unstructured:
        return {"shape": "mixed", "note": "Binary/unstructured payload with structured metadata"}
    if has_binary:
        return {"shape": "binary", "note": "Binary payload"}
    if has_unstructured:
        return {"shape": "unstructured", "note": "Unstructured text or media payload"}

    src_key = _normalize_connector_id(source_format)
    tgt_key = _normalize_connector_id(target_format)
    if src_key in STRUCTURED_FILE_FORMATS or tgt_key in STRUCTURED_FILE_FORMATS:
        return {"shape": "structured", "note": "Tabular file payload with rows and columns"}
    if src_key in SEMI_STRUCTURED_FILE_FORMATS or tgt_key in SEMI_STRUCTURED_FILE_FORMATS:
        return {"shape": "semi_structured", "note": "Hierarchical document payload without strict schema"}

    src_schemaless = src.get("requires_schema") is False
    tgt_schemaless = tgt.get("requires_schema") is False
    if src_schemaless or tgt_schemaless:
        if src.get("supports_unstructured") or tgt.get("supports_unstructured"):
            return {"shape": "semi_structured", "note": "Document or object payload without strict schema"}
        return {"shape": "structured", "note": "Tabular payload with inferred schema"}

    return {"shape": "structured", "note": "Structured tabular payload"}
