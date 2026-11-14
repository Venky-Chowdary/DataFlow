"""Engine names as people read them ("PostgreSQL", not "postgresql").

Text an operator reads (progress lines, event logs) names the engine and the
database, never a host and port: the address is infrastructure detail, and an
event log is often pasted into tickets and screen shares (UX-34).
"""

from __future__ import annotations

_NAMES = {
    "postgresql": "PostgreSQL",
    "postgres": "PostgreSQL",
    "mysql": "MySQL",
    "mariadb": "MariaDB",
    "sqlserver": "SQL Server",
    "mssql": "SQL Server",
    "oracle": "Oracle",
    "mongodb": "MongoDB",
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "redshift": "Amazon Redshift",
    "sqlite": "SQLite",
    "duckdb": "DuckDB",
    "databricks": "Databricks",
    "clickhouse": "ClickHouse",
    "dynamodb": "DynamoDB",
    "elasticsearch": "Elasticsearch",
    "kafka": "Kafka",
    "redis": "Redis",
    "s3": "Amazon S3",
    "gcs": "Google Cloud Storage",
    "adls": "Azure Data Lake Storage",
}


def engine_display_name(engine: str | None) -> str:
    """The engine's product name; an unknown id is returned as given."""
    key = str(engine or "").strip().lower()
    return _NAMES.get(key, str(engine or "").strip())
