"""Destination dialect facts: engine aliases and collation compatibility.

Every fidelity rule starts by asking *which engine is this really* — a config
may say ``postgres``, ``pg``, ``aws_aurora_postgresql`` or ``cockroachdb`` and
mean the same carrier rules. Keeping that answer (and the collation pairing it
implies) in one leaf module means Map, Validate and Execute cannot disagree
about the destination they are reasoning over.
"""

from __future__ import annotations


def _normalize_dest_db(db_type: str | None) -> str:
    """Canonical destination engine id for DDL / cap lookups."""
    db = (db_type or "").strip().lower()
    # PostgreSQL family — DDL_TYPES / caps keyed only on ``postgresql``.
    # Without this, create-new invents TEXT for NUMBER/DATE/BOOLEAN with soft-pass.
    if db in {
        "postgres",
        "pg",
        "postgresql",
        "cockroachdb",
        "cockroach",
        "timescaledb",
        "timescale",
        "alloydb",
        "yugabytedb",
        "yugabyte",
        "citus",
        "supabase",
        "supabase_db",
        "greenplum",
        "greenplum_cloud",
        "neon",
        "neon_serverless",
        "azure_postgres",
        "aws_rds_postgres",
        "rds_postgres",
        "aurora",
        "aurora_postgres",
        "aurora-postgresql",
        "pgbouncer",
        "cloudsql_postgres",
        "gcp_cloud_sql_postgres",
        "cloud_sql_postgres",
        "opengauss",
        "open_gauss",
        "kingbase",
        "vastbase",
        "hologres",
        "tdsql",
        "materialize",
        "risingwave",
    }:
        return "postgresql"
    if db in {
        "mariadb",
        "tidb",
        "tidb_cloud",
        "mysql2",
        "aurora_mysql",
        "aurora-mysql",
        "singlestore",
        "memsql",
        "cloudsql_mysql",
        "gcp_cloud_sql_mysql",
        "rds_mysql",
        "maria",
        "percona",
        "doris",
        "starrocks",
        "oceanbase",
        "selectdb",
        # Product catalog wires these through mysql+pymysql (not PG wire).
        "polardb",
        "gaussdb",
        "goldendb",
        "vitess",
        "planetscale",
        "mysql_planetscale",
    }:
        return "mysql"
    if db in {
        "mongo",
        "mongodb",
        "documentdb",
        "document_db",
        "cosmos",
        "cosmos-mongodb",
        "cosmos_mongodb",
        "cosmosdb",
        "firestore",
    }:
        return "mongodb"
    if db in {
        "spark",
        "delta",
        "delta_lake",
        "databricks_sql",
        "unity_catalog",
        "databricks_azure",
        "databricks_aws",
        "databricks_gcp",
        "hive",
        "impala",
        "emr",
        "glue",
        "synapse_spark",
        "flink",
        "maxcompute",
        "odps",
        "databend",
    }:
        return "databricks"
    if db in {"apache_iceberg", "iceberg_rest", "nessie"}:
        return "iceberg"
    if db in {"opensearch", "amazon_elasticsearch", "elastic_cloud"}:
        return "elasticsearch"
    if db in {"amazon_dynamodb"}:
        return "dynamodb"
    if db in {"redis-kv", "redis_kv"}:
        return "redis"
    if db in {"ch", "clickhouse_cloud", "bytehouse"}:
        return "clickhouse"
    if db in {"redshift", "redshift_serverless", "amazon_redshift"}:
        return "redshift"
    if db in {"snowflake", "snowflake_aws", "snowflake_azure", "snowflake_gcp"}:
        return "snowflake"
    if db in {"athena", "amazon_athena", "aws_athena", "dremio"}:
        return "trino"
    # Spanner is NOT BigQuery — inventing DATETIME/BIGNUMERIC/TIME is illegal DDL.
    if db in {"spanner", "google_spanner", "cloud_spanner"}:
        return "spanner"
    if db in {"duckdb", "motherduck"}:
        return "duckdb"
    if db in {"sqlite", "libsql", "turso"}:
        return "sqlite"
    # Microsoft T-SQL family — one DDL SSOT (NVARCHAR / BIT / DATETIME2).
    if db in {
        "mssql",
        "azure_sql",
        "azure_sql_db",
        "azure_sql_mi",
        "azuresql",
        "azure-sql",
        "sqlazure",
        "synapse",
        "azure_synapse",
        "sql_server",
        "fabric",
        "fabric_sql",
        "fabric_warehouse",
    }:
        return "sqlserver"
    # No dedicated DDL map yet — generic SQL rather than soft-pass TEXT.
    if db in {
        "db2",
        "ibm_db2",
        "ibm-db2",
        "db2luw",
        "cassandra",
        "bigtable",
        "google_bigtable",
        "teradata",
        "vertica",
        "netezza",
        "exasol",
        "crate",
        "cratedb",
        "questdb",
        "pinot",
        "druid",
        "kylin",
        "beam",
        "datafusion",
    }:
        return "generic_sql"
    return db


def _collation_compatible_with_dest(db: str, collation: str) -> bool:
    """Refuse cross-engine invent (MySQL utf8mb4_* on PG, etc.)."""
    coll = (collation or "").strip()
    if not coll or len(coll) > 128:
        return False
    upper = coll.upper()
    # MySQL-only tokens — do not treat SQL Server Latin1_General_CI_* as MySQL.
    mysqlish = bool(
        "UTF8MB4" in upper
        or "UTF8MB3" in upper
        or "_0900_" in upper
        or "_AI_CI" in upper
        or "_AS_CI" in upper
        or (
            upper.endswith(("_UNICODE_CI", "_GENERAL_CI"))
            and "LATIN1_GENERAL" not in upper
            and not upper.startswith("SQL_")
        )
    )
    windowish = bool(
        re.search(r"LATIN1_GENERAL|SQL_LATIN|_C[IS]_A[IS]", upper)
        or upper.startswith("SQL_")
    )
    if db in {"mysql", "mariadb"}:
        if not re.match(r"^[A-Za-z0-9_]+$", coll):
            return False
        if windowish:
            return False
        return bool(
            re.search(r"UTF8|LATIN1|ASCII|UCA|BINARY|UNICODE|GENERAL", upper)
            or upper.endswith(("_CI", "_CS", "_BIN"))
        )
    if db == "sqlserver":
        if not re.match(r"^[A-Za-z0-9_]+$", coll):
            return False
        if mysqlish:
            return False
        return bool(
            re.search(r"LATIN|SQL_|JAPANESE|CHINESE|KOREAN|CYRILLIC|_C[IS]_A[IS]", upper)
        )
    if db in {"postgresql", "redshift"}:
        if coll.lower() in {"default", "c", "posix"}:
            return False
        # ICU / libc names only — never invent MySQL/SS collations on PG.
        if mysqlish or windowish:
            return False
        return bool(re.match(r"^[A-Za-z0-9_.\-]+$", coll))
    return False
