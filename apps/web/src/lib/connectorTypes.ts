import { CONNECTOR_CATALOG } from "./types";

/** Implemented driver types — must match apps/api/src/transfer/connector_capabilities.py */
export const TRANSFER_LIVE_TYPES = new Set([
  "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet",
  "dynamodb", "s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
  "sqlite", "generic_sql",
]);

export const CONNECT_ONLY_TYPES = new Set<string>([]);

const CATALOG_ALIASES: Record<string, string> = {
  csv___tsv: "csv",
  amazon_s3: "s3",
  aws_s3: "s3",
  google_cloud_storage: "gcs",
  gcs: "gcs",
  minio: "s3",
  wasabi: "s3",
  backblaze_b2: "s3",
  digitalocean_spaces: "s3",
  cloudflare_r2: "s3",
  amazon_redshift: "redshift",
  google_bigquery: "bigquery",
  opensearch: "elasticsearch",
  amazon_elasticsearch: "elasticsearch",
  elastic_cloud: "elasticsearch",
  amazon_dynamodb: "dynamodb",
  planetscale: "mysql",
  mariadb: "mysql",
  percona_mysql: "mysql",
  amazon_aurora_mysql: "mysql",
  amazon_aurora_postgresql: "postgresql",
  amazon_rds_postgresql: "postgresql",
  amazon_rds_mysql: "mysql",
  google_cloud_sql_postgresql: "postgresql",
  google_cloud_sql_mysql: "mysql",
  azure_database_postgresql: "postgresql",
  azure_database_mysql: "mysql",
  supabase: "postgresql",
  neon: "postgresql",
  timescaledb: "postgresql",
  cockroachdb: "postgresql",
  jsonl: "jsonl",
  ndjson: "ndjson",
};

/** Map marketplace catalog id → connectable type used by API/forms (strict) */
export function resolveCatalogIdToType(catalogId: string): string {
  const id = catalogId.toLowerCase().trim();
  if (!id) return "mongodb";

  if (CATALOG_ALIASES[id]) return CATALOG_ALIASES[id];
  if (TRANSFER_LIVE_TYPES.has(id) || CONNECT_ONLY_TYPES.has(id)) return id;

  const direct = CONNECTOR_CATALOG.find((c) => c.id === id);
  if (direct && (TRANSFER_LIVE_TYPES.has(direct.id) || CONNECT_ONLY_TYPES.has(direct.id))) {
    return direct.id;
  }

  // Substring match for wire-compatible databases / object stores / warehouses.
  if (id.includes("postgres")) return "postgresql";
  if (id.includes("mongo") || id.includes("documentdb")) return "mongodb";
  if (id.includes("mysql") || id.includes("mariadb") || id.includes("percona") || id.includes("planetscale") || id.includes("vitess") || id.includes("tidb") || id.includes("oceanbase") || id.includes("polardb") || id.includes("singlestore") || id.includes("gaussdb") || id.includes("goldendb")) return "mysql";
  if (id.includes("aurora")) return "mysql";
  if (id.includes("snowflake")) return "snowflake";
  if (id.includes("bigquery")) return "bigquery";
  if (id.includes("dynamodb")) return "dynamodb";
  if (id.includes("redis") || id.includes("valkey") || id.includes("keydb") || id.includes("dragonfly")) return "redis";
  if (id.includes("elastic") || id.includes("opensearch")) return "elasticsearch";
  if (id.includes("redshift")) return "redshift";
  if (id.includes("cockroach")) return "postgresql";
  if (id.includes("yugabyte")) return "postgresql";
  if (id.includes("timescale")) return "postgresql";
  if (id.includes("alloydb")) return "postgresql";
  if (id.includes("supabase")) return "postgresql";
  if (id.includes("neon")) return "postgresql";
  if (id.includes("gcs") || id.includes("google_cloud_storage") || id.includes("google_cloud_sql")) return "gcs";
  if (id.includes("minio") || id.includes("wasabi") || id.includes("backblaze") || id.includes("spaces") || id.includes("object_storage") || id.includes("r2") || id.includes("s3_compatible") || id.includes("s3_compatible_storage")) return "s3";
  if (id.includes("s3") || id.includes("aws_s3")) return "s3";
  if (id.includes("parquet")) return "parquet";
  if (id.includes("jsonl") || id.includes("ndjson")) return "jsonl";
  if (id.includes("excel") || id.endsWith(".xlsx")) return "excel";
  if (id.includes("json")) return "json";
  if (id.includes("csv") || id.includes("tsv")) return "csv";
  if (id.includes("sqlite")) return "sqlite";

  // Generic SQL fallback — any SQL engine with a SQLAlchemy dialect is routed through generic_sql.
  if (
    id.includes("mssql") || id.includes("sql_server") || id.includes("microsoft_sql") || id.includes("azure_sql") ||
    id.includes("oracle") || id.includes("db2") || id.includes("teradata") || id.includes("netezza") ||
    id.includes("vertica") || id.includes("exasol") || id.includes("sybase") || id.includes("sap_ase") ||
    id.includes("sap_iq") || id.includes("sap_hana") || id.includes("hana") || id.includes("firebird") ||
    id.includes("h2") || id.includes("clickhouse") || id.includes("druid") || id.includes("pinot") ||
    id.includes("presto") || id.includes("trino") || id.includes("hive") || id.includes("spark") ||
    id.includes("impala") || id.includes("phoenix") || id.includes("duckdb") || id.includes("databricks") ||
    id.includes("greenplum") || id.includes("cratedb") || id.includes("questdb") || id.includes("doris") ||
    id.includes("starrocks") || id.includes("yellowbrick") || id.includes("actian") || id.includes("informix") ||
    id.includes("athena") || id.includes("synapse") || id.includes("dremio") || id.includes("firebolt") ||
    id.includes("risingwave") || id.includes("materialize") || id.includes("citus")
  ) {
    return "generic_sql";
  }

  return id.split("_")[0] || id;
}

export function isTransferLiveType(type: string): boolean {
  return TRANSFER_LIVE_TYPES.has(type);
}

export function isConnectOnlyType(type: string): boolean {
  return CONNECT_ONLY_TYPES.has(type);
}

export function getConnectorDefaults(type: string): { host: string; port: number; label: string } {
  const item = CONNECTOR_CATALOG.find((c) => c.id === type);
  if (type === "mongodb") return { host: "localhost", port: 27017, label: "MongoDB" };
  if (type === "mysql") return { host: "localhost", port: 3306, label: "MySQL" };
  if (type === "postgresql") return { host: "localhost", port: 5432, label: "PostgreSQL" };
  if (type === "dynamodb") return { host: "us-east-1", port: 443, label: "Amazon DynamoDB" };
  if (type === "bigquery") return { host: "bigquery.googleapis.com", port: 443, label: "BigQuery" };
  if (type === "snowflake") return { host: "localhost", port: 443, label: "Snowflake" };
  if (type === "redshift") return { host: "localhost", port: 5439, label: "Amazon Redshift" };
  if (type === "gcs") return { host: "", port: 443, label: "Google Cloud Storage" };
  if (type === "redis") return { host: "localhost", port: 6379, label: "Redis" };
  if (type === "elasticsearch") return { host: "localhost", port: 9200, label: "Elasticsearch" };
  if (type === "sqlite") return { host: "", port: 0, label: "SQLite" };
  if (type === "generic_sql") return { host: "", port: 0, label: "Generic SQL (any SQLAlchemy engine)" };
  if (["csv", "tsv", "json", "jsonl", "ndjson", "parquet", "excel"].includes(type)) {
    return { host: "", port: 0, label: item?.label ?? type };
  }
  return {
    host: "localhost",
    port: item?.port ?? 5432,
    label: item?.label ?? type,
  };
}

export function isAwsConnector(type: string): boolean {
  return ["dynamodb", "s3", "redshift", "kinesis"].includes(type);
}

export function isGcpConnector(type: string): boolean {
  return ["bigquery", "gcs", "google_cloud_storage"].includes(type);
}

export function isConfigurableInStudio(type: string): boolean {
  return !["csv", "tsv", "json", "jsonl", "parquet", "avro", "excel"].includes(type);
}

export const GENERIC_SQL_DRIVERS = [
  { id: "postgresql", label: "PostgreSQL" },
  { id: "mysql", label: "MySQL / MariaDB" },
  { id: "mssql", label: "SQL Server" },
  { id: "oracle", label: "Oracle" },
  { id: "sqlite", label: "SQLite" },
  { id: "duckdb", label: "DuckDB" },
  { id: "clickhouse", label: "ClickHouse" },
  { id: "trino", label: "Trino / Presto" },
  { id: "dremio", label: "Dremio" },
  { id: "firebolt", label: "Firebolt" },
  { id: "risingwave", label: "RisingWave" },
  { id: "materialize", label: "Materialize" },
  { id: "db2", label: "IBM DB2" },
  { id: "teradata", label: "Teradata" },
  { id: "sap_hana", label: "SAP HANA" },
  { id: "informix", label: "Informix" },
  { id: "athena", label: "Amazon Athena" },
  { id: "synapse", label: "Azure Synapse" },
];

export function getGenericSqlPlaceholder(driver: string): string {
  const d = (driver || "postgresql").toLowerCase();
  const map: Record<string, string> = {
    postgresql: "postgresql+psycopg2://user:pass@host:5432/db",
    mysql: "mysql+pymysql://user:pass@host:3306/db",
    mssql: "mssql+pyodbc://user:pass@host:1433/db",
    oracle: "oracle+oracledb://user:pass@host:1521/db",
    sqlite: "sqlite:////path/to/db.sqlite",
    duckdb: "duckdb:////path/to/db.duckdb",
    clickhouse: "clickhouse+http://user:pass@host:8123/db",
    trino: "trino://user:pass@host:8080/catalog",
    dremio: "dremio+flight://user:pass@host:32010",
    firebolt: "firebolt://user:pass@host:443/db",
    risingwave: "postgresql+psycopg2://user:pass@host:4566/db",
    materialize: "postgresql+psycopg2://user:pass@host:6875/db",
    db2: "db2+ibm_db://user:pass@host:50000/db",
    teradata: "teradatasql://user:pass@host:1025/db",
    sap_hana: "hana+hdbcli://user:pass@host:30015/db",
    informix: "informix+pyodbc://user:pass@host:9088/db",
    athena: "awsathena+rest://@athena.region.amazonaws.com:443/?s3_staging_dir=s3://bucket",
    synapse: "mssql+pyodbc://user:pass@host:1433/db",
  };
  return map[d] || "driver://user:pass@host:port/db";
}

export function defaultPortForType(type: string): number {
  return getConnectorDefaults(type).port;
}
