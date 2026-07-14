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

const BASE_DEFAULTS: Record<string, { host: string; port: number }> = {
  postgresql: { host: "localhost", port: 5432 },
  mysql: { host: "localhost", port: 3306 },
  mongodb: { host: "localhost", port: 27017 },
  snowflake: { host: "localhost", port: 443 },
  bigquery: { host: "bigquery.googleapis.com", port: 443 },
  redshift: { host: "localhost", port: 5439 },
  dynamodb: { host: "us-east-1", port: 443 },
  s3: { host: "", port: 443 },
  gcs: { host: "", port: 443 },
  google_cloud_storage: { host: "", port: 443 },
  adls: { host: "", port: 443 },
  redis: { host: "localhost", port: 6379 },
  elasticsearch: { host: "localhost", port: 9200 },
  sqlite: { host: "", port: 0 },
  generic_sql: { host: "localhost", port: 0 },
  csv: { host: "", port: 0 },
  tsv: { host: "", port: 0 },
  json: { host: "", port: 0 },
  jsonl: { host: "", port: 0 },
  ndjson: { host: "", port: 0 },
  excel: { host: "", port: 0 },
  parquet: { host: "", port: 0 },
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
  if (isGenericSql(id)) {
    return id;
  }

  return id.split("_")[0] || id;
}

export function isGenericSql(id: string): boolean {
  const type = id.toLowerCase().trim();
  if (!type) return false;
  return (
    type.includes("mssql") || type.includes("sql_server") || type.includes("sqlserver") || type.includes("microsoft_sql") || type.includes("azure_sql") ||
    type.includes("oracle") || type.includes("db2") || type.includes("teradata") || type.includes("netezza") ||
    type.includes("vertica") || type.includes("exasol") || type.includes("sybase") || type.includes("sap_ase") ||
    type.includes("sap_iq") || type.includes("sap_hana") || type.includes("hana") || type.includes("firebird") ||
    type.includes("h2") || type.includes("clickhouse") || type.includes("druid") || type.includes("pinot") ||
    type.includes("presto") || type.includes("trino") || type.includes("hive") || type.includes("spark") ||
    type.includes("impala") || type.includes("phoenix") || type.includes("duckdb") || type.includes("databricks") ||
    type.includes("greenplum") || type.includes("cratedb") || type.includes("questdb") || type.includes("doris") ||
    type.includes("starrocks") || type.includes("yellowbrick") || type.includes("actian") || type.includes("informix") ||
    type.includes("athena") || type.includes("synapse") || type.includes("dremio") || type.includes("firebolt") ||
    type.includes("risingwave") || type.includes("materialize") || type.includes("citus")
  );
}

export function isTransferLiveType(type: string): boolean {
  return TRANSFER_LIVE_TYPES.has(type);
}

export function isConnectOnlyType(type: string): boolean {
  return CONNECT_ONLY_TYPES.has(type);
}

export function resolveDriverType(catalogId: string): string {
  const t = resolveCatalogIdToType(catalogId);
  return isGenericSql(t) && t !== "generic_sql" ? "generic_sql" : t;
}

export function getConnectorLabel(type: string, item?: { label?: string } | null): string {
  if (item?.label) return item.label;
  const generic = GENERIC_SQL_DRIVERS.find((d) => d.id === type);
  if (generic) return generic.label;
  return type
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\b(Sql|Db)\b/g, (m) => (m === "Sql" ? "SQL" : "DB"));
}

export function getConnectorDefaults(type: string): { host: string; port: number; label: string } {
  const item = CONNECTOR_CATALOG.find((c) => c.id === type);
  const driver = resolveDriverType(type);
  const base = BASE_DEFAULTS[driver] || { host: "localhost", port: 0 };
  return { host: base.host, port: base.port, label: getConnectorLabel(type, item) };
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
    redshift: "postgresql+psycopg2://user:pass@host:5439/db",
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
