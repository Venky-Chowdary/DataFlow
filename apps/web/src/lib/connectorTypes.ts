import { CONNECTOR_CATALOG } from "./types";
import { GENERIC_SQL_INFO } from "./genericSqlMap";

/** Implemented driver types — must match apps/api/src/transfer/connector_capabilities.py */
export const TRANSFER_LIVE_TYPES = new Set([
  "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet",
  "dynamodb", "s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
  "sqlite", "generic_sql",
]);

export const CONNECT_ONLY_TYPES = new Set<string>([]);

/** SQL engines that are first-class drivers; the rest of generic SQL IDs route through generic_sql. */
const FIRST_CLASS_SQL = new Set(["mysql", "postgresql", "redshift", "sqlite"]);

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
  if (id.includes("mysql") || id.includes("mariadb") || id.includes("percona") || id.includes("planetscale")) return "mysql";
  if (id.includes("aurora")) return "mysql";
  if (id.includes("snowflake")) return "snowflake";
  if (id.includes("bigquery")) return "bigquery";
  if (id.includes("dynamodb")) return "dynamodb";
  if (id.includes("redis") || id.includes("valkey") || id.includes("keydb") || id.includes("dragonfly")) return "redis";
  if (id.includes("elastic") || id.includes("opensearch")) return "elasticsearch";
  if (id.includes("redshift")) return "redshift";
  if (id.includes("cockroach")) return "postgresql";
  if (id.includes("timescale")) return "postgresql";
  if (id.includes("alloydb")) return "postgresql";
  if (id.includes("supabase")) return "postgresql";
  if (id.includes("neon")) return "postgresql";
  if (id.includes("gcs") || id.includes("google_cloud_storage")) return "gcs";
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
  return type in GENERIC_SQL_INFO && !FIRST_CLASS_SQL.has(type);
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
    .replace(/\b(Sql|Db|Rds|Ibm|Db2|Aws|Gcs|Gcp|Azure|Api|Http|Url|Uri|Uid|Ssl|Ssh|Smtp|Pop3|Imap|Tidb)\b/g, (m) => {
      const map: Record<string, string> = {
        Sql: "SQL", Db: "DB", Rds: "RDS", Ibm: "IBM", Db2: "DB2", Aws: "AWS",
        Gcs: "GCS", Gcp: "GCP", Azure: "Azure", Api: "API", Http: "HTTP",
        Url: "URL", Uri: "URI", Uid: "UID", Ssl: "SSL", Ssh: "SSH",
        Smtp: "SMTP", Pop3: "POP3", Imap: "IMAP", Tidb: "TiDB",
      };
      return map[m] || m.toUpperCase();
    });
}

export function getConnectorDefaults(type: string): { host: string; port: number; label: string } {
  const item = CONNECTOR_CATALOG.find((c) => c.id === type);
  const driver = resolveDriverType(type);
  const label = getConnectorLabel(type, item);

  if (isGenericSql(type)) {
    const info = GENERIC_SQL_INFO[type];
    if (info) {
      const host = info.base === "duckdb" || info.base === "sqlite" ? "" : "localhost";
      return { host, port: info.port, label };
    }
  }

  const base = BASE_DEFAULTS[driver] || { host: "localhost", port: 0 };
  return { host: base.host, port: base.port, label };
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

export function getGenericSqlBase(type: string): string | undefined {
  const info = GENERIC_SQL_INFO[type.toLowerCase().trim()];
  return info?.base;
}

export function getGenericSqlPort(type: string): number {
  return GENERIC_SQL_INFO[type.toLowerCase().trim()]?.port ?? 0;
}

export function getGenericSqlGroup(type: string): string {
  const normalized = resolveCatalogIdToType(type);
  return getGenericSqlBase(normalized) || normalized;
}

export function getGenericSqlPlaceholder(type: string): string {
  const t = type.toLowerCase().trim();
  const info = GENERIC_SQL_INFO[t];
  if (!info) return "driver://user:pass@host:port/db";
  const { base, port } = info;

  if (base === "sqlite") return "sqlite:////path/to/db.sqlite";
  if (base === "duckdb") return "duckdb:////path/to/db.duckdb";
  if (t === "dremio" || base === "dremio+flight") return `dremio+flight://user:pass@host:${port}`;
  if (t === "athena" || base === "awsathena+rest") return `awsathena+rest://@athena.region.amazonaws.com:${port}/?s3_staging_dir=s3://bucket`;
  if (base === "presto" || base === "trino") return `${base}://user:pass@host:${port}/catalog`;
  if (base === "druid") return `druid://user:pass@host:${port}/druid/v2/sql`;

  return `${base}://user:pass@host:${port}/db`;
}

export function defaultPortForType(type: string): number {
  return getConnectorDefaults(type).port;
}
