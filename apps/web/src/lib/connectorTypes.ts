import { CONNECTOR_CATALOG } from "./types";

/** Implemented driver types — must match apps/api/src/transfer/connector_capabilities.py */
export const TRANSFER_LIVE_TYPES = new Set([
  "postgresql", "mysql", "mongodb", "snowflake", "bigquery", "redshift",
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet",
  "dynamodb", "s3", "gcs", "google_cloud_storage", "redis", "elasticsearch",
]);

export const CONNECT_ONLY_TYPES = new Set<string>([]);

const CATALOG_ALIASES: Record<string, string> = {
  csv___tsv: "csv",
  amazon_s3: "s3",
  aws_s3: "s3",
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

  // Substring match only for known implemented drivers
  if (id.includes("postgres")) return "postgresql";
  if (id.includes("mongo")) return "mongodb";
  if (id.includes("mysql") || id.includes("mariadb")) return "mysql";
  if (id.includes("snowflake")) return "snowflake";
  if (id.includes("bigquery")) return "bigquery";
  if (id.includes("dynamodb")) return "dynamodb";
  if (id.includes("redis")) return "redis";
  if (id.includes("elastic")) return "elasticsearch";
  if (id.includes("redshift")) return "redshift";
  if (id.includes("gcs") || id.includes("google_cloud")) return "gcs";
  if (id.includes("parquet")) return "parquet";
  if (id.includes("jsonl") || id.includes("ndjson")) return "jsonl";
  if (id.includes("excel") || id.endsWith(".xlsx")) return "excel";
  if (id.includes("json")) return "json";
  if (id.includes("csv") || id.includes("tsv")) return "csv";

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

export function defaultPortForType(type: string): number {
  return getConnectorDefaults(type).port;
}
