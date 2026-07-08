import { CONNECTOR_CATALOG } from "./types";

/** Map marketplace catalog id → connectable type used by API/forms */
export function resolveCatalogIdToType(catalogId: string): string {
  const id = catalogId.toLowerCase().trim();
  if (!id) return "mongodb";

  const direct = CONNECTOR_CATALOG.find((c) => c.id === id);
  if (direct) return direct.id;

  if (id.includes("dynamodb")) return "dynamodb";
  if (id.includes("postgres")) return "postgresql";
  if (id.includes("mongo")) return "mongodb";
  if (id.includes("mysql") || id.includes("mariadb")) return "mysql";
  if (id.includes("snowflake")) return "snowflake";
  if (id.includes("bigquery")) return "bigquery";
  if (id.includes("redshift")) return "redshift";
  if (id.includes("clickhouse")) return "clickhouse";
  if (id.includes("redis")) return "redis";
  if (id.includes("elastic")) return "elasticsearch";
  if (id.includes("kafka")) return "kafka";
  if (id.includes("s3") || id.includes("aws_s3")) return "s3";

  const base = id.replace(/___/g, "_").split("_").filter(Boolean)[0];
  const byBase = CONNECTOR_CATALOG.find((c) => c.id === base);
  return byBase?.id ?? base;
}

export function getConnectorDefaults(type: string): { host: string; port: number; label: string } {
  const item = CONNECTOR_CATALOG.find((c) => c.id === type);
  if (type === "dynamodb") return { host: "us-east-1", port: 443, label: "Amazon DynamoDB" };
  if (type === "bigquery") return { host: "bigquery.googleapis.com", port: 443, label: "BigQuery" };
  if (type === "s3") return { host: "s3.amazonaws.com", port: 443, label: "Amazon S3" };
  return {
    host: type === "mongodb" ? "localhost" : "localhost",
    port: item?.port ?? 5432,
    label: item?.label ?? type,
  };
}

export function isAwsConnector(type: string): boolean {
  return ["dynamodb", "s3", "redshift", "kinesis"].includes(type);
}

export function isConfigurableInStudio(type: string): boolean {
  return !["csv", "tsv", "json", "jsonl", "parquet", "avro", "excel"].includes(type);
}
