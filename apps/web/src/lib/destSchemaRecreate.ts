/**
 * Whether a full-refresh overwrite actually drops and recreates destination DDL.
 *
 * Mirrors ``services.db_type_utils.dest_schema_is_recreated_on_overwrite``.
 * Keep the two lists aligned — CRM / stream / vector dests upsert into live
 * Describe and must not skip Map's narrowing hold.
 */

const SCHEMALESS_DESTS = new Set(["mongodb", "dynamodb", "redis"]);

const NO_RELATIONAL_DDL_DESTS = new Set([
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
]);

const DESTS_WITHOUT_SCHEMA_RECREATE = new Set([
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
]);

const DB_TYPE_ALIASES: Record<string, string> = {
  mongo: "mongodb",
  "mongodb+srv": "mongodb",
  mongodb_atlas: "mongodb",
  atlas: "mongodb",
  "cosmos-mongodb": "mongodb",
  cosmos_mongodb: "mongodb",
  documentdb: "mongodb",
  aws_documentdb: "mongodb",
  dynamo: "dynamodb",
  "redis-kv": "redis",
  redis_kv: "redis",
};

export function normalizeDestKind(destDbType: string | null | undefined): string {
  const raw = String(destDbType || "").trim().toLowerCase().replace(/ /g, "_");
  if (!raw) return "";
  if (DB_TYPE_ALIASES[raw]) return DB_TYPE_ALIASES[raw];
  if (raw.startsWith("mongodb")) return "mongodb";
  if (raw.startsWith("dynamodb")) return "dynamodb";
  if (raw.startsWith("redis")) return "redis";
  return raw;
}

export function destSchemaIsRecreatedOnOverwrite(
  destDbType: string | null | undefined,
): boolean {
  const kind = normalizeDestKind(destDbType);
  if (!kind) return true;
  if (DESTS_WITHOUT_SCHEMA_RECREATE.has(kind)) return false;
  if (SCHEMALESS_DESTS.has(kind) || NO_RELATIONAL_DDL_DESTS.has(kind)) return false;
  return true;
}

export function isOverwriteSyncMode(syncMode: string | null | undefined): boolean {
  const mode = String(syncMode || "").trim().toLowerCase();
  return mode === "full_refresh_overwrite" || mode === "overwrite";
}

/**
 * True when this run will drop the live destination table and recreate it.
 * The standing column types are then doomed — G3/G6/Map must not treat them
 * as the write carrier. G19 is the gate that names a narrowing replacement.
 */
export function liveCarrierIsDoomedOnThisRun(opts: {
  syncMode: string | null | undefined;
  destDbType: string | null | undefined;
  destTableExists: boolean | null | undefined;
}): boolean {
  return (
    opts.destTableExists === true
    && isOverwriteSyncMode(opts.syncMode)
    && destSchemaIsRecreatedOnOverwrite(opts.destDbType)
  );
}

export function stampLiveCarrierDoomed<
  T extends { existsInDestination?: boolean; liveCarrierDoomed?: boolean },
>(mappings: T[], doomed: boolean): T[] {
  let changed = false;
  const next = mappings.map((m) => {
    const flag = Boolean(doomed && m.existsInDestination === true);
    if (Boolean(m.liveCarrierDoomed) === flag) return m;
    changed = true;
    return { ...m, liveCarrierDoomed: flag || undefined };
  });
  return changed ? next : mappings;
}
