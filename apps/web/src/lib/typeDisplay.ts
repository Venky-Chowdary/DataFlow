/** CSS class for logical / SQL data types — drives badges in the UI. */

export type TypeFamily =
  | "int"
  | "decimal"
  | "bool"
  | "temporal"
  | "json"
  | "uuid"
  | "binary"
  | "string";

/** Canonical destination types offered in Map — covers warehouse + document common cases. */
export const LOGICAL_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "VARCHAR", label: "VARCHAR — text", family: "string" },
  { value: "TEXT", label: "TEXT — long text", family: "string" },
  { value: "INTEGER", label: "INTEGER", family: "int" },
  { value: "BIGINT", label: "BIGINT — 64-bit", family: "int" },
  { value: "SMALLINT", label: "SMALLINT", family: "int" },
  { value: "DECIMAL", label: "DECIMAL — precise number", family: "decimal" },
  { value: "NUMERIC", label: "NUMERIC", family: "decimal" },
  { value: "FLOAT", label: "FLOAT", family: "decimal" },
  { value: "DOUBLE", label: "DOUBLE", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "TIMESTAMPTZ", label: "TIMESTAMPTZ", family: "temporal" },
  { value: "JSON", label: "JSON / document", family: "json" },
  { value: "JSONB", label: "JSONB", family: "json" },
  { value: "ARRAY", label: "ARRAY", family: "json" },
  { value: "UUID", label: "UUID", family: "uuid" },
  { value: "BINARY", label: "BINARY / bytes", family: "binary" },
  { value: "BYTEA", label: "BYTEA", family: "binary" },
  { value: "VECTOR", label: "VECTOR — embedding", family: "binary" },
  { value: "INTERVAL", label: "INTERVAL — duration", family: "temporal" },
  { value: "GEOGRAPHY", label: "GEOGRAPHY / GEOMETRY", family: "json" },
];

/** Snowflake-native DDL labels — Map should show what CREATE will actually use. */
export const SNOWFLAKE_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "VARCHAR", label: "VARCHAR — text", family: "string" },
  { value: "NUMBER(38,0)", label: "NUMBER(38,0) — integer", family: "int" },
  { value: "NUMBER(38,10)", label: "NUMBER(38,10) — decimal", family: "decimal" },
  { value: "FLOAT", label: "FLOAT", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "TIMESTAMP_TZ", label: "TIMESTAMP_TZ", family: "temporal" },
  { value: "TIMESTAMP_NTZ", label: "TIMESTAMP_NTZ", family: "temporal" },
  { value: "TIMESTAMP_LTZ", label: "TIMESTAMP_LTZ", family: "temporal" },
  { value: "VARIANT", label: "VARIANT — semi-structured", family: "json" },
  { value: "BINARY", label: "BINARY / bytes", family: "binary" },
  { value: "GEOGRAPHY", label: "GEOGRAPHY", family: "json" },
  { value: "INTERVAL", label: "INTERVAL — stored as VARCHAR", family: "temporal" },
  { value: "VECTOR", label: "VECTOR(FLOAT, n) — set dimension", family: "binary" },
];

export const BIGQUERY_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "STRING", label: "STRING", family: "string" },
  { value: "INT64", label: "INT64", family: "int" },
  { value: "BIGNUMERIC", label: "BIGNUMERIC", family: "decimal" },
  { value: "FLOAT64", label: "FLOAT64", family: "decimal" },
  { value: "BOOL", label: "BOOL", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "JSON", label: "JSON", family: "json" },
  { value: "BYTES", label: "BYTES", family: "binary" },
  { value: "GEOGRAPHY", label: "GEOGRAPHY", family: "json" },
  { value: "INTERVAL", label: "INTERVAL — native duration", family: "temporal" },
];

export const MYSQL_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "TEXT", label: "TEXT", family: "string" },
  { value: "LONGTEXT", label: "LONGTEXT", family: "string" },
  { value: "VARCHAR(255)", label: "VARCHAR(255)", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INT", label: "INT", family: "int" },
  { value: "DECIMAL(38,15)", label: "DECIMAL(38,15)", family: "decimal" },
  { value: "FLOAT", label: "FLOAT", family: "decimal" },
  { value: "DOUBLE", label: "DOUBLE", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "DATETIME(6)", label: "DATETIME(6)", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "JSON", label: "JSON", family: "json" },
  { value: "LONGBLOB", label: "LONGBLOB", family: "binary" },
];

export const POSTGRES_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "TEXT", label: "TEXT", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INTEGER", label: "INTEGER", family: "int" },
  { value: "NUMERIC", label: "NUMERIC", family: "decimal" },
  { value: "DOUBLE PRECISION", label: "DOUBLE PRECISION", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "TIMESTAMPTZ", label: "TIMESTAMPTZ", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP — without time zone", family: "temporal" },
  { value: "UUID", label: "UUID", family: "uuid" },
  { value: "JSONB", label: "JSONB", family: "json" },
  { value: "BYTEA", label: "BYTEA", family: "binary" },
  { value: "VECTOR", label: "VECTOR — pgvector", family: "binary" },
  { value: "INTERVAL", label: "INTERVAL", family: "temporal" },
  { value: "GEOGRAPHY", label: "GEOGRAPHY / GEOMETRY", family: "json" },
  { value: "INET", label: "INET — identity text", family: "string" },
  { value: "MONEY", label: "MONEY — prefer NUMERIC", family: "decimal" },
];

export const REDSHIFT_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "VARCHAR", label: "VARCHAR", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INTEGER", label: "INTEGER", family: "int" },
  { value: "NUMERIC(38,10)", label: "NUMERIC(38,10)", family: "decimal" },
  { value: "DOUBLE PRECISION", label: "DOUBLE PRECISION", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "TIMESTAMPTZ", label: "TIMESTAMPTZ", family: "temporal" },
  { value: "SUPER", label: "SUPER — semi-structured", family: "json" },
  { value: "VARBYTE", label: "VARBYTE", family: "binary" },
  { value: "GEOMETRY", label: "GEOMETRY", family: "json" },
  { value: "INTERVAL", label: "INTERVAL — stored as VARCHAR", family: "temporal" },
];

/** Databricks / Spark SQL / Delta — aligned to ddl_type (JSON/ARRAY → STRING sink). */
export const DATABRICKS_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "STRING", label: "STRING (incl. JSON/ARRAY serialized)", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INT", label: "INT", family: "int" },
  { value: "DECIMAL(38,10)", label: "DECIMAL(38,10)", family: "decimal" },
  { value: "DOUBLE", label: "DOUBLE", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "BINARY", label: "BINARY", family: "binary" },
];

/** Apache Iceberg table types — objects serialize as string; arrays as list. */
export const ICEBERG_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string (incl. object/map serialized)", family: "string" },
  { value: "long", label: "long — 64-bit", family: "int" },
  { value: "int", label: "int — 32-bit", family: "int" },
  { value: "decimal(38,10)", label: "decimal(38,10)", family: "decimal" },
  { value: "double", label: "double", family: "decimal" },
  { value: "boolean", label: "boolean", family: "bool" },
  { value: "date", label: "date", family: "temporal" },
  { value: "time", label: "time", family: "temporal" },
  { value: "timestamptz", label: "timestamptz", family: "temporal" },
  { value: "timestamp", label: "timestamp — no TZ", family: "temporal" },
  { value: "uuid", label: "uuid", family: "uuid" },
  { value: "list", label: "list — array", family: "json" },
  { value: "binary", label: "binary", family: "binary" },
];

/** MongoDB BSON field types for create-new / match-existing Map pickers. */
export const MONGODB_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string", family: "string" },
  { value: "long", label: "long — Int64", family: "int" },
  { value: "int", label: "int — Int32", family: "int" },
  { value: "decimal", label: "decimal — Decimal128", family: "decimal" },
  { value: "double", label: "double", family: "decimal" },
  { value: "bool", label: "bool", family: "bool" },
  { value: "date", label: "date", family: "temporal" },
  { value: "object", label: "object — document", family: "json" },
  { value: "array", label: "array", family: "json" },
  { value: "binData", label: "binData", family: "binary" },
  { value: "objectId", label: "objectId", family: "uuid" },
];

/** DynamoDB AttributeValue wire types — CREATE uses these labels. */
export const DYNAMODB_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "S", label: "S — string", family: "string" },
  { value: "N", label: "N — number", family: "decimal" },
  { value: "BOOL", label: "BOOL", family: "bool" },
  { value: "M", label: "M — map", family: "json" },
  { value: "L", label: "L — list", family: "json" },
  { value: "B", label: "B — binary", family: "binary" },
];

/** Elasticsearch mapping types. */
export const ELASTICSEARCH_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "text", label: "text", family: "string" },
  { value: "keyword", label: "keyword", family: "string" },
  { value: "long", label: "long", family: "int" },
  { value: "double", label: "double", family: "decimal" },
  { value: "boolean", label: "boolean", family: "bool" },
  { value: "date", label: "date", family: "temporal" },
  { value: "object", label: "object", family: "json" },
  { value: "binary", label: "binary", family: "binary" },
];

export const ORACLE_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "VARCHAR2(4000)", label: "VARCHAR2(4000)", family: "string" },
  { value: "CLOB", label: "CLOB — long text", family: "string" },
  { value: "NUMBER(38,0)", label: "NUMBER(38,0) — integer", family: "int" },
  { value: "NUMBER(38,10)", label: "NUMBER(38,10) — decimal", family: "decimal" },
  { value: "BINARY_DOUBLE", label: "BINARY_DOUBLE — IEEE float", family: "decimal" },
  { value: "BINARY_FLOAT", label: "BINARY_FLOAT", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN (23c+)", family: "bool" },
  { value: "DATE", label: "DATE — datetime (Oracle)", family: "temporal" },
  { value: "TIMESTAMP WITH TIME ZONE", label: "TIMESTAMP WITH TIME ZONE", family: "temporal" },
  { value: "INTERVAL DAY TO SECOND", label: "INTERVAL DAY TO SECOND", family: "temporal" },
  { value: "JSON", label: "JSON", family: "json" },
  { value: "BLOB", label: "BLOB — bytes", family: "binary" },
  { value: "RAW(2000)", label: "RAW — bytes", family: "binary" },
  { value: "SDO_GEOMETRY", label: "SDO_GEOMETRY", family: "json" },
];

export const SQLSERVER_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "NVARCHAR(MAX)", label: "NVARCHAR(MAX)", family: "string" },
  { value: "VARCHAR(8000)", label: "VARCHAR(8000)", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INT", label: "INT", family: "int" },
  { value: "DECIMAL(38,10)", label: "DECIMAL(38,10)", family: "decimal" },
  { value: "MONEY", label: "MONEY", family: "decimal" },
  { value: "FLOAT", label: "FLOAT — IEEE", family: "decimal" },
  { value: "REAL", label: "REAL", family: "decimal" },
  { value: "BIT", label: "BIT — boolean", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIME", label: "TIME", family: "temporal" },
  { value: "DATETIME2", label: "DATETIME2", family: "temporal" },
  { value: "DATETIMEOFFSET", label: "DATETIMEOFFSET", family: "temporal" },
  { value: "UNIQUEIDENTIFIER", label: "UNIQUEIDENTIFIER — UUID", family: "uuid" },
  { value: "VARBINARY(MAX)", label: "VARBINARY(MAX)", family: "binary" },
  { value: "GEOGRAPHY", label: "GEOGRAPHY", family: "json" },
  { value: "XML", label: "XML — stored as text", family: "string" },
];

export const REDIS_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string — Redis value", family: "string" },
  { value: "hash", label: "hash — field map (JSON)", family: "json" },
  { value: "list", label: "list — array", family: "json" },
  { value: "set", label: "set — unique strings", family: "json" },
  { value: "zset", label: "zset — scored members", family: "json" },
  { value: "stream", label: "stream — entries", family: "json" },
  { value: "json", label: "ReJSON document", family: "json" },
];

export const SALESFORCE_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string / textarea", family: "string" },
  { value: "id", label: "id — Salesforce Id", family: "string" },
  { value: "reference", label: "reference — lookup", family: "string" },
  { value: "boolean", label: "boolean", family: "bool" },
  { value: "int", label: "int", family: "int" },
  { value: "long", label: "long", family: "int" },
  { value: "double", label: "double — IEEE", family: "decimal" },
  { value: "currency", label: "currency", family: "decimal" },
  { value: "percent", label: "percent", family: "decimal" },
  { value: "date", label: "date", family: "temporal" },
  { value: "datetime", label: "datetime", family: "temporal" },
  { value: "time", label: "time", family: "temporal" },
  { value: "base64", label: "base64 — binary", family: "binary" },
  { value: "address", label: "address — structured", family: "json" },
  { value: "picklist", label: "picklist", family: "string" },
  { value: "email", label: "email", family: "string" },
  { value: "phone", label: "phone", family: "string" },
  { value: "url", label: "url", family: "string" },
];

export const HUBSPOT_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string", family: "string" },
  { value: "enumeration", label: "enumeration", family: "string" },
  { value: "number", label: "number", family: "decimal" },
  { value: "bool", label: "bool", family: "bool" },
  { value: "date", label: "date", family: "temporal" },
  { value: "datetime", label: "datetime", family: "temporal" },
  { value: "phone_number", label: "phone_number", family: "string" },
  { value: "json", label: "json", family: "json" },
];

export const KAFKA_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "TEXT", label: "TEXT / string", family: "string" },
  { value: "INTEGER", label: "INTEGER / long", family: "int" },
  { value: "FLOAT", label: "FLOAT / double", family: "decimal" },
  { value: "DECIMAL", label: "DECIMAL", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "JSON", label: "JSON object", family: "json" },
  { value: "ARRAY", label: "ARRAY", family: "json" },
];

export function typeOptionsForDest(destType?: string): { value: string; label: string; family: TypeFamily }[] {
  const d = (destType || "").toLowerCase();
  if (d.includes("snowflake")) return SNOWFLAKE_TYPE_OPTIONS;
  if (d.includes("bigquery")) return BIGQUERY_TYPE_OPTIONS;
  if (d.includes("redshift")) return REDSHIFT_TYPE_OPTIONS;
  if (d.includes("databricks") || d.includes("spark") || d.includes("delta")) return DATABRICKS_TYPE_OPTIONS;
  if (d.includes("iceberg")) return ICEBERG_TYPE_OPTIONS;
  if (d.includes("mongo")) return MONGODB_TYPE_OPTIONS;
  if (d.includes("dynamo")) return DYNAMODB_TYPE_OPTIONS;
  if (d.includes("elastic") || d.includes("opensearch")) return ELASTICSEARCH_TYPE_OPTIONS;
  if (d.includes("salesforce") || d === "sf") return SALESFORCE_TYPE_OPTIONS;
  if (d.includes("hubspot")) return HUBSPOT_TYPE_OPTIONS;
  if (d.includes("kafka") || d.includes("confluent")) return KAFKA_TYPE_OPTIONS;
  if (d.includes("oracle")) return ORACLE_TYPE_OPTIONS;
  if (d.includes("sqlserver") || d.includes("mssql") || d.includes("sql_server") || d.includes("azure_sql")) {
    return SQLSERVER_TYPE_OPTIONS;
  }
  if (d.includes("redis") || d.includes("elasticache") || d.includes("memorystore")) return REDIS_TYPE_OPTIONS;
  if (d.includes("postgres") || d === "pg") return POSTGRES_TYPE_OPTIONS;
  if (d.includes("mysql") || d.includes("mariadb")) return MYSQL_TYPE_OPTIONS;
  return LOGICAL_TYPE_OPTIONS;
}

export function typeFamily(rawType: string | undefined): TypeFamily {
  const t = (rawType || "string").toLowerCase().trim();
  // NUMBER(p,s): scale 0 → int; scale > 0 → decimal. Bare NUMBER(38) → int.
  const numberPs = t.match(/number\s*\(\s*\d+\s*,\s*(\d+)\s*\)/);
  if (numberPs) {
    return Number(numberPs[1]) === 0 ? "int" : "decimal";
  }
  if (/number\s*\(\s*\d+\s*\)/.test(t)) return "int";
  if (/\b(int|integer|bigint|smallint|tinyint|long)\b/.test(t) && !/interval/.test(t)) return "int";
  if (/decimal|numeric|bignumeric|money|^number$/.test(t)) return "decimal";
  if (/float|double|real/.test(t)) return "decimal"; // display family; logical FLOAT stays distinct in Map
  if (/bool/.test(t)) return "bool";
  if (/interval/.test(t)) return "temporal";
  if (/timestamp|datetime|date|time/.test(t)) return "temporal";
  if (/vector/.test(t)) return "binary";
  if (/geography|geometry|geojson|geopoint/.test(t)) return "json";
  if (/json|variant|object|array|list|super|map|struct|record/.test(t)) return "json";
  if (/uuid|guid|objectid/.test(t)) return "uuid";
  if (/binary|blob|bytea|bytes|varbinary|bindata/.test(t)) return "binary";
  return "string";
}

export function typeBadgeClass(rawType: string | undefined): string {
  return `df2-type-${typeFamily(rawType)}`;
}

/** Options for a select, always including the current value if custom. */
export function destTypeSelectOptions(
  current?: string,
  destType?: string,
): { value: string; label: string }[] {
  const options = typeOptionsForDest(destType);
  const base = options.map(({ value, label }) => ({ value, label }));
  const cur = (current || "").trim();
  if (!cur) return base;
  const upper = cur.toUpperCase();
  const matched = base.find((o) => o.value.toUpperCase() === upper);
  if (matched) return base;
  // Map generic INTEGER → Snowflake NUMBER(38,0) label when selecting for snowflake.
  if ((destType || "").toLowerCase().includes("snowflake")) {
    if (upper === "INTEGER" || upper === "BIGINT" || upper === "INT") {
      return base;
    }
    if (upper === "TIMESTAMP" || upper === "TIMESTAMPTZ") {
      return base;
    }
  }
  return [{ value: cur, label: `${cur} — current` }, ...base];
}

/** Normalize a type string to a select option value when possible. */
export function normalizeDestTypeValue(current?: string, destType?: string): string {
  const cur = (current || "").trim();
  if (!cur) return "VARCHAR";
  const upper = cur.toUpperCase();
  const options = typeOptionsForDest(destType);
  const matched = options.find((o) => o.value.toUpperCase() === upper);
  if (matched) return matched.value;
  if ((destType || "").toLowerCase().includes("snowflake")) {
    if (upper === "INTEGER" || upper === "INT" || upper === "BIGINT" || upper === "SMALLINT") {
      return "NUMBER(38,0)";
    }
    if (upper === "DECIMAL" || upper === "NUMERIC" || upper === "DOUBLE" || upper === "FLOAT") {
      // Preserve approximate float semantics — do not invent NUMBER(38,10) scale.
      if (upper === "FLOAT" || upper === "DOUBLE") return "FLOAT";
      return "NUMBER(38,10)";
    }
    if (upper === "TIMESTAMP" || upper === "TIMESTAMPTZ" || upper === "DATETIME") {
      return "TIMESTAMP_TZ";
    }
    if (upper === "JSON" || upper === "JSONB" || upper === "ARRAY") {
      return "VARIANT";
    }
    if (upper === "TEXT" || upper === "STRING") {
      return "VARCHAR";
    }
  }
  const dest = (destType || "").toLowerCase();
  if (dest.includes("databricks") || dest.includes("spark") || dest.includes("delta")) {
    if (upper === "INTEGER" || upper === "INT" || upper === "SMALLINT") return "INT";
    if (upper === "BIGINT" || upper === "LONG") return "BIGINT";
    if (upper === "DECIMAL" || upper === "NUMERIC") return "DECIMAL(38,10)";
    if (upper === "FLOAT" || upper === "FLOAT64") return "DOUBLE";
    if (upper === "VARCHAR" || upper === "TEXT" || upper === "STRING") return "STRING";
    // Honest sink — ddl_type maps JSON/ARRAY/MAP/STRUCT → STRING (not native MAP).
    if (upper === "JSON" || upper === "JSONB" || upper === "ARRAY" || upper === "VARIANT" || upper === "MAP" || upper === "STRUCT") {
      return "STRING";
    }
    if (upper === "TIMESTAMPTZ" || upper === "DATETIME") return "TIMESTAMP";
    if (upper === "BYTEA" || upper === "BLOB" || upper === "BYTES") return "BINARY";
  }
  if (dest.includes("iceberg")) {
    if (upper === "INTEGER" || upper === "INT" || upper === "SMALLINT") return "int";
    if (upper === "BIGINT" || upper === "LONG") return "long";
    if (upper === "DECIMAL" || upper === "NUMERIC") return "decimal(38,10)";
    if (upper === "FLOAT" || upper === "FLOAT64") return "double";
    if (upper === "VARCHAR" || upper === "TEXT" || upper === "STRING") return "string";
    if (upper === "JSON" || upper === "JSONB" || upper === "MAP" || upper === "STRUCT" || upper === "OBJECT") return "string";
    if (upper === "ARRAY" || upper === "LIST") return "list";
    if (upper === "TIMESTAMPTZ" || upper === "TIMESTAMP_TZ") return "timestamptz";
    if (upper === "DATETIME" || upper === "TIMESTAMP") return "timestamp";
    if (upper === "BYTEA" || upper === "BLOB" || upper === "BYTES" || upper === "BINARY") return "binary";
    if (upper === "BOOLEAN" || upper === "BOOL") return "boolean";
    if (upper === "UUID") return "uuid";
  }
  if (dest.includes("mongo")) {
    if (upper === "INTEGER" || upper === "INT" || upper === "SMALLINT") return "int";
    if (upper === "BIGINT" || upper === "LONG" || upper === "NUMBER(38,0)") return "long";
    if (upper === "DECIMAL" || upper === "NUMERIC" || upper.startsWith("NUMBER")) return "decimal";
    if (upper === "FLOAT" || upper === "DOUBLE") return "double";
    if (upper === "VARCHAR" || upper === "TEXT" || upper === "STRING") return "string";
    if (upper === "BOOLEAN" || upper === "BOOL") return "bool";
    if (upper === "JSON" || upper === "JSONB" || upper === "OBJECT" || upper === "VARIANT") return "object";
    if (upper === "ARRAY") return "array";
    if (upper === "BINARY" || upper === "BYTEA" || upper === "BLOB" || upper === "BYTES") return "binData";
    if (upper === "TIMESTAMP" || upper === "TIMESTAMPTZ" || upper === "DATETIME" || upper === "DATE") return "date";
  }
  return cur;
}
