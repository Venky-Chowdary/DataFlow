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
  { value: "UUID", label: "UUID", family: "uuid" },
  { value: "JSONB", label: "JSONB", family: "json" },
  { value: "BYTEA", label: "BYTEA", family: "binary" },
];

export const MYSQL_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "TEXT", label: "TEXT", family: "string" },
  { value: "LONGTEXT", label: "LONGTEXT", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INT", label: "INT", family: "int" },
  { value: "DECIMAL(38,15)", label: "DECIMAL(38,15)", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "DATETIME(6)", label: "DATETIME(6)", family: "temporal" },
  { value: "JSON", label: "JSON", family: "json" },
  { value: "LONGBLOB", label: "LONGBLOB", family: "binary" },
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
];

/** Databricks / Spark SQL / Delta lakehouse native types. */
export const DATABRICKS_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "STRING", label: "STRING", family: "string" },
  { value: "BIGINT", label: "BIGINT", family: "int" },
  { value: "INT", label: "INT", family: "int" },
  { value: "DECIMAL(38,10)", label: "DECIMAL(38,10)", family: "decimal" },
  { value: "DOUBLE", label: "DOUBLE", family: "decimal" },
  { value: "BOOLEAN", label: "BOOLEAN", family: "bool" },
  { value: "DATE", label: "DATE", family: "temporal" },
  { value: "TIMESTAMP", label: "TIMESTAMP", family: "temporal" },
  { value: "BINARY", label: "BINARY", family: "binary" },
  { value: "MAP", label: "MAP / STRUCT — semi-structured", family: "json" },
];

/** Apache Iceberg table types (catalog / writer native). */
export const ICEBERG_TYPE_OPTIONS: { value: string; label: string; family: TypeFamily }[] = [
  { value: "string", label: "string", family: "string" },
  { value: "long", label: "long — 64-bit", family: "int" },
  { value: "int", label: "int — 32-bit", family: "int" },
  { value: "decimal(38,10)", label: "decimal(38,10)", family: "decimal" },
  { value: "double", label: "double", family: "decimal" },
  { value: "boolean", label: "boolean", family: "bool" },
  { value: "date", label: "date", family: "temporal" },
  { value: "timestamptz", label: "timestamptz", family: "temporal" },
  { value: "timestamp", label: "timestamp — no TZ", family: "temporal" },
  { value: "uuid", label: "uuid", family: "uuid" },
  { value: "list", label: "list", family: "json" },
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

export function typeOptionsForDest(destType?: string): { value: string; label: string; family: TypeFamily }[] {
  const d = (destType || "").toLowerCase();
  if (d.includes("snowflake")) return SNOWFLAKE_TYPE_OPTIONS;
  if (d.includes("bigquery")) return BIGQUERY_TYPE_OPTIONS;
  if (d.includes("redshift")) return REDSHIFT_TYPE_OPTIONS;
  if (d.includes("databricks") || d.includes("spark") || d.includes("delta")) return DATABRICKS_TYPE_OPTIONS;
  if (d.includes("iceberg")) return ICEBERG_TYPE_OPTIONS;
  if (d.includes("mongo")) return MONGODB_TYPE_OPTIONS;
  if (d.includes("postgres") || d === "pg") return POSTGRES_TYPE_OPTIONS;
  if (d.includes("mysql") || d.includes("mariadb")) return MYSQL_TYPE_OPTIONS;
  return LOGICAL_TYPE_OPTIONS;
}

export function typeFamily(rawType: string | undefined): TypeFamily {
  const t = (rawType || "string").toLowerCase();
  if (/int|bigint|smallint|number\(/.test(t)) return "int";
  if (/decimal|numeric|float|double|real|bignumeric|number$/.test(t)) return "decimal";
  if (/bool/.test(t)) return "bool";
  if (/timestamp|datetime|date|time/.test(t)) return "temporal";
  if (/json|variant|object|array|super|map|struct/.test(t)) return "json";
  if (/uuid|guid/.test(t)) return "uuid";
  if (/binary|blob|bytea|bytes|varbinary/.test(t)) return "binary";
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
      return upper === "FLOAT" ? "FLOAT" : "NUMBER(38,10)";
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
    if (upper === "VARCHAR" || upper === "TEXT" || upper === "STRING") return "STRING";
    if (upper === "JSON" || upper === "JSONB" || upper === "ARRAY" || upper === "VARIANT") return "MAP";
    if (upper === "TIMESTAMPTZ" || upper === "DATETIME") return "TIMESTAMP";
    if (upper === "BYTEA" || upper === "BLOB" || upper === "BYTES") return "BINARY";
  }
  if (dest.includes("iceberg")) {
    if (upper === "INTEGER" || upper === "INT" || upper === "SMALLINT") return "int";
    if (upper === "BIGINT" || upper === "LONG") return "long";
    if (upper === "DECIMAL" || upper === "NUMERIC") return "decimal(38,10)";
    if (upper === "VARCHAR" || upper === "TEXT" || upper === "STRING") return "string";
    if (upper === "JSON" || upper === "JSONB" || upper === "ARRAY") return "list";
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
