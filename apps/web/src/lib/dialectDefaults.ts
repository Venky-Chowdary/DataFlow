/**
 * Dialect defaults for schema/namespace — mirrors apps/api/services/dialect_profiles.py.
 * Keep in sync: never send Postgres `public` to Snowflake / SQL Server / BigQuery.
 */

const DEFAULTS: Record<string, string | null> = {
  postgresql: "public",
  postgres: "public",
  redshift: "public",
  pgvector: "public",
  snowflake: "PUBLIC",
  mysql: null,
  mariadb: null,
  sqlserver: "dbo",
  mssql: "dbo",
  "mssql+pyodbc": "dbo",
  oracle: null,
  bigquery: "dataflow",
  sqlite: null,
  duckdb: "main",
  databricks: "default",
  presto: "public",
  trino: "default",
};

const ALIASES: Record<string, string> = {
  "postgresql+psycopg2": "postgresql",
  "mysql+pymysql": "mysql",
  "oracle+oracledb": "oracle",
  bq: "bigquery",
};

export function normalizeDialectDriver(driver?: string | null): string {
  const raw = String(driver || "").trim().toLowerCase();
  return ALIASES[raw] || raw;
}

/** Default schema/dataset for empty UI fields — null means omit. */
export function defaultSchemaForDriver(driver?: string | null): string {
  const key = normalizeDialectDriver(driver);
  const v = DEFAULTS[key];
  if (v == null) return "";
  return v;
}

/** Fold identifier for Snowflake/Oracle-style uppercase dialects. */
export function foldSchemaForDriver(driver: string | null | undefined, schema: string): string {
  const key = normalizeDialectDriver(driver);
  const raw = String(schema || "").trim();
  if (!raw) return defaultSchemaForDriver(key);
  if (key === "snowflake" || key === "oracle") {
    if (raw !== raw.toUpperCase() && raw !== raw.toLowerCase()) return raw;
    return raw.toUpperCase();
  }
  if (key === "postgresql" || key === "postgres" || key === "redshift" || key === "pgvector") {
    if (raw !== raw.toUpperCase() && raw !== raw.toLowerCase()) return raw;
    return raw.toLowerCase();
  }
  return raw;
}
