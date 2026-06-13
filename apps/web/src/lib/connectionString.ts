import { DATABASE_OPTIONS, emptyDatabase, type DatabaseConnection, type DatabaseType } from "./types";

export interface ParsedConnection {
  type: DatabaseType;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema: string;
  connectionString: string;
  ssl: boolean;
  warehouse: string;
}

function defaultPort(type: DatabaseType): number {
  return DATABASE_OPTIONS.find((d) => d.id === type)?.defaultPort ?? 5432;
}

/** Infer database engine from a pasted connection string. */
export function detectDatabaseType(raw: string): DatabaseType {
  const s = raw.trim().toLowerCase();
  if (s.startsWith("postgres://") || s.startsWith("postgresql://")) return "postgresql";
  if (s.startsWith("mysql://") || s.startsWith("mariadb://")) return "mysql";
  if (s.startsWith("mongodb://") || s.startsWith("mongodb+srv://")) return "mongodb";
  if (s.startsWith("redis://") || s.startsWith("rediss://")) return "redis";
  if (s.startsWith("snowflake://")) return "snowflake";
  if (s.includes("sql server") || s.includes("server=tcp:") || s.includes("data source=")) return "sqlserver";
  if (s.includes("oracle") || s.startsWith("jdbc:oracle")) return "oracle";
  if (s.includes("bigquery") || s.includes("project=")) return "bigquery";
  if (s.includes("databricks") || s.includes("token=")) return "databricks";
  return "postgresql";
}

function parseUrlStyle(raw: string, type: DatabaseType): Partial<ParsedConnection> {
  try {
    const url = new URL(raw.trim());
    const database = url.pathname.replace(/^\//, "").split("?")[0] || "";
    const schema =
      url.searchParams.get("schema") ??
      url.searchParams.get("currentSchema") ??
      (type === "snowflake" ? "PUBLIC" : "public");
    return {
      host: url.hostname,
      port: url.port ? Number(url.port) : defaultPort(type),
      database,
      username: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      schema,
      ssl: url.protocol !== "postgres:" && url.searchParams.get("sslmode") !== "disable",
      warehouse: url.searchParams.get("warehouse") ?? "",
    };
  } catch {
    return {};
  }
}

function parseKeyValue(raw: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const part of raw.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    const key = part.slice(0, eq).trim().toLowerCase();
    const val = part.slice(eq + 1).trim();
    out[key] = val;
  }
  return out;
}

function parseAdoStyle(raw: string, type: DatabaseType): Partial<ParsedConnection> {
  const kv = parseKeyValue(raw);
  const host =
    kv["server"]?.replace(/^tcp:/i, "").split(",")[0] ??
    kv["data source"]?.split(",")[0] ??
    kv["host"] ??
    "";
  const portPart = kv["server"]?.split(",")[1] ?? kv["port"];
  return {
    host,
    port: portPart ? Number(portPart) : defaultPort(type),
    database: kv["database"] ?? kv["initial catalog"] ?? "",
    username: kv["user id"] ?? kv["uid"] ?? kv["username"] ?? "",
    password: kv["password"] ?? kv["pwd"] ?? "",
    schema: kv["schema"] ?? "public",
    ssl: kv["encrypt"] !== "false",
    warehouse: kv["warehouse"] ?? "",
  };
}

export function buildConnectionString(fields: ParsedConnection): string {
  const { type, host, port, database, username, password, schema, warehouse, ssl } = fields;
  const user = encodeURIComponent(username);
  const pass = encodeURIComponent(password);

  if (type === "postgresql") {
    const base = `postgresql://${user}:${pass}@${host}:${port}/${database}`;
    return schema ? `${base}?schema=${encodeURIComponent(schema)}` : base;
  }
  if (type === "mysql") {
    return `mysql://${user}:${pass}@${host}:${port}/${database}`;
  }
  if (type === "mongodb") {
    return `mongodb://${user}:${pass}@${host}:${port}/${database}`;
  }
  if (type === "snowflake") {
    const wh = warehouse ? `?warehouse=${encodeURIComponent(warehouse)}` : "";
    return `snowflake://${user}:${pass}@${host}/${database}${wh}`;
  }
  if (type === "sqlserver") {
    return `Server=tcp:${host},${port};Database=${database};User Id=${username};Password=${password};Encrypt=${ssl ? "True" : "False"}`;
  }
  if (type === "redis") {
    return `redis://${password ? `:${pass}@` : ""}${host}:${port}/${database || "0"}`;
  }
  return `${type}://${user}:${pass}@${host}:${port}/${database}`;
}

/** Parse a pasted connection string into structured fields. */
export function parseConnectionString(raw: string): ParsedConnection {
  const trimmed = raw.trim();
  const type = detectDatabaseType(trimmed);
  const fromUrl = trimmed.includes("://") ? parseUrlStyle(trimmed, type) : {};
  const fromKv =
    trimmed.includes("=") && !trimmed.includes("://") ? parseAdoStyle(trimmed, type) : {};

  const merged = { ...fromKv, ...fromUrl };
  return {
    type,
    host: merged.host ?? "",
    port: merged.port ?? defaultPort(type),
    database: merged.database ?? "",
    username: merged.username ?? "",
    password: merged.password ?? "",
    schema: merged.schema ?? (type === "snowflake" ? "PUBLIC" : "public"),
    connectionString: trimmed,
    ssl: merged.ssl ?? true,
    warehouse: merged.warehouse ?? "",
  };
}

export function toDatabaseConnection(parsed: ParsedConnection): DatabaseConnection {
  return {
    ...emptyDatabase(parsed.type),
    ...parsed,
    tables: [],
    targetColumns: [],
    sourceTable: "",
  };
}

export function connectionSummary(db: DatabaseConnection): string {
  if (db.connectionString) {
    const masked = db.connectionString.replace(/:([^:@/]+)@/, ":****@");
    return masked.length > 72 ? `${masked.slice(0, 69)}…` : masked;
  }
  return `${db.type}://${db.host}:${db.port}/${db.database}`;
}
