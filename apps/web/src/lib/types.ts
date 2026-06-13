/** Universal data platform types — any source, any destination, any operation. */

export type WizardStep = "connect" | "source" | "destination" | "transfer";

export type NavItemId = "transfer" | "connectors" | "jobs";

export type EndpointKind = "file" | "database" | "api";

export type DataOperation =
  | "upload" // file → database/warehouse
  | "dump" // database → file
  | "transfer" // db → db, file → file, api → db
  | "migration" // full db → db with schema
  | "convert"; // file → file (CSV → Word, etc.)

export type DatabaseType =
  | "postgresql"
  | "sqlserver"
  | "mysql"
  | "oracle"
  | "mongodb"
  | "snowflake"
  | "bigquery"
  | "redis"
  | "databricks";

export type FileFormat =
  | "auto"
  | "csv"
  | "excel"
  | "json"
  | "parquet"
  | "avro"
  | "fixed_width"
  | "pdf"
  | "word"
  | "xml"
  | "sql";

export interface DatabaseConnection {
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
  tables: string[];
  targetColumns: { name: string; inferred_type: string; nullable: boolean }[];
  sourceTable: string;
}

export interface FileEndpoint {
  fileName: string | null;
  fileId: string | null;
  format: FileFormat;
  detectedFormat: string | null;
  encoding: string;
  rowCount: number | null;
  columns: { name: string; inferred_type: string; nullable: boolean; samples: string[] }[];
  previewRows: string[][];
}

export interface ApiEndpoint {
  url: string;
  authType: "none" | "bearer" | "basic";
}

export interface EndpointConfig {
  kind: EndpointKind;
  label: string;
  file: FileEndpoint;
  database: DatabaseConnection | null;
  api: ApiEndpoint | null;
  /** When destination is file export */
  exportFormat: FileFormat | null;
  connected: boolean;
  connectionError: string | null;
}

export interface TransferJobSummary {
  operation: DataOperation;
  operationLabel: string;
  sourceSummary: string;
  destinationSummary: string;
}

export const DATABASE_OPTIONS: { id: DatabaseType; label: string; defaultPort: number }[] = [
  { id: "postgresql", label: "PostgreSQL", defaultPort: 5432 },
  { id: "sqlserver", label: "SQL Server", defaultPort: 1433 },
  { id: "mysql", label: "MySQL", defaultPort: 3306 },
  { id: "oracle", label: "Oracle", defaultPort: 1521 },
  { id: "mongodb", label: "MongoDB", defaultPort: 27017 },
  { id: "snowflake", label: "Snowflake", defaultPort: 443 },
  { id: "bigquery", label: "BigQuery", defaultPort: 443 },
  { id: "redis", label: "Redis", defaultPort: 6379 },
  { id: "databricks", label: "Databricks", defaultPort: 443 },
];

export const FILE_FORMAT_OPTIONS: { id: FileFormat; label: string }[] = [
  { id: "auto", label: "Auto-detect" },
  { id: "csv", label: "CSV" },
  { id: "excel", label: "Excel (.xlsx)" },
  { id: "json", label: "JSON" },
  { id: "parquet", label: "Parquet" },
  { id: "fixed_width", label: "Fixed-width" },
  { id: "pdf", label: "PDF (extract)" },
  { id: "word", label: "Word (.docx)" },
  { id: "xml", label: "XML" },
  { id: "sql", label: "SQL dump" },
];

export function emptyDatabase(type: DatabaseType = "postgresql"): DatabaseConnection {
  const opt = DATABASE_OPTIONS.find((d) => d.id === type)!;
  return {
    type,
    host: "",
    port: opt.defaultPort,
    database: "",
    username: "",
    password: "",
    schema: type === "snowflake" ? "PUBLIC" : "public",
    connectionString: "",
    ssl: true,
    warehouse: "",
    tables: [],
    targetColumns: [],
    sourceTable: "",
  };
}

export function emptyFileEndpoint(): FileEndpoint {
  return {
    fileName: null,
    fileId: null,
    format: "auto",
    detectedFormat: null,
    encoding: "utf-8",
    rowCount: null,
    columns: [],
    previewRows: [],
  };
}

export function emptyEndpoint(kind: EndpointKind, label: string): EndpointConfig {
  return {
    kind,
    label,
    file: emptyFileEndpoint(),
    database: kind === "database" ? emptyDatabase() : null,
    api: kind === "api" ? { url: "", authType: "none" } : null,
    exportFormat: null,
    connected: false,
    connectionError: null,
  };
}

export function inferOperation(source: EndpointConfig, dest: EndpointConfig): DataOperation {
  if (source.kind === "file" && dest.kind === "file") return "convert";
  if (source.kind === "file" && dest.kind === "database") return "upload";
  if (source.kind === "database" && dest.kind === "file") return "dump";
  if (source.kind === "database" && dest.kind === "database") return "migration";
  return "transfer";
}

export function operationLabel(op: DataOperation): string {
  const labels: Record<DataOperation, string> = {
    upload: "File upload → database",
    dump: "Database dump → file",
    transfer: "Data transfer",
    migration: "Database migration",
    convert: "Format conversion",
  };
  return labels[op];
}

export function endpointSummary(ep: EndpointConfig): string {
  if (ep.kind === "file") {
    return ep.file.fileName ?? "No file";
  }
  if (ep.kind === "database" && ep.database) {
    const d = ep.database;
    if (d.connectionString) {
      const masked = d.connectionString.replace(/:([^:@/]+)@/, ":****@");
      return masked.length > 56 ? `${masked.slice(0, 53)}…` : masked;
    }
    if (d.sourceTable) return `${d.type} · ${d.sourceTable}`;
    return `${d.type}://${d.host}:${d.port}/${d.database || "…"}`;
  }
  if (ep.kind === "api" && ep.api) {
    return ep.api.url || "API endpoint";
  }
  return ep.label;
}
