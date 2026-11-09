/**
 * Saved file-connector Configure dialogs attach a file the server stores.
 *
 * Transfer Studio already uploads through ``#df2-source-file``. CSV, TSV,
 * Excel, JSON, and SQLite connections used to offer only a path box, so a
 * hosted customer who typed a laptop path failed Test with "Path not found".
 * The upload fills that path with the stored file and keeps ``uploaded_file_id``
 * so Test can open it again.
 */

import { resolveCatalogIdToType } from "./connectorTypes";

/** Connectors whose Configure dialog must offer a file input. */
const UPLOAD_CONNECTOR_TYPES = [
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
  "pdf",
  "docx",
  "html",
  "yaml",
  "fixed_width",
  "sqlite",
  "duckdb",
] as const;

const ACCEPT_BY_TYPE: Record<string, string> = {
  csv: ".csv",
  tsv: ".tsv,.txt",
  json: ".json",
  jsonl: ".jsonl,.ndjson",
  ndjson: ".jsonl,.ndjson",
  excel: ".xlsx,.xls",
  parquet: ".parquet",
  avro: ".avro",
  orc: ".orc",
  xml: ".xml",
  pdf: ".pdf",
  docx: ".docx",
  html: ".html,.htm",
  yaml: ".yaml,.yml",
  fixed_width: ".fwf,.txt",
  sqlite: ".db,.sqlite,.sqlite3",
  duckdb: ".duckdb,.db",
};

/** Profiled ``file_type`` values that belong on this connector. */
const FILE_TYPE_ALIASES: Record<string, string[]> = {
  csv: ["csv"],
  tsv: ["tsv"],
  json: ["json"],
  jsonl: ["jsonl", "ndjson"],
  ndjson: ["jsonl", "ndjson"],
  excel: ["excel", "xlsx", "xls"],
  parquet: ["parquet"],
  avro: ["avro"],
  orc: ["orc"],
  xml: ["xml"],
  pdf: ["pdf"],
  docx: ["docx"],
  html: ["html", "htm"],
  yaml: ["yaml", "yml"],
  fixed_width: ["fixed_width", "fwf"],
  sqlite: ["sqlite"],
  duckdb: ["duckdb"],
};

export function connectorAcceptsUpload(type: string): boolean {
  const resolved = resolveCatalogIdToType(type);
  return (UPLOAD_CONNECTOR_TYPES as readonly string[]).includes(resolved);
}

export function connectorUploadAccept(type: string): string {
  const resolved = resolveCatalogIdToType(type);
  return ACCEPT_BY_TYPE[resolved] || "";
}

export function uploadedFileMatchesConnector(
  connectorType: string,
  fileType: string,
  filename = "",
): boolean {
  const resolved = resolveCatalogIdToType(connectorType);
  const allowed = FILE_TYPE_ALIASES[resolved] || [resolved];
  const profiled = String(fileType || "").trim().toLowerCase();
  if (profiled && allowed.includes(profiled)) return true;
  const ext = filename.toLowerCase().split(".").pop() || "";
  return Boolean(ext) && allowed.includes(ext);
}

export interface UploadedConnectorFields {
  connectionString?: string;
  database?: string;
  uploadedFileId: string;
}

/**
 * Which saved-connector fields the stored upload path fills.
 *
 * File formats keep the path on ``connection_string`` (the file_path auth
 * mode). SQLite and DuckDB keep it on ``database`` unless the operator is on
 * the connection-string tab.
 */
export function connectorFieldsFromUpload(input: {
  type: string;
  authMode: string;
  path: string;
  fileId: string;
}): UploadedConnectorFields {
  const resolved = resolveCatalogIdToType(input.type);
  const embedded = resolved === "sqlite" || resolved === "duckdb";
  const uploadedFileId = String(input.fileId || "").trim();
  const path = String(input.path || "").trim();
  if (embedded && input.authMode === "connection_string") {
    return { connectionString: path, uploadedFileId };
  }
  if (embedded) {
    return { database: path, uploadedFileId };
  }
  return { connectionString: path, uploadedFileId };
}
