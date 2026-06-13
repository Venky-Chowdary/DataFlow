import type { DataOperation, EndpointKind, FileFormat } from "./types";

export interface TransferTemplate {
  id: string;
  label: string;
  description: string;
  sourceKind: EndpointKind;
  destKind: EndpointKind | "file";
  destFormat?: FileFormat;
  operation: DataOperation;
}

/** One-click templates — matches PRODUCT_SCOPE operation matrix */
export const TRANSFER_TEMPLATES: TransferTemplate[] = [
  {
    id: "file-db",
    label: "File → Database",
    description: "CSV, Excel, JSON, PDF, Word → any database",
    sourceKind: "file",
    destKind: "database",
    operation: "upload",
  },
  {
    id: "db-db",
    label: "Database → Database",
    description: "PostgreSQL, SQL Server, MongoDB, Snowflake…",
    sourceKind: "database",
    destKind: "database",
    operation: "migration",
  },
  {
    id: "file-file",
    label: "File → File",
    description: "CSV ↔ Excel, PDF ↔ Word, any format conversion",
    sourceKind: "file",
    destKind: "file",
    destFormat: "pdf",
    operation: "convert",
  },
  {
    id: "db-file",
    label: "Database → File",
    description: "Export tables to CSV, Excel, JSON, SQL",
    sourceKind: "database",
    destKind: "file",
    destFormat: "csv",
    operation: "dump",
  },
  {
    id: "api-db",
    label: "API → Database",
    description: "REST endpoint ingest to any database",
    sourceKind: "api",
    destKind: "database",
    operation: "transfer",
  },
];

export function operationDescription(op: DataOperation): string {
  const map: Record<DataOperation, string> = {
    upload: "Upload file into destination database with semantic column mapping",
    migration: "Migrate data between databases — schema detected automatically",
    convert: "Convert file format using content-aware transformation",
    dump: "Export database table to file format",
    transfer: "Transfer data from source to destination",
  };
  return map[op];
}
