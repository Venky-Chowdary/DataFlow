/**
 * Destination step route text in the product's words (UX-39).
 *
 * The callout read "Route ready · convert" and "Export file as `json` in server
 * exports folder", and the toast "Live route: csv → json": operation codes,
 * lowercase format ids and raw backticks. These turn the API's route plan into
 * labels; the API's own strings stay as they are for its other readers.
 */
import { connectorDisplayName } from "./connectorTypes";

const FORMAT_LABELS: Record<string, string> = {
  csv: "CSV",
  tsv: "TSV",
  json: "JSON",
  jsonl: "JSON Lines",
  ndjson: "JSON Lines",
  parquet: "Parquet",
  avro: "Avro",
  orc: "ORC",
  xml: "XML",
  xlsx: "Excel",
  xls: "Excel",
  excel: "Excel",
  pdf: "PDF",
  docx: "Word",
  html: "HTML",
  txt: "Text",
};

const OPERATION_LABELS: Record<string, string> = {
  upload: "load the file into the database",
  migration: "copy between databases",
  convert: "export to a file",
  dump: "export to a file",
  transfer: "transfer",
};

/** A file format or engine id as people name it ("json" → "JSON", "mysql" → "MySQL"). */
export function routeFormatLabel(id: string): string {
  const key = (id || "").trim().toLowerCase();
  if (!key) return "";
  return FORMAT_LABELS[key] ?? connectorDisplayName(key);
}

export function routeOperationLabel(op: string): string {
  const key = (op || "").trim().toLowerCase();
  return OPERATION_LABELS[key] ?? key.replace(/_/g, " ");
}

/** One plan line: `x` spans lose their backticks, and format ids get their labels. */
export function routePlanItemText(item: string): string {
  return String(item || "").replace(/`([^`]+)`/g, (_m, inner: string) => {
    const label = FORMAT_LABELS[inner.trim().toLowerCase()];
    return label ?? inner;
  });
}

/** "Live route: csv → json export" → "Live route: CSV → JSON export". */
export function routeMessageText(message: string): string {
  return String(message || "").replace(
    /^(Live route:\s*)([\w.-]+)\s*→\s*([\w.-]+)/,
    (_m, head: string, a: string, b: string) => `${head}${routeFormatLabel(a)} → ${routeFormatLabel(b)}`,
  );
}
