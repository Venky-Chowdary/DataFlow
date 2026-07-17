import type { ParsedUpload } from "./types";

/** Lightweight CSV parse for Transfer Studio when the API is offline. */
export function parseCsvTextForPreview(text: string): ParsedUpload {
  const lines = text
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim()
    .split("\n")
    .filter((line) => line.length > 0);

  if (lines.length < 1) {
    throw new Error("Empty file");
  }

  const delimiter = lines[0].includes("\t") ? "\t" : ",";
  const headers = lines[0].split(delimiter).map((h) => h.trim());

  const rows = lines.slice(1).map((line) => {
    const vals = line.split(delimiter).map((v) => v.trim());
    const row: Record<string, unknown> = {};
    headers.forEach((h, i) => {
      row[h] = vals[i] ?? "";
    });
    return row;
  });

  const schema: Record<string, string> = {};
  headers.forEach((h) => {
    schema[h] = "string";
  });

  return {
    row_count: rows.length,
    columns: headers,
    file_type: delimiter === "\t" ? "tsv" : "csv",
    data: rows,
    sample_data: rows.slice(0, 50),
    schema,
    validation: null,
  };
}
