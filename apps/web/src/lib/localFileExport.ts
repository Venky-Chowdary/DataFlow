import type { EditableMapping } from "./mapping";
import { applyLocalTransform } from "./localTransform";
import type { NumberLocale } from "./numberLocale";
import type { TransferResult } from "./types";

function mapRow(
  row: Record<string, unknown>,
  mappings: EditableMapping[],
  numberLocale: NumberLocale | string = "",
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const m of mappings) {
    const raw = row[m.source];
    const transform = m.transform === "none" ? undefined : m.transform;
    out[m.target] = applyLocalTransform(raw, transform, numberLocale);
  }
  return out;
}

function serializeRows(rows: Record<string, unknown>[], format: string): { blob: Blob; mime: string; ext: string } {
  const fmt = format.toLowerCase();
  if (fmt === "json") {
    return {
      blob: new Blob([JSON.stringify(rows, null, 2)], { type: "application/json" }),
      mime: "application/json",
      ext: "json",
    };
  }
  if (fmt === "jsonl") {
    const text = rows.map((r) => JSON.stringify(r)).join("\n");
    return { blob: new Blob([text], { type: "application/x-ndjson" }), mime: "application/x-ndjson", ext: "jsonl" };
  }
  const delimiter = fmt === "tsv" ? "\t" : ",";
  const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
  const escape = (v: unknown) => {
    const s = v == null ? "" : String(v);
    if (s.includes(delimiter) || s.includes('"') || s.includes("\n")) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };
  const lines = [
    headers.join(delimiter),
    ...rows.map((r) => headers.map((h) => escape(r[h])).join(delimiter)),
  ];
  const mime = fmt === "tsv" ? "text/tab-separated-values" : "text/csv";
  return { blob: new Blob([lines.join("\n")], { type: mime }), mime, ext: fmt === "tsv" ? "tsv" : "csv" };
}

export interface LocalFileExportInput {
  sourceFilename: string;
  rows: Record<string, unknown>[];
  mappings: EditableMapping[];
  format: string;
  outputBasename?: string;
  numberLocale?: NumberLocale | string;
}

/** Export mapped rows in the browser when the transfer API is offline. */
export function runLocalFileExport(input: LocalFileExportInput): TransferResult {
  const mapped = input.rows.map((row) => mapRow(row, input.mappings, input.numberLocale));
  const { blob, ext } = serializeRows(mapped, input.format);
  const base = (input.outputBasename || input.sourceFilename.replace(/\.[^/.]+$/, "") || "export").replace(/[^\w.-]+/g, "_");
  const filename = `${base}.${ext}`;
  const downloadUrl = URL.createObjectURL(blob);

  const anchor = document.createElement("a");
  anchor.href = downloadUrl;
  anchor.download = filename;
  anchor.click();

  return {
    success: true,
    records_transferred: mapped.length,
    destination: {
      database: "local",
      collection: filename,
      path: filename,
      format: input.format,
      filename,
      download_url: downloadUrl,
    },
    destination_summary: {
      type: "file_export",
      filename,
      download_url: downloadUrl,
      driver: "browser",
      warnings: ["Exported locally — start the API for governed Job Theater proof."],
    },
    reconciliation: {
      passed: true,
      source_rows: mapped.length,
      target_rows: mapped.length,
      message: "Browser export preview — full reconciliation requires API.",
    },
    event_log: [
      `export · ${mapped.length} rows → ${filename} · ${new Date().toISOString()}`,
    ],
  };
}
