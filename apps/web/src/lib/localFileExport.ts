import { toDelimitedText } from "./csvSafety";
import { FILE_EXPORT_FORMULA_PROTECTION_DEFAULT } from "./fileExportDestination";
import type { EditableMapping } from "./mapping";
import { applyLocalTransform, requiresServerPreview } from "./localTransform";
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

export interface SerializeOptions {
  /**
   * The Studio file_export destination's "Protect against spreadsheet formulas"
   * (`formula_protection: "prefix"`): CSV/TSV text cells and header names that
   * would open as a formula get a leading `'` — the API's rule (csvSafety).
   * Off (the destination default) keeps the file byte-exact, as the server's
   * file_export writes it.
   */
  formulaProtection?: boolean;
}

export function serializeRows(
  rows: Record<string, unknown>[],
  format: string,
  options: SerializeOptions = {},
): { blob: Blob; mime: string; ext: string } {
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
  // RFC 4180 for any delimiter (a CR, LF, quote or the delimiter is quoted) and, when
  // the destination opts in, the same formula guard the API applies.
  const text = toDelimitedText(headers, rows, { delimiter, formulaProtection: options.formulaProtection });
  const mime = fmt === "tsv" ? "text/tab-separated-values" : "text/csv";
  return { blob: new Blob([text], { type: mime }), mime, ext: fmt === "tsv" ? "tsv" : "csv" };
}

export interface LocalFileExportInput {
  sourceFilename: string;
  rows: Record<string, unknown>[];
  mappings: EditableMapping[];
  format: string;
  outputBasename?: string;
  numberLocale?: NumberLocale | string;
  /**
   * The destination's formula_protection is "prefix" (see SerializeOptions).
   * Unset follows a new Studio job's default (on), as the server run would.
   */
  formulaProtection?: boolean;
}

/** Export mapped rows in the browser when the transfer API is offline. */
export function runLocalFileExport(input: LocalFileExportInput): TransferResult {
  const serverOnly = input.mappings.find((mapping) => requiresServerPreview(mapping.transform));
  if (serverOnly) {
    return {
      success: false,
      records_transferred: 0,
      error: `${serverOnly.transform} cannot be applied in the browser. Run the transfer so the server computes it.`,
      event_log: [`export refused · ${serverOnly.transform} is a server transform`],
    };
  }
  const mapped = input.rows.map((row) => mapRow(row, input.mappings, input.numberLocale));
  const { blob, ext } = serializeRows(mapped, input.format, {
    formulaProtection: input.formulaProtection ?? FILE_EXPORT_FORMULA_PROTECTION_DEFAULT,
  });
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
