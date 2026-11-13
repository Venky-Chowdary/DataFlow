/**
 * The Studio file_export destination as the API receives it (G7-F2).
 *
 * `formulaProtection` is the destination's "Protect against spreadsheet formulas"
 * option. A NEW Studio job starts with it on: a CSV/TSV is written with
 * `formula_protection: "prefix"` (csv_safety), which Gate-8 reverses on read-back,
 * so the proof still compares the exact values. A saved job keeps what it stored —
 * no field means the API default "off" (byte-exact) — so reopening or re-running
 * an existing job never changes its output. Other formats ignore it.
 */
import { formulaGuardApplies } from "./csvSafety";

/** A new Studio file_export (and the offline browser export) protects by default. */
export const FILE_EXPORT_FORMULA_PROTECTION_DEFAULT = true;

/** The toggle's trade-off, shown under it in Studio. */
export const FILE_EXPORT_FORMULA_PROTECTION_HELP =
  "Cells starting with = + - @ get a ' prefix so spreadsheets don't execute them. The prefix is "
  + "reversible: Gate-8 strips it and proves the exact values. Numbers such as -2 are untouched. "
  + "Turn off for a byte-exact file that only machines read.";

/** The same trade-off for the Query console download. */
export const QUERY_EXPORT_FORMULA_PROTECTION_HELP =
  "Cells starting with = + - @ get a ' prefix so spreadsheets don't execute them. The prefix is "
  + "reversible (drop the leading ' to get the exact value). Numbers such as -2 are untouched. "
  + "Turn off for exact values.";

// ── TSV NULL marker (G7-F3) ────────────────────────────────────────────────

/** `tsv_null`: "empty" (default — SQL NULL is an empty cell) or `\N` (MySQL/PostgreSQL text format). */
export type TsvNull = "empty" | "\\N";
export const TSV_NULL_TEXT: TsvNull = "\\N";
export const TSV_NULL_DEFAULT: TsvNull = "empty";
export const TSV_NULL_CHOICES: ReadonlyArray<{ value: TsvNull; label: string }> = [
  { value: "empty", label: "Empty cell — default" },
  { value: "\\N", label: "\\N" },
];
export const TSV_NULL_HELP =
  "Empty cell: NULL and an empty string are both written as an empty cell. \\N: NULL is written as \\N "
  + "(MySQL/PostgreSQL text format) and an empty string stays an empty cell, so the two read back apart.";

/** Object-store / file-server destinations whose file format follows the object name. */
const OBJECT_FILE_DESTS = new Set([
  "s3", "gcs", "adls", "azure_blob", "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage",
  "minio", "r2", "sftp",
]);

/** Whether a destination writes a TSV file, so the NULL marker choice applies. */
export function tsvNullApplies(
  dest: { kind: "file_export"; format: string } | { kind: "database"; type: string; objectName: string },
): boolean {
  if (dest.kind === "file_export") return String(dest.format).trim().toLowerCase() === "tsv";
  return OBJECT_FILE_DESTS.has(String(dest.type).trim().toLowerCase())
    && /\.tsv$/i.test(String(dest.objectName).trim());
}

/** The NULL marker a saved destination was written with (absent = the default). */
export function destTsvNull(destination: { extra?: unknown; tsv_null?: unknown }): TsvNull {
  const extra = (destination.extra ?? {}) as Record<string, unknown>;
  return String(extra.tsv_null ?? destination.tsv_null ?? "").trim() === TSV_NULL_TEXT ? TSV_NULL_TEXT : TSV_NULL_DEFAULT;
}

/** `{ tsv_null: "\\N" }` for an object-store TSV destination that asks for it, else `{}`. */
export function objectStoreDestExtra(dest: { type: string; objectName: string; tsvNull: TsvNull }): Record<string, unknown> {
  return dest.tsvNull === TSV_NULL_TEXT && tsvNullApplies({ kind: "database", type: dest.type, objectName: dest.objectName })
    ? { tsv_null: TSV_NULL_TEXT }
    : {};
}

export interface FileExportOptions {
  format: string;
  formulaProtection: boolean;
  /** TSV only; the default writes NULL as an empty cell. */
  tsvNull?: TsvNull;
}

/** The writer options (`extra`) a file_export destination carries; `{}` when none apply. */
export function fileExportDestExtra(options: FileExportOptions): Record<string, unknown> {
  return {
    ...(options.formulaProtection && formulaGuardApplies(options.format) ? { formula_protection: "prefix" } : {}),
    ...(options.tsvNull === TSV_NULL_TEXT && tsvNullApplies({ kind: "file_export", format: options.format })
      ? { tsv_null: TSV_NULL_TEXT }
      : {}),
  };
}

/** The file_export destination endpoint for plans, route analysis and contracts. */
export function fileExportEndpoint(
  options: FileExportOptions & { database: string; outputPath: string },
): Record<string, unknown> {
  const extra = fileExportDestExtra(options);
  return {
    kind: "file_export",
    format: options.format,
    database: options.database,
    output_path: options.outputPath,
    ...(Object.keys(extra).length ? { extra } : {}),
  };
}

/** The option a saved job's file_export destination was written with (restoring a route). */
export function fileExportFormulaProtection(destination: { extra?: unknown; formula_protection?: unknown }): boolean {
  const extra = (destination.extra ?? {}) as Record<string, unknown>;
  const mode = String(extra.formula_protection ?? destination.formula_protection ?? "").trim().toLowerCase();
  return mode === "prefix";
}
