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

export interface FileExportOptions {
  format: string;
  formulaProtection: boolean;
}

/** The writer options (`extra`) a file_export destination carries; `{}` when none apply. */
export function fileExportDestExtra(options: FileExportOptions): Record<string, unknown> {
  return options.formulaProtection && formulaGuardApplies(options.format) ? { formula_protection: "prefix" } : {};
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
