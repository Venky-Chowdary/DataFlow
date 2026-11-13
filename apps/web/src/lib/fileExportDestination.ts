/**
 * The Studio file_export destination as the API receives it (G7-F2).
 *
 * `formulaProtection` is the destination's "Protect against spreadsheet formulas"
 * option. The API default for a transfer job's file_export is "off": the file is
 * data (Gate-8 re-reads it, downstream loads ingest it) and stays byte-exact. On,
 * a CSV/TSV is written with `formula_protection: "prefix"` (csv_safety), which
 * Gate-8 reverses on read-back. Other formats ignore it.
 */
import { formulaGuardApplies } from "./csvSafety";

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
