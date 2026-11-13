/**
 * Studio step-preview.csv (G7-F2): a preview a person opens in a spreadsheet,
 * so formula-looking cells and header names are defused with the API's rule
 * (csvSafety); CR/LF/quotes/commas are quoted.
 */
import { toDelimitedText } from "./csvSafety";

/** One preview cell as text: null/undefined empty, booleans true/false, objects JSON. */
export function previewCellText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function stepPreviewCsv(columns: string[], rows: Record<string, unknown>[]): string {
  return toDelimitedText(columns, rows, { formulaProtection: true, cell: previewCellText });
}
