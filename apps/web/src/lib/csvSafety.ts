/**
 * Spreadsheet formula-injection guard for CSV/TSV the browser builds (G7-F2, CWE-1236).
 *
 * The one TypeScript copy of the API's rule in `apps/api/services/csv_safety.py`
 * (OWASP): a text cell whose first character is `=`, `+`, `-`, `@`, a tab or a CR
 * is executed as a formula when the file is opened in Excel/Sheets, so it gets a
 * leading `'`. Plain numerals (`-5`, `+3`, `-1.5`, `-1e-3`, ASCII digits only) are
 * numbers, never formulas, and stay intact. Text that already starts with quotes
 * before a formula leader (`'=x`) gains one more quote, so the guard reverses
 * exactly (`restoreExportCell`).
 *
 * Both implementations are checked against one vector file,
 * `apps/api/tests/fixtures/formula_guard_vectors.json`, so they cannot drift.
 */

const FORMULA_LEADERS = new Set(["=", "+", "-", "@", "\t", "\r"]);

/** Python `[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z` with `re.ASCII`. */
const NUMERIC_LITERAL = /^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$/;

/**
 * Whether the formula guard applies to an export format: delimited text only
 * (XLSX writes text as typed string cells, never formulas; JSON is not opened
 * as a spreadsheet).
 */
export function formulaGuardApplies(format: string | null | undefined): boolean {
  const fmt = String(format ?? "").trim().toLowerCase();
  return fmt === "csv" || fmt === "tsv";
}

/** The text a cell is written as: null/undefined is empty, everything else `String()`. */
export function cellText(value: unknown): string {
  return value === null || value === undefined ? "" : String(value);
}

function exportCellNeedsGuard(text: string): boolean {
  const core = text.replace(/^'+/, "");
  return core.length > 0 && FORMULA_LEADERS.has(core[0]) && !NUMERIC_LITERAL.test(core);
}

/** Mirror of `neutralize_export_cell`: `'`-prefix a formula-looking text cell; numerals are exempt. */
export function neutralizeExportCell(value: unknown): string {
  const text = cellText(value);
  return exportCellNeedsGuard(text) ? `'${text}` : text;
}

/** Mirror of `restore_export_cell`: the exact value `neutralizeExportCell` was given. */
export function restoreExportCell(text: string): string {
  return text.startsWith("'") && exportCellNeedsGuard(text.slice(1)) ? text.slice(1) : text;
}

/** Mirror of `neutralize_formula_cell` (operator reports): any formula leader, numerals included. */
export function neutralizeFormulaCell(value: unknown): string {
  const text = cellText(value);
  return text.length > 0 && FORMULA_LEADERS.has(text[0]) ? `'${text}` : text;
}

export interface DelimitedOptions {
  /** `true` applies `neutralizeExportCell` to every cell (header names included). */
  formulaProtection?: boolean;
  /** Field delimiter; `,` by default. */
  delimiter?: string;
  /** Quote every cell (the quarantine report's layout). */
  quoteAll?: boolean;
  /**
   * TSV NULL marker (`\N`, G7-F3): null/undefined is written as the marker, `''`
   * as an empty cell, and a text equal to the marker is quoted (read back as text).
   */
  nullMarker?: string;
}

/**
 * One cell of a delimited row: guarded when asked, then RFC 4180-quoted when it
 * holds the delimiter, a `"`, CR or LF (any delimiter, so a tab or newline inside
 * a TSV value can never split the row or start a new formula cell).
 */
export function delimitedCell(value: unknown, options: DelimitedOptions = {}): string {
  const delimiter = options.delimiter ?? ",";
  if (options.nullMarker && (value === null || value === undefined)) return options.nullMarker;
  const text = options.formulaProtection ? neutralizeExportCell(value) : cellText(value);
  const needsQuotes = options.quoteAll || text.includes(delimiter) || /["\r\n]/.test(text)
    || (Boolean(options.nullMarker) && text === options.nullMarker);
  return needsQuotes ? `"${text.replace(/"/g, '""')}"` : text;
}

/** A header row plus data rows as delimited text (rows joined with `\n`, no trailing newline). */
export function toDelimitedText(
  columns: string[],
  rows: Array<Record<string, unknown>>,
  options: DelimitedOptions & { cell?: (value: unknown) => unknown } = {},
): string {
  const delimiter = options.delimiter ?? ",";
  const cell = options.cell ?? ((value: unknown) => value);
  const line = (values: unknown[]) => values.map((v) => delimitedCell(v, options)).join(delimiter);
  return [line(columns), ...rows.map((row) => line(columns.map((c) => cell(row[c]))))].join("\n");
}
