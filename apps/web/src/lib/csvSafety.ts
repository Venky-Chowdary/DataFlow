/**
 * Spreadsheet formula-injection guard for CSV/TSV exports (OWASP).
 * A cell starting with `= + - @ \t \r` executes as a formula in Excel/Sheets;
 * a leading `'` renders it as text. Mirrors `services/csv_safety.py`.
 */
const FORMULA_PREFIXES = ["=", "+", "-", "@", "\t", "\r"];

export function neutralizeFormulaCell(value: unknown): string {
  const text = value == null ? "" : String(value);
  if (text && FORMULA_PREFIXES.includes(text[0])) return `'${text}`;
  return text;
}
