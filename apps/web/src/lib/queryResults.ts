/**
 * Pure result-cell logic for the query workspace grid.
 *
 * Kept out of the component so the invariants that matter for a type-fidelity
 * product — an absent key, a JSON `null` and an empty string are three
 * different facts — are unit-testable without a DOM.
 */

export type CellKind = "missing" | "null" | "empty" | "json" | "value";

export interface Cell {
  kind: CellKind;
  text: string;
}

/** Classify a cell without flattening the three distinct kinds of "nothing". */
export function classifyCell(row: Record<string, unknown>, column: string): Cell {
  if (!Object.prototype.hasOwnProperty.call(row, column)) {
    return { kind: "missing", text: "(missing)" };
  }
  const v = row[column];
  if (v === null || v === undefined) return { kind: "null", text: "NULL" };
  if (typeof v === "object") return { kind: "json", text: JSON.stringify(v) };
  const s = String(v);
  if (s === "") return { kind: "empty", text: "(empty)" };
  return { kind: "value", text: s };
}

export type TypeTone =
  | "unknown"
  | "number"
  | "time"
  | "bool"
  | "struct"
  | "binary"
  | "text";

/** Map a logical type name onto a visual family. Unknown stays unknown. */
export function typeTone(type?: string): TypeTone {
  const t = (type || "").toUpperCase();
  if (!t || t === "UNKNOWN") return "unknown";
  if (/INT|DECIMAL|NUMERIC|FLOAT|DOUBLE|REAL|MONEY/.test(t)) return "number";
  if (/TIMESTAMP|DATE|TIME|INTERVAL/.test(t)) return "time";
  if (/BOOL/.test(t)) return "bool";
  if (/JSON|ARRAY|STRUCT|MAP|VARIANT|XML/.test(t)) return "struct";
  if (/BINARY|BLOB|BYTEA/.test(t)) return "binary";
  return "text";
}

/** Numeric-aware comparison so `10` sorts after `9`, with nulls last. */
export function compareValues(a: unknown, b: unknown): number {
  const aNull = a === null || a === undefined;
  const bNull = b === null || b === undefined;
  if (aNull && bNull) return 0;
  if (aNull) return 1;
  if (bNull) return -1;
  const an = typeof a === "number" ? a : Number(a);
  const bn = typeof b === "number" ? b : Number(b);
  if (
    !Number.isNaN(an) &&
    !Number.isNaN(bn) &&
    String(a).trim() !== "" &&
    String(b).trim() !== ""
  ) {
    return an - bn;
  }
  return String(a).localeCompare(String(b), undefined, { numeric: true });
}

/** Filter rows by per-column substring, matched against rendered cell text. */
export function filterRows(
  rows: Record<string, unknown>[],
  filters: Record<string, string>,
): Record<string, unknown>[] {
  const active = Object.entries(filters).filter(([, v]) => v.trim() !== "");
  if (active.length === 0) return rows;
  return rows.filter((r) =>
    active.every(([col, needle]) => {
      const { text } = classifyCell(r, col);
      return text.toLowerCase().includes(needle.trim().toLowerCase());
    }),
  );
}

/** Sort a copy — mutating fetched rows would desync the grid from history. */
export function sortRows(
  rows: Record<string, unknown>[],
  sort: { column: string; dir: "asc" | "desc" } | null,
): Record<string, unknown>[] {
  if (!sort) return rows;
  const out = [...rows];
  out.sort((a, b) => {
    const r = compareValues(a[sort.column], b[sort.column]);
    return sort.dir === "asc" ? r : -r;
  });
  return out;
}

/** Third click on a header restores result order rather than cycling forever. */
export function nextSort(
  prev: { column: string; dir: "asc" | "desc" } | null,
  column: string,
): { column: string; dir: "asc" | "desc" } | null {
  if (!prev || prev.column !== column) return { column, dir: "asc" };
  if (prev.dir === "asc") return { column, dir: "desc" };
  return null;
}

export function toCsv(columns: string[], rows: Record<string, unknown>[]): string {
  const esc = (v: unknown) => {
    if (v === null || v === undefined) return "";
    const s = typeof v === "object" ? JSON.stringify(v) : String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [
    columns.map(esc).join(","),
    ...rows.map((r) => columns.map((c) => esc(r[c])).join(",")),
  ].join("\n");
}

export function toMarkdown(columns: string[], rows: Record<string, unknown>[]): string {
  const cell = (v: unknown) => {
    if (v === null || v === undefined) return "NULL";
    const s = typeof v === "object" ? JSON.stringify(v) : String(v);
    return s.replace(/\|/g, "\\|");
  };
  return [
    `| ${columns.join(" | ")} |`,
    `| ${columns.map(() => "---").join(" | ")} |`,
    ...rows.map((r) => `| ${columns.map((c) => cell(r[c])).join(" | ")} |`),
  ].join("\n");
}
