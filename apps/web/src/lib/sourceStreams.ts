/**
 * Parse comma-separated table/collection names for multi-stream CDC / incremental.
 * Trims whitespace, drops empties, de-dupes while preserving order.
 */
export function parseStreamNames(input: string | undefined | null): string[] {
  if (!input?.trim()) return [];
  const seen = new Set<string>();
  const out: string[] = [];
  for (const part of input.split(",")) {
    const name = part.trim();
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(name);
  }
  return out;
}

export function primaryStreamName(input: string | undefined | null): string {
  return parseStreamNames(input)[0] || (input || "").trim();
}

/**
 * One banner for a multi-stream read that produced no schema.
 * Identical connector errors are stated once; distinct errors stay with their names.
 */
export function summarizeStreamReadFailures(
  failures: { name: string; error?: string }[],
): string {
  const rows = failures.filter((row) => row.name);
  if (!rows.length) return "Could not read source schema.";
  const textOf = (row: { error?: string }) => (row.error || "Could not read this stream.").trim();
  if (rows.length === 1) return textOf(rows[0]);
  const unique = [...new Set(rows.map(textOf))];
  const names = rows.map((row) => row.name).join(", ");
  if (unique.length === 1) {
    return `None of the ${rows.length} streams could be read (${names}). Fix the connector, then retry.`;
  }
  const byError = new Map<string, string[]>();
  for (const row of rows) {
    const err = textOf(row);
    const list = byError.get(err) ?? [];
    list.push(row.name);
    byError.set(err, list);
  }
  const parts = [...byError.entries()].map(([err, streamNames]) => `${streamNames.join(", ")}: ${err}`);
  return `None of the ${rows.length} streams could be read. ${parts.join(" · ")}`;
}

export type StreamSchemaPreview = {
  name: string;
  status: "idle" | "loading" | "ok" | "error";
  columns: string[];
  schema: Record<string, string>;
  rows: Record<string, unknown>[];
  rowEstimate?: number;
  error?: string;
};
