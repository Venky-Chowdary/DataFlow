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

export type StreamSchemaPreview = {
  name: string;
  status: "idle" | "loading" | "ok" | "error";
  columns: string[];
  schema: Record<string, string>;
  rows: Record<string, unknown>[];
  rowEstimate?: number;
  error?: string;
};
