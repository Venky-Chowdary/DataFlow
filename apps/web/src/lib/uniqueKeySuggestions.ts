/** Rank source columns that look unique in a Validate/Map sample (honest sample-only). */

export type UniqueKeySuggestion = {
  column: string;
  uniqueCount: number;
  nullCount: number;
  sampleRows: number;
  /** uniqueCount / non-null rows in sample */
  uniquenessRatio: number;
};

function cellKey(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/**
 * Suggest identity columns from preview rows.
 * Label in UI as "unique in sample (N rows)" — never claim full-table uniqueness.
 */
export function suggestUniqueKeyCandidates(
  sampleRows: Record<string, unknown>[] | null | undefined,
  columns: string[],
  opts?: { exclude?: string[]; limit?: number },
): UniqueKeySuggestion[] {
  const rows = sampleRows || [];
  if (!rows.length || !columns.length) return [];
  const exclude = new Set((opts?.exclude || []).map((c) => c.toLowerCase()));
  const limit = opts?.limit ?? 5;
  const out: UniqueKeySuggestion[] = [];

  for (const column of columns) {
    if (!column || exclude.has(column.toLowerCase())) continue;
    const seen = new Set<string>();
    let nullCount = 0;
    for (const row of rows) {
      const raw = row[column];
      const key = cellKey(raw);
      if (!key) {
        nullCount += 1;
        continue;
      }
      seen.add(key);
    }
    const nonNull = rows.length - nullCount;
    if (nonNull < 2) continue;
    const uniqueCount = seen.size;
    if (uniqueCount < nonNull) continue; // has duplicates in sample
    if (nullCount > Math.floor(rows.length * 0.1)) continue; // too many nulls
    out.push({
      column,
      uniqueCount,
      nullCount,
      sampleRows: rows.length,
      uniquenessRatio: uniqueCount / nonNull,
    });
  }

  out.sort((a, b) => {
    if (b.uniquenessRatio !== a.uniquenessRatio) return b.uniquenessRatio - a.uniquenessRatio;
    if (a.nullCount !== b.nullCount) return a.nullCount - b.nullCount;
    // Prefer natural key names after uniqueness.
    const rank = (c: string) => {
      const n = c.toLowerCase();
      if (n === "fingerprint" || n === "dedup_hash") return 0;
      if (n === "external_id" || n === "job_id") return 1;
      if (n.endsWith("_id") || n === "uuid") return 2;
      return 3;
    };
    return rank(a.column) - rank(b.column) || a.column.localeCompare(b.column);
  });

  return out.slice(0, limit);
}

export type CompositeUniqueKeySuggestion = {
  columns: string[];
  uniqueCount: number;
  nullCount: number;
  sampleRows: number;
  uniquenessRatio: number;
};

/**
 * Suggest 2-column composite identity keys from preview rows.
 * Honest sample-only — never claim full-table uniqueness.
 */
export function suggestCompositeUniqueKeyCandidates(
  sampleRows: Record<string, unknown>[] | null | undefined,
  columns: string[],
  opts?: { exclude?: string[]; limit?: number },
): CompositeUniqueKeySuggestion[] {
  const rows = sampleRows || [];
  if (rows.length < 2 || columns.length < 2) return [];
  const exclude = new Set((opts?.exclude || []).map((c) => c.toLowerCase()));
  const limit = opts?.limit ?? 3;
  const candidates = columns.filter((c) => c && !exclude.has(c.toLowerCase()));
  const out: CompositeUniqueKeySuggestion[] = [];

  for (let i = 0; i < candidates.length; i += 1) {
    for (let j = i + 1; j < candidates.length; j += 1) {
      const a = candidates[i];
      const b = candidates[j];
      const seen = new Set<string>();
      let nullCount = 0;
      for (const row of rows) {
        const ka = cellKey(row[a]);
        const kb = cellKey(row[b]);
        if (!ka || !kb) {
          nullCount += 1;
          continue;
        }
        seen.add(`${ka}\u0001${kb}`);
      }
      const nonNull = rows.length - nullCount;
      if (nonNull < 2) continue;
      if (seen.size < nonNull) continue;
      if (nullCount > Math.floor(rows.length * 0.1)) continue;
      out.push({
        columns: [a, b],
        uniqueCount: seen.size,
        nullCount,
        sampleRows: rows.length,
        uniquenessRatio: seen.size / nonNull,
      });
    }
  }

  out.sort((x, y) => {
    if (y.uniquenessRatio !== x.uniquenessRatio) return y.uniquenessRatio - x.uniquenessRatio;
    return x.nullCount - y.nullCount;
  });
  return out.slice(0, limit);
}
