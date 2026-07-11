import type { EditableMapping } from "./mapping";

export type ColumnFilter =
  | "all"
  | "review"
  | "block"
  | "warn"
  | "pii"
  | "new"
  | "ready";

export type ColumnSort = "confidence-asc" | "confidence-desc" | "name-asc" | "name-desc";

export const COLUMN_PAGE_SIZES = [25, 50, 100] as const;
export type ColumnPageSize = (typeof COLUMN_PAGE_SIZES)[number];

export type MappingTier = "ok" | "warn" | "block";

export interface IndexedMapping {
  mapping: EditableMapping;
  index: number;
}

export function mappingTier(
  m: EditableMapping,
  threshold: number,
): MappingTier {
  if (m.approved) return "ok";
  if (m.confidence >= threshold) return "ok";
  if (m.confidence >= threshold - 0.1) return "warn";
  return "block";
}

export function isMappingReady(m: EditableMapping, threshold: number): boolean {
  return m.approved || (!m.requiresReview && m.confidence >= threshold);
}

export function needsMappingReview(m: EditableMapping, threshold: number): boolean {
  return !m.approved && (m.requiresReview || m.confidence < threshold);
}

export function countByFilter(
  mappings: EditableMapping[],
  threshold: number,
): Record<ColumnFilter, number> {
  const counts: Record<ColumnFilter, number> = {
    all: mappings.length,
    review: 0,
    block: 0,
    warn: 0,
    pii: 0,
    new: 0,
    ready: 0,
  };

  for (const m of mappings) {
    const tier = mappingTier(m, threshold);
    if (needsMappingReview(m, threshold)) counts.review += 1;
    if (tier === "block") counts.block += 1;
    if (tier === "warn") counts.warn += 1;
    if (m.isPii) counts.pii += 1;
    if (!m.existsInDestination) counts.new += 1;
    if (isMappingReady(m, threshold)) counts.ready += 1;
  }

  return counts;
}

function matchesSearch(m: EditableMapping, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return (
    m.source.toLowerCase().includes(q)
    || m.target.toLowerCase().includes(q)
    || (m.inferredType ?? "").toLowerCase().includes(q)
    || (m.reason ?? "").toLowerCase().includes(q)
    || (m.sample ?? "").toLowerCase().includes(q)
  );
}

function matchesFilter(
  m: EditableMapping,
  filter: ColumnFilter,
  threshold: number,
): boolean {
  if (filter === "all") return true;
  if (filter === "review") return needsMappingReview(m, threshold);
  if (filter === "block") return mappingTier(m, threshold) === "block";
  if (filter === "warn") return mappingTier(m, threshold) === "warn";
  if (filter === "pii") return Boolean(m.isPii);
  if (filter === "new") return !m.existsInDestination;
  if (filter === "ready") return isMappingReady(m, threshold);
  return true;
}

function sortMappings(items: IndexedMapping[], sort: ColumnSort): IndexedMapping[] {
  const next = [...items];
  next.sort((a, b) => {
    if (sort === "name-asc") return a.mapping.source.localeCompare(b.mapping.source);
    if (sort === "name-desc") return b.mapping.source.localeCompare(a.mapping.source);
    if (sort === "confidence-asc") return a.mapping.confidence - b.mapping.confidence;
    return b.mapping.confidence - a.mapping.confidence;
  });
  return next;
}

export function filterMappings(
  mappings: EditableMapping[],
  options: {
    search: string;
    filter: ColumnFilter;
    sort: ColumnSort;
    threshold: number;
  },
): IndexedMapping[] {
  const indexed = mappings.map((mapping, index) => ({ mapping, index }));
  const filtered = indexed.filter(
    ({ mapping }) =>
      matchesSearch(mapping, options.search)
      && matchesFilter(mapping, options.filter, options.threshold),
  );
  return sortMappings(filtered, options.sort);
}

export function paginateMappings<T>(items: T[], page: number, pageSize: number): T[] {
  const start = (page - 1) * pageSize;
  return items.slice(start, start + pageSize);
}

export function totalPages(count: number, pageSize: number): number {
  return Math.max(1, Math.ceil(count / pageSize));
}

export type AttentionKind = "critical" | "review" | "pii" | "warn";

export interface AttentionItem {
  mapping: EditableMapping;
  index: number;
  kind: AttentionKind;
  tier: MappingTier;
}

/** Columns that need human attention — for the intelligence rail. */
export function attentionMappings(
  mappings: EditableMapping[],
  threshold: number,
  limit = 50,
): AttentionItem[] {
  const items: AttentionItem[] = [];
  mappings.forEach((mapping, index) => {
    const tier = mappingTier(mapping, threshold);
    const review = needsMappingReview(mapping, threshold);
    if (mapping.isPii) {
      items.push({ mapping, index, kind: "pii", tier });
    } else if (tier === "block") {
      items.push({ mapping, index, kind: "critical", tier });
    } else if (review) {
      items.push({ mapping, index, kind: "review", tier });
    } else if (tier === "warn") {
      items.push({ mapping, index, kind: "warn", tier });
    }
  });
  items.sort((a, b) => a.mapping.confidence - b.mapping.confidence);
  return items.slice(0, limit);
}
