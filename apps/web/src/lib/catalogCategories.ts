/**
 * Catalog category names as the operator reads them (UX-29).
 *
 * The catalog panel printed ids the label map did not know ("vector",
 * "lake", "streaming") lowercase. Every id now reads as words.
 */
export const CATEGORY_LABELS: Record<string, string> = {
  database: "Databases",
  warehouse: "Data Warehouses",
  file: "Files & Formats",
  cloud_storage: "Cloud Storage",
  saas: "SaaS Applications",
  api: "APIs & Webhooks",
  finance: "Finance",
  healthcare: "Healthcare",
  marketing: "Marketing",
  logistics: "Logistics",
  vector: "Vector Databases",
  lake: "Lakehouse",
  streaming: "Streaming",
};

/** A category the label map does not name yet still reads as words, not an id (UX-29). */
export function catalogCategoryLabel(id: string): string {
  if (CATEGORY_LABELS[id]) return CATEGORY_LABELS[id];
  return id
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

