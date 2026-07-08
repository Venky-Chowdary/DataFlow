import type { ColumnAnalysis } from "./types";

/** Semantic target column name — matches MappingCanvas normalization. */
export function normalizeMappingTarget(name: string, col?: Pick<ColumnAnalysis, "canonical_form">): string {
  if (col?.canonical_form) return col.canonical_form;
  return name
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .replace(/[\s-]+/g, "_")
    .toLowerCase();
}

export function buildPreflightMappings(columns: ColumnAnalysis[]) {
  return columns.map((col) => ({
    source: col.column_name,
    target: normalizeMappingTarget(col.column_name, col),
    confidence: col.confidence,
    reason: col.semantic_type || col.inferred_type || "Semantic match",
  }));
}
