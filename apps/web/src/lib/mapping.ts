import type { ColumnAnalysis } from "./types";

export type MappingTransform =
  | "none"
  | "trim"
  | "upper"
  | "lower"
  | "date_iso"
  | "hash_pii"
  | "cast_number"
  | "cast_boolean"
  | "parse_json";

export const MAPPING_TRANSFORMS: { id: MappingTransform; label: string; detail: string }[] = [
  { id: "none", label: "None", detail: "Pass through as detected" },
  { id: "trim", label: "Trim", detail: "Strip leading/trailing whitespace" },
  { id: "upper", label: "Uppercase", detail: "Normalize to UPPER CASE" },
  { id: "lower", label: "Lowercase", detail: "Normalize to lower case" },
  { id: "date_iso", label: "Date → ISO", detail: "Parse dates to ISO-8601" },
  { id: "hash_pii", label: "Hash PII", detail: "One-way hash for sensitive fields" },
  { id: "cast_number", label: "Cast number", detail: "Coerce to numeric" },
  { id: "cast_boolean", label: "Cast boolean", detail: "Coerce to true/false" },
  { id: "parse_json", label: "Parse JSON", detail: "Normalize JSON payloads into structured objects" },
];

export interface EditableMapping {
  source: string;
  target: string;
  confidence: number;
  inferredType?: string;
  sample?: string;
  approved: boolean;
  isPii?: boolean;
  reason?: string;
  existsInDestination?: boolean;
  transform?: MappingTransform;
  requiresReview?: boolean;
  scoreGap?: number;
}

/** Semantic target column name — matches MappingCanvas normalization. */
export function normalizeMappingTarget(name: string, col?: Pick<ColumnAnalysis, "canonical_form">): string {
  if (col?.canonical_form) return col.canonical_form;
  return name
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .replace(/[\s-]+/g, "_")
    .toLowerCase();
}

function boostIdentityConfidence(source: string, target: string, confidence: number): number {
  const norm = normalizeMappingTarget(source);
  if (norm === target || source.toLowerCase() === target.toLowerCase()) {
    return Math.max(confidence, 0.95);
  }
  return confidence;
}

export function mappingsFromAnalysis(
  columns: ColumnAnalysis[],
  sampleRows?: Record<string, unknown>[],
): EditableMapping[] {
  return columns.map((col) => {
    const target = normalizeMappingTarget(col.column_name, col);
    const sampleVal = sampleRows?.find((r) => r[col.column_name] != null)?.[col.column_name];
    const conf = boostIdentityConfidence(col.column_name, target, col.confidence);
    return {
      source: col.column_name,
      target,
      confidence: conf,
      inferredType: col.semantic_type || col.inferred_type || "string",
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: conf >= 0.9 && !col.is_pii,
      isPii: col.is_pii,
      reason: col.semantic_type || col.inferred_type || "Semantic match",
      transform: col.is_pii ? "hash_pii" : "none",
    };
  });
}

export function buildPreflightMappings(
  columns: ColumnAnalysis[],
  editable?: EditableMapping[],
) {
  const toEngineTransform = (t?: MappingTransform): string | undefined => {
    if (!t || t === "none") return undefined;
    const map: Partial<Record<MappingTransform, string>> = {
      trim: "trim",
      upper: "upper",
      lower: "lower",
      date_iso: "datetime",
      hash_pii: "hash_pii",
      cast_number: "decimal",
      cast_boolean: "boolean",
      parse_json: "json",
    };
    return map[t];
  };

  if (editable?.length) {
    return editable.map((m) => ({
      source: m.source,
      target: m.target,
      confidence: m.confidence,
      reason: m.reason || "User reviewed",
      user_override: m.approved,
      transform: toEngineTransform(m.transform),
      target_type: m.inferredType,
      requires_review: Boolean(m.requiresReview && !m.approved),
      score_gap: m.scoreGap ?? 1,
    }));
  }
  return columns.map((col) => {
    const target = normalizeMappingTarget(col.column_name, col);
    return {
      source: col.column_name,
      target,
      confidence: boostIdentityConfidence(col.column_name, target, col.confidence),
      reason: col.semantic_type || col.inferred_type || "Semantic match",
      user_override: col.confidence >= 0.9,
    };
  });
}

export function engineTransformToUi(engine?: string): MappingTransform {
  if (!engine) return "none";
  const map: Record<string, MappingTransform> = {
    trim: "none",
    trim_id: "none",
    uuid: "none",
    upper: "upper",
    lower: "lower",
    date: "date_iso",
    datetime: "date_iso",
    decimal: "cast_number",
    integer: "cast_number",
    boolean: "cast_boolean",
    hash_pii: "hash_pii",
    json: "parse_json",
  };
  return map[engine] ?? "none";
}

export function editableFromPipelineMappings(
  mappings: Array<{
    source: string;
    target: string;
    confidence: number;
    reasoning?: string;
    requires_review?: boolean;
    score_gap?: number;
    transform?: string;
    source_type?: string;
    is_pii?: boolean;
  }>,
  sampleRows?: Record<string, unknown>[],
  destColumns?: string[],
  threshold = 0.75,
): EditableMapping[] {
  const destSet = new Set((destColumns ?? []).map((c) => c.toLowerCase()));
  return mappings.map((m) => {
    const sampleVal = sampleRows?.find((r) => r[m.source] != null)?.[m.source];
    const existsInDest = destSet.has(m.target.toLowerCase());
    const conf = boostIdentityConfidence(m.source, m.target, m.confidence);
    const requiresReview = Boolean(m.requires_review);
    const identityMatch = normalizeMappingTarget(m.source) === m.target.toLowerCase();
    return {
      source: m.source,
      target: m.target,
      confidence: conf,
      inferredType: m.source_type,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: !requiresReview && (conf >= threshold || identityMatch),
      isPii: m.is_pii,
      reason: m.reasoning,
      existsInDestination: existsInDest,
      requiresReview,
      scoreGap: m.score_gap,
      transform: m.is_pii ? "hash_pii" : engineTransformToUi(m.transform),
    };
  });
}

export function confidenceThresholdForMode(mode: string): number {
  if (mode === "balanced") return 0.75;
  if (mode === "maximum") return 0.95;
  return 0.85;
}
