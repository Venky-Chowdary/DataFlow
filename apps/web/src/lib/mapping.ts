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
  | "parse_json"
  | "strip_controls";

export const MAPPING_TRANSFORMS: { id: MappingTransform; label: string; detail: string }[] = [
  { id: "none", label: "None", detail: "Pass through as detected" },
  { id: "trim", label: "Trim", detail: "Strip leading/trailing whitespace" },
  { id: "strip_controls", label: "Strip controls", detail: "Remove zero-width / null / format-control chars (warehouse-safe)" },
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
  destType?: string;
  sample?: string;
  approved: boolean;
  isPii?: boolean;
  reason?: string;
  existsInDestination?: boolean;
  transform?: MappingTransform;
  requiresReview?: boolean;
  scoreGap?: number;
  /** From schema intelligence — e.g. string_enum, boolean_flag */
  semanticRole?: string;
  /** Intentionally ADD COLUMN / create-new (e.g. ObjectId → _id beside DECIMAL id). */
  createNew?: boolean;
  assignmentStrategy?: string;
}

const STATUS_ENUM_TOKENS = new Set([
  "active", "inactive", "enabled", "disabled", "pending", "invalidated",
  "approved", "rejected", "completed", "cancelled", "canceled", "draft",
  "published", "archived", "deleted", "suspended", "processing", "queued",
]);

const STRICT_BOOL_TOKENS = new Set([
  "true", "false", "t", "f", "yes", "no", "y", "n", "0", "1", "on", "off",
]);

function looksLikeStringEnumSample(sample?: string, semanticRole?: string): boolean {
  if (semanticRole === "string_enum") return true;
  if (!sample) return false;
  const token = sample.trim().toLowerCase();
  if (!token || STRICT_BOOL_TOKENS.has(token)) return false;
  return STATUS_ENUM_TOKENS.has(token) || /^[a-z][a-z0-9_\-]{1,31}$/i.test(token);
}

export function isEnumToBooleanConflict(m: EditableMapping): boolean {
  const dest = (m.destType || "").toLowerCase();
  const destIsBool = dest.includes("bool");
  const transformIsBool = m.transform === "cast_boolean";
  if (!destIsBool && !transformIsBool) return false;
  return looksLikeStringEnumSample(m.sample, m.semanticRole)
    || (m.inferredType || "").toLowerCase().includes("varchar")
    || (m.inferredType || "").toLowerCase().includes("text")
    || (m.inferredType || "").toLowerCase().includes("string")
    || m.semanticRole === "string_enum";
}

/** Widen destination type to VARCHAR and clear cast_boolean — safe for **new** tables only. */
export function widenMappingToVarchar(m: EditableMapping): EditableMapping {
  return {
    ...m,
    destType: "VARCHAR",
    transform: m.transform === "cast_boolean" ? "none" : m.transform,
    approved: false,
    requiresReview: false,
    reason: [m.reason, "Widened to VARCHAR (string enum — not boolean)"].filter(Boolean).join(" · "),
  };
}

/**
 * True when enum→BOOLEAN conflict hits an **existing** destination column.
 * Mapping-only Widen cannot ALTER physical BOOLEAN → VARCHAR.
 */
export function isExistingEnumBooleanConflict(m: EditableMapping): boolean {
  return Boolean(m.existsInDestination && isEnumToBooleanConflict(m));
}

/** Flag for review without pretending the physical column type changed. */
export function flagExistingEnumBooleanConflict(m: EditableMapping): EditableMapping {
  return {
    ...m,
    approved: false,
    requiresReview: true,
    transform: m.transform === "cast_boolean" ? "none" : m.transform,
    reason: [
      m.reason,
      "Existing destination is BOOLEAN but samples are a string enum — remap to a VARCHAR column or ALTER the destination; mapping Widen alone will not change DDL",
    ]
      .filter(Boolean)
      .join(" · "),
  };
}

/** Semantic target column name — matches MappingCanvas normalization. */
export function normalizeMappingTarget(name: string, col?: Pick<ColumnAnalysis, "canonical_form">): string {
  if (col?.canonical_form) return col.canonical_form;
  return name
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .replace(/[\s-]+/g, "_")
    .toLowerCase();
}

function boostIdentityConfidence(
  source: string,
  target: string,
  confidence: number,
  createNew = false,
): number {
  const norm = normalizeMappingTarget(source);
  if (norm === target || source.toLowerCase() === target.toLowerCase()) {
    // Create-new identity is "ready to CREATE", not proven 99% against existing dest.
    if (createNew) return Math.min(Math.max(confidence, 0.9), 0.93);
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
      strip_controls: "strip_controls",
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
    return editable.map((m) => {
      const enumBool = isEnumToBooleanConflict(m);
      // Existing physical BOOLEAN: keep live dest type so preflight DDL gate sees the conflict.
      const safe =
        enumBool && !m.existsInDestination
          ? widenMappingToVarchar(m)
          : enumBool && m.existsInDestination
            ? flagExistingEnumBooleanConflict(m)
            : m;
      return {
        source: safe.source,
        target: safe.target,
        confidence: safe.confidence,
        reason: safe.reason || "User reviewed",
        user_override: safe.approved && !enumBool,
        transform: toEngineTransform(safe.transform),
        // Prefer live dest type when column already exists (e.g. BOOLEAN status).
        target_type: m.existsInDestination
          ? (m.destType || safe.destType || safe.inferredType)
          : (safe.destType || safe.inferredType),
        source_type: safe.inferredType,
        requires_review: Boolean((safe.requiresReview || enumBool) && !safe.approved),
        score_gap: safe.scoreGap ?? 1,
        semantic_role: safe.semanticRole,
        // Preserve create-new so Validate G6 matches Execute ADD COLUMN behavior.
        create_new: Boolean(
          safe.createNew
          || safe.assignmentStrategy === "create_compatible_new"
          || (safe.existsInDestination === false && Boolean(safe.target)),
        ),
        assignment_strategy:
          safe.assignmentStrategy
          || (safe.createNew || safe.existsInDestination === false
            ? "create_compatible_new"
            : undefined),
      };
    });
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
  if (!engine || engine === "none" || engine === "identity") return "none";
  const map: Record<string, MappingTransform> = {
    trim: "trim",
    trim_id: "trim",
    strip_controls: "strip_controls",
    normalize_unicode: "strip_controls",
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
    target_type?: string;
    is_pii?: boolean;
    semantic_role?: string;
    assignment_strategy?: string;
    create_new?: boolean;
  }>,
  sampleRows?: Record<string, unknown>[],
  destColumns?: string[],
  threshold = 0.75,
  destSchema?: Record<string, string>,
): EditableMapping[] {
  const destSet = new Set((destColumns ?? []).map((c) => c.toLowerCase()));
  const createNew =
    (destColumns?.length ?? 0) === 0
    || mappings.some((m) => m.assignment_strategy === "identity_passthrough" || m.create_new);
  const destTypeByLower = new Map(
    Object.entries(destSchema || {}).map(([k, v]) => [k.toLowerCase(), v]),
  );
  return mappings.map((m) => {
    const sampleVal = sampleRows?.find((r) => r[m.source] != null)?.[m.source];
    const existsInDest = destSet.has(m.target.toLowerCase());
    const liveDestType = destTypeByLower.get(m.target.toLowerCase());
    const conf = boostIdentityConfidence(m.source, m.target, m.confidence, createNew);
    const requiresReview = Boolean(m.requires_review);
    const identityMatch = normalizeMappingTarget(m.source) === m.target.toLowerCase();
    const base: EditableMapping = {
      source: m.source,
      target: m.target,
      confidence: conf,
      inferredType: m.source_type,
      // Prefer live destination DDL type when the column already exists.
      destType: liveDestType || m.target_type || m.source_type,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: !requiresReview && (conf >= threshold || identityMatch),
      isPii: m.is_pii,
      reason: m.reasoning,
      existsInDestination: existsInDest,
      requiresReview,
      scoreGap: m.score_gap,
      transform: m.is_pii ? "hash_pii" : engineTransformToUi(m.transform),
      semanticRole: m.semantic_role,
      createNew: Boolean(m.create_new) || m.assignment_strategy === "create_compatible_new",
      assignmentStrategy: m.assignment_strategy,
    };
    if (isEnumToBooleanConflict(base)) {
      if (base.existsInDestination) {
        return flagExistingEnumBooleanConflict(base);
      }
      return {
        ...widenMappingToVarchar(base),
        requiresReview: true,
        approved: false,
      };
    }
    return base;
  });
}

export function confidenceThresholdForMode(mode: string): number {
  if (mode === "balanced") return 0.75;
  if (mode === "maximum") return 0.95;
  return 0.85;
}
