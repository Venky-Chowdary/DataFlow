import type { ColumnAnalysis } from "./types";

/**
 * Studio MappingTransform vocabulary — must stay aligned with
 * ``apps/api/services/transform_resolver.py`` UI_TO_ENGINE / ENGINE_TO_UI.
 * Prefer ``engineTransform`` on EditableMapping for round-trip fidelity when
 * the pipeline chose a semantic transform (phone/currency/…) not shown as a
 * first-class UI cast.
 */
export type MappingTransform =
  | "none"
  | "trim"
  | "upper"
  | "lower"
  | "date_iso"
  | "time_iso"
  | "hash_pii"
  | "cast_number"
  | "cast_integer"
  | "cast_boolean"
  | "parse_json"
  | "binary"
  | "phone"
  | "email"
  | "currency"
  | "percentage"
  | "strip_controls"
  | "identity_specialty";

export const MAPPING_TRANSFORMS: { id: MappingTransform; label: string; detail: string }[] = [
  { id: "none", label: "None", detail: "Pass through as detected" },
  { id: "trim", label: "Trim", detail: "Strip leading/trailing whitespace" },
  { id: "strip_controls", label: "Strip controls", detail: "Remove zero-width / null / format-control chars (warehouse-safe)" },
  { id: "upper", label: "Uppercase", detail: "Normalize to UPPER CASE" },
  { id: "lower", label: "Lowercase", detail: "Normalize to lower case" },
  { id: "date_iso", label: "Date → ISO", detail: "Parse dates/timestamps to ISO-8601" },
  { id: "time_iso", label: "Time → ISO", detail: "Parse time-of-day values" },
  { id: "hash_pii", label: "Hash PII", detail: "One-way hash for sensitive fields" },
  { id: "cast_integer", label: "Cast integer", detail: "Coerce to whole number (no fractional scale)" },
  { id: "cast_number", label: "Cast decimal", detail: "Coerce to precise numeric / DECIMAL" },
  { id: "cast_boolean", label: "Cast boolean", detail: "Coerce to true/false" },
  { id: "parse_json", label: "Parse JSON", detail: "Normalize JSON / ARRAY / STRUCT payloads" },
  { id: "binary", label: "Binary / base64", detail: "Preserve bytes as base64-safe wire form" },
  { id: "phone", label: "Normalize phone", detail: "Normalize phone numbers for text destinations" },
  { id: "email", label: "Normalize email", detail: "Normalize email addresses" },
  { id: "currency", label: "Parse currency", detail: "Strip currency symbols → decimal" },
  { id: "percentage", label: "Parse percentage", detail: "Parse percent strings → decimal" },
  {
    id: "identity_specialty",
    label: "Identity (specialty)",
    detail: "VECTOR / INTERVAL / GEOGRAPHY travel as identity — no invented cast or dimension",
  },
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
  /**
   * Exact engine transform id from the mapping pipeline (phone, integer, …).
   * Preserved across Map edits unless the operator changes the UI transform.
   */
  engineTransform?: string;
  requiresReview?: boolean;
  scoreGap?: number;
  /** From schema intelligence — e.g. string_enum, boolean_flag */
  semanticRole?: string;
  /** Intentionally ADD COLUMN / create-new (e.g. ObjectId → _id beside DECIMAL id). */
  createNew?: boolean;
  assignmentStrategy?: string;
  /**
   * STRUCT / JSON object handling — explicit Map choice.
   * Write path materializes flatten_top_level_keys via json_intelligence.
   */
  structPolicy?: StructPolicy;
  /** True when this row was synthesized from a parent flatten choice. */
  structDerived?: boolean;
  /** Parent source column when structDerived. */
  structParent?: string;
}

const STATUS_ENUM_TOKENS = new Set([
  "active", "inactive", "enabled", "disabled", "pending", "invalidated",
  "approved", "rejected", "completed", "cancelled", "canceled", "draft",
  "published", "archived", "deleted", "suspended", "processing", "queued",
]);

const STRICT_BOOL_TOKENS = new Set([
  "true", "false", "t", "f", "yes", "no", "y", "n", "0", "1", "on", "off",
]);

export type StructPolicy =
  | "store_as_json"
  | "flatten_top_level_keys"
  | "flatten_deep"
  | "explode_rows";

export const STRUCT_POLICIES: { id: StructPolicy; label: string; detail: string }[] = [
  {
    id: "store_as_json",
    label: "JSON blob",
    detail: "Keep STRUCT/JSON as one VARIANT/JSON/TEXT column — no key expansion",
  },
  {
    id: "flatten_top_level_keys",
    label: "Flatten keys",
    detail: "Promote top-level scalar/array keys to columns; nested objects stay on the parent blob",
  },
  {
    id: "flatten_deep",
    label: "Deep flatten",
    detail: "Promote nested keys up to depth 2 (capped); parent JSON blob is always kept",
  },
];

export const ARRAY_POLICIES: { id: StructPolicy; label: string; detail: string }[] = [
  {
    id: "store_as_json",
    label: "Serialize JSON",
    detail: "Keep ARRAY as one JSON/list column — no row explosion",
  },
  {
    id: "explode_rows",
    label: "Explode rows",
    detail: "Duplicate parent row per array element (capped at 256) — parent array kept",
  },
];

const FLATTEN_POLICIES = new Set<StructPolicy>(["flatten_top_level_keys", "flatten_deep"]);

const STRUCT_TYPE_RE = /\b(json|jsonb|struct|map|object|variant|document|record)\b/i;
const ARRAY_TYPE_RE = /\b(array|list|repeated)\b/i;
const SPECIALTY_TYPE_RE = /\b(vector|interval|geography|geometry|geopoint|geojson)\b/i;
/** Engine transforms with no first-class UI cast — surface as a pipeline chip. */
const PIPELINE_ONLY_ENGINE = new Set(["url", "iban", "postal", "uuid", "trim_id"]);


/** UI MappingTransform → engine transform id (aligned with transform_resolver.UI_TO_ENGINE). */
export const UI_TO_ENGINE_TRANSFORM: Record<MappingTransform, string> = {
  none: "none",
  trim: "trim",
  upper: "upper",
  lower: "lower",
  date_iso: "datetime",
  time_iso: "time",
  hash_pii: "hash_pii",
  cast_number: "decimal",
  cast_integer: "integer",
  cast_boolean: "boolean",
  parse_json: "json",
  binary: "binary",
  phone: "phone",
  email: "email",
  currency: "currency",
  percentage: "percentage",
  strip_controls: "strip_controls",
  identity_specialty: "none",
};

/** Engine transform → UI MappingTransform (aligned with transform_resolver.ENGINE_TO_UI + extensions). */
export const ENGINE_TO_UI_TRANSFORM: Record<string, MappingTransform> = {
  none: "none",
  identity: "none",
  trim: "trim",
  trim_id: "trim",
  strip_controls: "strip_controls",
  normalize_unicode: "strip_controls",
  uuid: "none",
  upper: "upper",
  lower: "lower",
  date: "date_iso",
  datetime: "date_iso",
  time: "time_iso",
  timestamp: "date_iso",
  decimal: "cast_number",
  integer: "cast_integer",
  boolean: "cast_boolean",
  hash_pii: "hash_pii",
  mask_pii: "hash_pii",
  json: "parse_json",
  binary: "binary",
  phone: "phone",
  email: "email",
  url: "none",
  iban: "none",
  postal: "none",
  currency: "currency",
  percentage: "percentage",
  base64: "binary",
};

function looksLikeStringEnumSample(sample?: string, semanticRole?: string): boolean {
  if (semanticRole === "string_enum") return true;
  if (!sample) return false;
  const token = sample.trim().toLowerCase();
  if (!token || STRICT_BOOL_TOKENS.has(token)) return false;
  return STATUS_ENUM_TOKENS.has(token) || /^[a-z][a-z0-9_\-]{1,31}$/i.test(token);
}

export function isSpecialtyLogicalType(type?: string): boolean {
  return Boolean(type && SPECIALTY_TYPE_RE.test(type));
}

/** True when the column is a STRUCT/JSON object candidate for Map policy. */
export function isStructLogicalType(type?: string): boolean {
  return Boolean(type && STRUCT_TYPE_RE.test(type) && !isArrayLogicalType(type));
}

/** True when the column is an ARRAY / list (serialize — no key flatten). */
export function isArrayLogicalType(type?: string): boolean {
  return Boolean(type && ARRAY_TYPE_RE.test(type));
}

/** Engine-only semantic transforms that Map shows as a chip when UI select is None. */
export function pipelineTransformChip(engine?: string): string | null {
  const e = (engine || "").trim().toLowerCase();
  if (!e || e === "none" || e === "identity") return null;
  if (PIPELINE_ONLY_ENGINE.has(e)) return e;
  // Visible UI mapping already covers this engine id.
  if (engineTransformToUi(e) !== "none") return null;
  return e;
}

/**
 * Infer a coarse logical type from one sample — used for STRUCT flatten children
 * so we do not invent VARCHAR for every promoted key.
 */
export function inferLogicalFromSample(sample?: string): string {
  if (!sample) return "VARCHAR";
  const s = sample.trim();
  if (!s) return "VARCHAR";
  if (/^(true|false)$/i.test(s)) return "BOOLEAN";
  if (/^[+-]?\d+$/.test(s)) {
    try {
      const n = BigInt(s);
      if (n > BigInt("9223372036854775807") || n < BigInt("-9223372036854775808")) return "VARCHAR";
    } catch {
      return "VARCHAR";
    }
    return "INTEGER";
  }
  if (/^[+-]?\d+\.\d+(?:[eE][+-]?\d+)?$/.test(s)) return "DECIMAL";
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return "DATE";
  if (/^\d{4}-\d{2}-\d{2}[T ]/.test(s)) {
    return /Z|[+-]\d{2}:?\d{2}$/i.test(s) ? "TIMESTAMPTZ" : "TIMESTAMP";
  }
  if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
    return s.startsWith("[") ? "ARRAY" : "JSON";
  }
  if (s.length > 255) return "TEXT";
  return "VARCHAR";
}

/** True when Approve-all must leave this row for operator review. */
export function mappingRequiresManualApproval(m: EditableMapping): boolean {
  if (isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m)) return true;
  if (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType)) return true;
  if (m.transform === "identity_specialty") return true;
  if (m.structPolicy === "flatten_top_level_keys" || m.structPolicy === "flatten_deep" || m.structPolicy === "explode_rows" || m.structDerived) return true;
  return false;
}

/**
 * Single honesty path for Approve / Approve-all (Map panel + Validate CTA).
 * Never auto-approves specialty identity, STRUCT flatten children, or existing DDL conflicts.
 */
export function approveMappingHonestly(m: EditableMapping): EditableMapping {
  if (isExistingEnumBooleanConflict(m)) {
    return flagExistingEnumBooleanConflict(m);
  }
  if (isExistingDestTypeOverride(m)) {
    return { ...m, approved: false, requiresReview: true };
  }
  if (isEnumToBooleanConflict(m) && canWidenMapping(m)) {
    return { ...widenMappingToVarchar(m), approved: true, requiresReview: false };
  }
  if (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType) || m.transform === "identity_specialty") {
    return {
      ...m,
      approved: false,
      requiresReview: true,
      transform: m.transform === "none" || !m.transform ? "identity_specialty" : m.transform,
    };
  }
  if (m.structPolicy === "flatten_top_level_keys" || m.structPolicy === "flatten_deep" || m.structPolicy === "explode_rows" || m.structDerived) {
    return { ...m, approved: false, requiresReview: true };
  }
  return { ...m, approved: true, requiresReview: false };
}

export function approveMappingsHonestly(mappings: EditableMapping[]): EditableMapping[] {
  return mappings.map(approveMappingHonestly);
}

export function countApproveEligible(mappings: EditableMapping[]): number {
  return mappings.filter((m) => !m.approved && !mappingRequiresManualApproval(m)).length;
}

/** Top-level promotable keys from a JSON object sample (mirrors backend). */
export function topLevelKeysFromSample(sample?: string, maxKeys = 32): string[] {
  if (!sample) return [];
  const trimmed = sample.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return [];
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
    const keys: string[] = [];
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      const name = String(key).trim();
      if (!name) continue;
      // Nested objects stay on the parent blob (max_depth=1).
      if (value !== null && typeof value === "object" && !Array.isArray(value)) continue;
      keys.push(name);
      if (keys.length >= maxKeys) break;
    }
    return keys;
  } catch {
    return [];
  }
}

/** Deep-promotable leaf paths (depth≤2) for flatten_deep — mirrors backend caps. */
export function deepKeysFromSample(sample?: string, maxKeys = 64): string[] {
  if (!sample) return [];
  const trimmed = sample.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return [];
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
    const keys: string[] = [];
    const walk = (obj: Record<string, unknown>, prefix: string, depth: number) => {
      for (const [key, value] of Object.entries(obj)) {
        const name = String(key).trim();
        if (!name) continue;
        const path = prefix ? `${prefix}_${name}` : name;
        if (value !== null && typeof value === "object" && !Array.isArray(value) && depth < 2) {
          walk(value as Record<string, unknown>, path, depth + 1);
        } else {
          keys.push(path);
        }
        if (keys.length >= maxKeys) return;
      }
    };
    walk(parsed as Record<string, unknown>, "", 1);
    return keys;
  } catch {
    return [];
  }
}

function childSampleFromParent(sample: string | undefined, key: string): string | undefined {
  if (!sample) return undefined;
  try {
    const parsed = JSON.parse(sample.trim()) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object") return undefined;
    // Support deep path keys like geo_lat from flatten_deep.
    if (key.includes("_") && !(key in parsed)) {
      const parts = key.split("_");
      let cur: unknown = parsed;
      for (const part of parts) {
        if (!cur || typeof cur !== "object" || Array.isArray(cur)) return undefined;
        cur = (cur as Record<string, unknown>)[part];
      }
      if (cur == null) return undefined;
      return typeof cur === "string" ? cur : JSON.stringify(cur);
    }
    const v = parsed[key];
    if (v == null) return undefined;
    return typeof v === "string" ? v : JSON.stringify(v);
  } catch {
    return undefined;
  }
}

/**
 * Apply STRUCT/ARRAY Map policy. Flatten synthesizes ``parent_key`` child mappings;
 * explode_rows synthesizes ``parent_elem``; store_as_json removes prior derived children.
 * Parent blob is always kept.
 */
export function applyStructPolicyChange(
  mappings: EditableMapping[],
  index: number,
  policy: StructPolicy,
): EditableMapping[] {
  const parent = mappings[index];
  if (!parent) return mappings;
  const withoutDerived = mappings.filter(
    (m, i) => i === index || !(m.structDerived && m.structParent === parent.source),
  );
  const parentIdx = withoutDerived.findIndex((m) => m.source === parent.source && !m.structDerived);
  if (parentIdx < 0) return mappings;

  const flattenish = FLATTEN_POLICIES.has(policy);
  const exploding = policy === "explode_rows";
  const nextParent: EditableMapping = {
    ...withoutDerived[parentIdx],
    structPolicy: policy,
    approved: false,
    requiresReview: flattenish || exploding ? true : withoutDerived[parentIdx].requiresReview,
    reason: exploding
      ? "ARRAY explode — one output row per element (capped); parent array kept"
      : policy === "flatten_deep"
        ? "STRUCT deep flatten — nested keys promoted (depth≤2); parent JSON kept"
        : policy === "flatten_top_level_keys"
          ? "STRUCT flatten — top-level keys promoted; nested objects stay on parent JSON"
          : isArrayLogicalType(parent.inferredType) || isArrayLogicalType(parent.destType)
            ? "ARRAY serialized as JSON/list"
            : "STRUCT stored as JSON/VARIANT blob",
    transform:
      withoutDerived[parentIdx].transform === "none" || !withoutDerived[parentIdx].transform
        ? "parse_json"
        : withoutDerived[parentIdx].transform,
    engineTransform:
      withoutDerived[parentIdx].transform === "none" || !withoutDerived[parentIdx].transform
        ? "json"
        : withoutDerived[parentIdx].engineTransform,
  };
  const next = [...withoutDerived];
  next[parentIdx] = nextParent;

  if (!flattenish && !exploding) {
    return next;
  }

  if (exploding) {
    const elemSource = `${parent.source}_elem`;
    if (!next.some((m) => m.source === elemSource)) {
      next.splice(parentIdx + 1, 0, {
        source: elemSource,
        target: normalizeMappingTarget(elemSource),
        confidence: Math.min(parent.confidence, 0.85),
        inferredType: "VARCHAR",
        destType: "VARCHAR",
        approved: false,
        requiresReview: true,
        reason: `Exploded element from ${parent.source}`,
        transform: "none",
        structDerived: true,
        structParent: parent.source,
      });
    }
    return next;
  }

  const keys =
    policy === "flatten_deep"
      ? deepKeysFromSample(parent.sample)
      : topLevelKeysFromSample(parent.sample);
  if (!keys.length) {
    next[parentIdx] = {
      ...nextParent,
      reason:
        policy === "flatten_deep"
          ? "STRUCT deep flatten requested — no promotable keys in sample"
          : "STRUCT flatten requested — no promotable top-level keys in sample (nested objects stay on blob)",
    };
    return next;
  }

  const existingSources = new Set(next.map((m) => m.source));
  const children: EditableMapping[] = [];
  for (const key of keys) {
    const source = `${parent.source}_${key}`;
    if (existingSources.has(source)) continue;
    const sample = childSampleFromParent(parent.sample, key);
    const childType = inferLogicalFromSample(sample);
    const specialty = isSpecialtyLogicalType(childType);
    const structish = isStructLogicalType(childType) || isArrayLogicalType(childType);
    children.push({
      source,
      target: normalizeMappingTarget(source),
      confidence: Math.min(parent.confidence, 0.85),
      inferredType: childType,
      destType: childType,
      sample,
      approved: false,
      requiresReview: true,
      reason: `Flattened from ${parent.source}.${key} (${childType})`,
      transform: specialty ? "identity_specialty" : structish ? "parse_json" : "none",
      engineTransform: structish ? "json" : undefined,
      structDerived: true,
      structParent: parent.source,
      structPolicy: isStructLogicalType(childType) ? "store_as_json" : undefined,
    });
    existingSources.add(source);
  }
  // Insert children immediately after parent for operator scan order.
  next.splice(parentIdx + 1, 0, ...children);
  return next;
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

/**
 * Widen is only honest for create-new columns. Existing DDL cannot be changed
 * from the Map step (Airbyte/Fivetran posture) — use Remap or ALTER.
 */
export function canWidenMapping(m: EditableMapping): boolean {
  return !m.existsInDestination;
}

/** Widen destination type to VARCHAR and clear numeric/boolean casts — new tables only. */
export function widenMappingToVarchar(m: EditableMapping): EditableMapping {
  if (!canWidenMapping(m)) {
    return flagExistingTypeConflict(m, "BOOLEAN/NUMBER");
  }
  const clearCast =
    m.transform === "cast_boolean"
    || m.transform === "cast_number"
    || m.transform === "cast_integer";
  return {
    ...m,
    destType: "VARCHAR",
    transform: clearCast ? "none" : m.transform,
    engineTransform: clearCast ? undefined : m.engineTransform,
    approved: false,
    requiresReview: false,
    reason: [m.reason, "Widened to VARCHAR (non-numeric / non-boolean samples)"].filter(Boolean).join(" · "),
  };
}

/**
 * True when enum→BOOLEAN conflict hits an **existing** destination column.
 * Mapping-only Widen cannot ALTER physical BOOLEAN → VARCHAR.
 */
export function isExistingEnumBooleanConflict(m: EditableMapping): boolean {
  return Boolean(m.existsInDestination && isEnumToBooleanConflict(m));
}

/** True when operator changed dest type on a column that already exists physically. */
export function isExistingDestTypeOverride(m: EditableMapping): boolean {
  if (!m.existsInDestination || !m.destType) return false;
  // Live dest type was stamped into destType at load; override flagged via reason tag.
  return /ALTER required|mapping Widen cannot change DDL|physical column/i.test(m.reason || "");
}

/** Flag for review without pretending the physical column type changed. */
export function flagExistingEnumBooleanConflict(m: EditableMapping): EditableMapping {
  return flagExistingTypeConflict(m, "BOOLEAN");
}

export function flagExistingTypeConflict(m: EditableMapping, destKind = "destination"): EditableMapping {
  return {
    ...m,
    approved: false,
    requiresReview: true,
    transform: m.transform === "cast_boolean" ? "none" : m.transform,
    engineTransform: m.transform === "cast_boolean" ? undefined : m.engineTransform,
    reason: [
      m.reason,
      `Existing ${destKind} column cannot be changed from Map — remap to a compatible column or ALTER the destination; mapping Widen alone will not change DDL`,
    ]
      .filter(Boolean)
      .join(" · "),
  };
}

/**
 * When the operator picks a new dest type on an existing column, keep the live
 * type in destType for preflight honesty and force review.
 */
export function applyDestTypeChange(m: EditableMapping, nextDestType: string): EditableMapping {
  if (m.existsInDestination && nextDestType && nextDestType !== m.destType) {
    return {
      ...flagExistingTypeConflict(m, m.destType || "destination"),
      // Keep physical type for G3/G6; note the desired type in reason.
      reason: [
        m.reason,
        `Desired type ${nextDestType} requires ALTER or remap (physical column stays ${m.destType || "as-is"})`,
      ]
        .filter(Boolean)
        .join(" · "),
      approved: false,
      requiresReview: true,
    };
  }
  return { ...m, destType: nextDestType, approved: false };
}

export function applyTransformChange(m: EditableMapping, next: MappingTransform): EditableMapping {
  return {
    ...m,
    transform: next,
    // Operator override — drop pipeline engineTransform so UI→engine is authoritative.
    engineTransform: next === "none" ? undefined : UI_TO_ENGINE_TRANSFORM[next],
    approved: false,
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
    const inferred = col.semantic_type || col.inferred_type || "string";
    const specialty = isSpecialtyLogicalType(inferred);
    const structish = isStructLogicalType(inferred);
    const arrayish = isArrayLogicalType(inferred);
    return {
      source: col.column_name,
      target,
      confidence: conf,
      inferredType: inferred,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: conf >= 0.9 && !col.is_pii && !specialty && !structish && !arrayish,
      isPii: col.is_pii,
      reason: specialty
        ? `${inferred} — identity payload (no invented cast/dim)`
        : structish
          ? "STRUCT/JSON — choose JSON blob, flatten keys, or deep flatten"
          : arrayish
            ? "ARRAY — serialize as JSON/list or explode rows (Map policy)"
            : (col.semantic_type || col.inferred_type || "Semantic match"),
      transform: col.is_pii
        ? "hash_pii"
        : specialty
          ? "identity_specialty"
          : structish || arrayish
            ? "parse_json"
            : "none",
      engineTransform: structish || arrayish ? "json" : undefined,
      requiresReview: specialty || structish || arrayish || undefined,
      structPolicy: structish || arrayish ? "store_as_json" : undefined,
    };
  });
}

export function uiTransformToEngine(t?: MappingTransform, engineTransform?: string): string | undefined {
  if (engineTransform && engineTransform !== "none" && engineTransform !== "identity") {
    // Prefer preserved pipeline transform when UI still shows the mapped control.
    const uiForEngine = engineTransformToUi(engineTransform);
    if (!t || t === uiForEngine || (t === "identity_specialty" && engineTransform === "none")) {
      return engineTransform === "identity" ? undefined : engineTransform;
    }
  }
  if (!t || t === "none" || t === "identity_specialty") return undefined;
  return UI_TO_ENGINE_TRANSFORM[t];
}

export function buildPreflightMappings(
  columns: ColumnAnalysis[],
  editable?: EditableMapping[],
) {
  if (editable?.length) {
    return editable.map((m) => {
      const enumBool = isEnumToBooleanConflict(m);
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
        transform: uiTransformToEngine(safe.transform, safe.engineTransform),
        target_type: m.existsInDestination
          ? (m.destType || safe.destType || safe.inferredType)
          : (safe.destType || safe.inferredType),
        source_type: safe.inferredType,
        requires_review: Boolean((safe.requiresReview || enumBool) && !safe.approved),
        score_gap: safe.scoreGap ?? 1,
        semantic_role: safe.semanticRole,
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
        struct_policy: safe.structPolicy,
        struct_derived: safe.structDerived || undefined,
        struct_parent: safe.structParent,
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
  return ENGINE_TO_UI_TRANSFORM[engine] ?? "none";
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
    struct_policy?: string;
    struct_derived?: boolean;
    struct_parent?: string;
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
    const sourceType = m.source_type;
    const destType = liveDestType || m.target_type || m.source_type;
    const specialty = isSpecialtyLogicalType(sourceType) || isSpecialtyLogicalType(destType);
    const structish = isStructLogicalType(sourceType) || isStructLogicalType(destType);
    const arrayish = isArrayLogicalType(sourceType) || isArrayLogicalType(destType);
    const engineTf = (m.transform || "").trim();
    let uiTf: MappingTransform = m.is_pii
      ? "hash_pii"
      : specialty && (!engineTf || engineTf === "none")
        ? "identity_specialty"
        : engineTransformToUi(engineTf);
    const structPolicy =
      m.struct_policy === "flatten_top_level_keys" ||
      m.struct_policy === "flatten_deep" ||
      m.struct_policy === "explode_rows" ||
      m.struct_policy === "store_as_json"
        ? m.struct_policy
        : structish
          ? "store_as_json"
          : arrayish
            ? "store_as_json"
            : undefined;
    const base: EditableMapping = {
      source: m.source,
      target: m.target,
      confidence: conf,
      inferredType: sourceType,
      destType,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: !requiresReview && !specialty && !structish && !arrayish && (conf >= threshold || identityMatch),
      isPii: m.is_pii,
      reason: specialty && !(m.reasoning || "").toLowerCase().includes("identity")
        ? [m.reasoning, `${sourceType || destType} — identity payload (dim/SRID not rewritten)`].filter(Boolean).join(" · ")
        : structish && !m.reasoning
          ? "STRUCT/JSON — choose JSON blob, flatten keys, or deep flatten"
          : arrayish && !m.reasoning
            ? "ARRAY — serialize as JSON/list or explode rows (Map policy)"
            : m.reasoning,
      existsInDestination: existsInDest,
      requiresReview: requiresReview || specialty || structish || arrayish,
      scoreGap: m.score_gap,
      transform: uiTf === "none" && (structish || arrayish) ? "parse_json" : uiTf,
      engineTransform: engineTf || (structish || arrayish ? "json" : undefined),
      semanticRole: m.semantic_role,
      createNew: Boolean(m.create_new) || m.assignment_strategy === "create_compatible_new",
      assignmentStrategy: m.assignment_strategy,
      structPolicy: structPolicy ?? (arrayish ? "store_as_json" : undefined),
      structDerived: Boolean(m.struct_derived),
      structParent: m.struct_parent,
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

export interface MappingHealthSummary {
  total: number;
  ready: number;
  needsReview: number;
  lowConfidence: number;
  unmappedTarget: number;
  specialtyIdentity: number;
  existingTypeConflict: number;
  weak: boolean;
  headline: string;
  detail: string;
}

/** Operator-facing Map health — surfaces empty/weak maps before Validate. */
export function mappingHealthSummary(
  mappings: EditableMapping[],
  threshold = 0.75,
): MappingHealthSummary {
  const total = mappings.length;
  const needsReview = mappings.filter((m) => m.requiresReview && !m.approved).length;
  const lowConfidence = mappings.filter((m) => m.confidence < threshold && !m.approved).length;
  const unmappedTarget = mappings.filter((m) => !String(m.target || "").trim()).length;
  const specialtyIdentity = mappings.filter(
    (m) => m.transform === "identity_specialty" || isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType),
  ).length;
  const existingTypeConflict = mappings.filter(
    (m) => isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m),
  ).length;
  const ready = mappings.filter(
    (m) => m.approved && String(m.target || "").trim() && !m.requiresReview,
  ).length;
  const weak =
    total === 0
    || unmappedTarget > 0
    || needsReview > 0
    || lowConfidence > 0
    || existingTypeConflict > 0;

  let headline = "Map looks ready";
  let detail = `${ready}/${total} mappings approved for Validate.`;
  if (total === 0) {
    headline = "No mappings yet";
    detail = "Run analysis or rematch before Validate — Execute will fail with an empty map.";
  } else if (unmappedTarget > 0) {
    headline = `${unmappedTarget} mapping(s) missing a destination column`;
    detail = "Every source field needs a target name (create-new or existing).";
  } else if (existingTypeConflict > 0) {
    headline = `${existingTypeConflict} existing-column type conflict(s)`;
    detail = "Remap to a compatible column or ALTER the destination — Map Widen cannot change DDL.";
  } else if (needsReview > 0 || lowConfidence > 0) {
    headline = `${needsReview + lowConfidence} mapping(s) need review`;
    detail = `Approve or fix low-confidence / specialty rows (threshold ${(threshold * 100).toFixed(0)}%).`;
  } else if (specialtyIdentity > 0) {
    headline = `${specialtyIdentity} specialty type(s) use identity`;
    detail = "VECTOR / INTERVAL / GEOGRAPHY travel as identity payloads — dimensions/SRID are not rewritten.";
  }

  return {
    total,
    ready,
    needsReview,
    lowConfidence,
    unmappedTarget,
    specialtyIdentity,
    existingTypeConflict,
    weak: weak || specialtyIdentity > 0,
    headline,
    detail,
  };
}
