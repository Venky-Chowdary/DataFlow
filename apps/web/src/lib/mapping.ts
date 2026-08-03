import type { ColumnAnalysis } from "./types";
import { declaredCarrierFidelityRisk } from "./typeCarrierFidelity";

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
  | "identity_specialty"
  | "omit";

export const MAPPING_TRANSFORMS: { id: MappingTransform; label: string; detail: string }[] = [
  { id: "none", label: "None", detail: "Pass through as detected" },
  { id: "omit", label: "Omit", detail: "Intentionally exclude this source column from the transfer" },
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
  /**
   * Engine fidelity verdict from `mapping_fidelity` — preserve | cast | mutate | lossy_cast.
   * Prefer this over client-side type heuristics when present.
   */
  fidelity?: "preserve" | "cast" | "mutate" | "lossy_cast" | string;
  /** One-line reason from the engine explaining the verdict. */
  fidelityReason?: string;
  /** True when the type path itself narrows (independent of transform). */
  typeNarrowing?: boolean;
  /**
   * Operator explicitly accepted precision/type loss for this row.
   * Required by G4 for lossy_cast / typeNarrowing — bare Approve is not enough.
   */
  riskAcknowledged?: boolean;
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
/** Aligned with engine specialty_carrier_base / INTERVAL / VECTOR / GEOGRAPHY. */
const SPECIALTY_TYPE_RE =
  /\b(vector|halfvec|sparsevec|interval|geography|geometry|geopoint|geojson|sdo_geometry|inet|cidr|macaddr8?|hstore|citext|objectid|xml|xmltype|tsvector|tsquery|pg_lsn|ltree|hierarchyid|rowversion|sql_variant|uniqueidentifier|ipv[46]|oid|jsonpath|regclass|name|user-defined|user_defined|anydata|hllsketch|money|smallmoney|long\s+raw)\b/i;
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
  omit: "omit",
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
  omit: "omit",
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

export function isLossyMapping(m: EditableMapping): boolean {
  return (m.fidelity || "").toLowerCase() === "lossy_cast" || Boolean(m.typeNarrowing);
}

/** Lossy, mutate, specialty, or STRUCT expand — G4 needs risk_acknowledged. */
export function mappingRequiresRiskAck(m: EditableMapping): boolean {
  if (isIntentionalOmit(m)) return false;
  const fidelity = (m.fidelity || "").toLowerCase();
  if (fidelity === "lossy_cast" || fidelity === "mutate" || m.typeNarrowing) return true;
  // Dest-type edits clear engine fidelity — recompute from carriers so Approve
  // cannot look green until Accept risk (Map→Validate SSOT).
  if (declaredCarrierFidelityRisk(m.inferredType, m.destType)) return true;
  if (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType)) return true;
  if (m.transform === "identity_specialty") return true;
  if (
    m.structPolicy === "flatten_top_level_keys"
    || m.structPolicy === "flatten_deep"
    || m.structPolicy === "explode_rows"
    || m.structDerived
  ) {
    return true;
  }
  return false;
}

/** True when Approve-all must leave this row for operator review. */
export function mappingRequiresManualApproval(m: EditableMapping): boolean {
  if (isIntentionalOmit(m)) return false;
  if (isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m)) return true;
  if (mappingRequiresRiskAck(m) && !m.riskAcknowledged) return true;
  return false;
}

/**
 * Explicit operator acceptance of fidelity / structural risk — unlocks G4.
 * Distinct from Approve: clients can prove intentional change was never silent.
 */
export function acknowledgeMappingRisk(m: EditableMapping): EditableMapping {
  if (!mappingRequiresRiskAck(m)) {
    return approveMappingHonestly(m);
  }
  const fidelity = (m.fidelity || "").toLowerCase();
  const ackNote =
    fidelity === "mutate"
      ? "Operator acknowledged value-mutating transform"
      : m.structDerived || m.structPolicy === "flatten_top_level_keys" || m.structPolicy === "flatten_deep" || m.structPolicy === "explode_rows"
        ? "Operator acknowledged STRUCT/ARRAY expand policy"
        : isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType) || m.transform === "identity_specialty"
          ? "Operator acknowledged specialty identity transport"
          : "Operator acknowledged type/precision loss risk";
  return {
    ...m,
    riskAcknowledged: true,
    approved: true,
    requiresReview: false,
    transform:
      (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType))
      && (!m.transform || m.transform === "none")
        ? "identity_specialty"
        : m.transform,
    reason: [m.reason, m.fidelityReason, ackNote].filter(Boolean).join(" · "),
  };
}

/**
 * Single honesty path for Approve / Approve-all (Map panel + Validate CTA).
 * Never auto-approves specialty identity, STRUCT flatten children, lossy casts,
 * mutate transforms, type narrowing, or existing DDL conflicts.
 * Risk rows require {@link acknowledgeMappingRisk}, not bare Approve.
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
  if (mappingRequiresRiskAck(m)) {
    if (m.riskAcknowledged) {
      return { ...m, approved: true, requiresReview: false };
    }
    return {
      ...m,
      approved: false,
      requiresReview: true,
      transform:
        (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType))
        && (!m.transform || m.transform === "none")
          ? "identity_specialty"
          : m.transform,
    };
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

/**
 * Deep-promotable leaf paths (depth≤2) for flatten_deep — mirrors backend caps.
 * Returns flattened column suffixes using `_` (backend SSOT). Collisions between
 * a literal key `geo_lat` and nested `geo.lat` are detected separately.
 */
export function deepKeysFromSample(sample?: string, maxKeys = 64): string[] {
  return deepKeyPathsFromSample(sample, maxKeys).map((p) => p.flat);
}

/** JSON path segments + flattened suffix for collision-safe Map synthesis. */
export function deepKeyPathsFromSample(
  sample?: string,
  maxKeys = 64,
): Array<{ flat: string; path: string[] }> {
  if (!sample) return [];
  const trimmed = sample.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return [];
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
    const keys: Array<{ flat: string; path: string[] }> = [];
    const walk = (obj: Record<string, unknown>, path: string[], depth: number) => {
      for (const [key, value] of Object.entries(obj)) {
        const name = String(key).trim();
        if (!name) continue;
        const nextPath = [...path, name];
        if (value !== null && typeof value === "object" && !Array.isArray(value) && depth < 2) {
          walk(value as Record<string, unknown>, nextPath, depth + 1);
        } else {
          keys.push({ flat: nextPath.join("_"), path: nextPath });
        }
        if (keys.length >= maxKeys) return;
      }
    };
    walk(parsed as Record<string, unknown>, [], 1);
    return keys;
  } catch {
    return [];
  }
}

function childSampleFromPath(sample: string | undefined, path: string[]): string | undefined {
  if (!sample || !path.length) return undefined;
  try {
    let cur: unknown = JSON.parse(sample.trim());
    for (const part of path) {
      if (!cur || typeof cur !== "object" || Array.isArray(cur)) return undefined;
      cur = (cur as Record<string, unknown>)[part];
    }
    if (cur == null) return undefined;
    return typeof cur === "string" ? cur : JSON.stringify(cur);
  } catch {
    return undefined;
  }
}

function childSampleFromParent(sample: string | undefined, key: string): string | undefined {
  if (!sample) return undefined;
  try {
    const parsed = JSON.parse(sample.trim()) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object") return undefined;
    if (key in parsed) {
      const v = parsed[key];
      if (v == null) return undefined;
      return typeof v === "string" ? v : JSON.stringify(v);
    }
    // Deep path keys like geo_lat — prefer exact segment walk when unambiguous.
    if (key.includes("_")) {
      return childSampleFromPath(sample, key.split("_"));
    }
    return undefined;
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

  const keyPaths =
    policy === "flatten_deep"
      ? deepKeyPathsFromSample(parent.sample)
      : topLevelKeysFromSample(parent.sample).map((k) => ({ flat: k, path: [k] }));
  if (!keyPaths.length) {
    next[parentIdx] = {
      ...nextParent,
      reason:
        policy === "flatten_deep"
          ? "STRUCT deep flatten requested — no promotable keys in sample"
          : "STRUCT flatten requested — no promotable top-level keys in sample (nested objects stay on blob)",
    };
    return next;
  }

  // Detect underscore-path collisions (literal geo_lat vs nested geo.lat → geo_lat).
  const flatOwners = new Map<string, string[][]>();
  for (const kp of keyPaths) {
    const owners = flatOwners.get(kp.flat) || [];
    owners.push(kp.path);
    flatOwners.set(kp.flat, owners);
  }
  const collisionFlats = new Set(
    [...flatOwners.entries()].filter(([, owners]) => owners.length > 1).map(([flat]) => flat),
  );
  if (collisionFlats.size) {
    next[parentIdx] = {
      ...nextParent,
      approved: false,
      requiresReview: true,
      reason: [
        nextParent.reason,
        `Flatten path collision on ${[...collisionFlats].join(", ")} — keep as JSON or rename source keys`,
      ]
        .filter(Boolean)
        .join(" · "),
    };
  }

  const existingSources = new Set(next.map((m) => m.source));
  const children: EditableMapping[] = [];
  for (const kp of keyPaths) {
    if (collisionFlats.has(kp.flat)) continue; // fail-closed: do not invent ambiguous columns
    const source = `${parent.source}_${kp.flat}`;
    if (existingSources.has(source)) {
      next[parentIdx] = {
        ...next[parentIdx],
        approved: false,
        requiresReview: true,
        reason: [
          next[parentIdx].reason,
          `Flatten target ${source} already exists — choose store-as-JSON or remap`,
        ]
          .filter(Boolean)
          .join(" · "),
      };
      continue;
    }
    const sample = childSampleFromPath(parent.sample, kp.path);
    const childType = inferLogicalFromSample(sample);
    const specialty = isSpecialtyLogicalType(childType);
    const structish = isStructLogicalType(childType) || isArrayLogicalType(childType);
    const dotted = kp.path.join(".");
    children.push({
      source,
      target: normalizeMappingTarget(source),
      confidence: Math.min(parent.confidence, 0.85),
      inferredType: childType,
      destType: childType,
      sample,
      approved: false,
      requiresReview: true,
      reason: `Flattened from ${parent.source}.${dotted} (${childType})`,
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
  const narrowing = declaredCarrierFidelityRisk(m.inferredType, nextDestType);
  return {
    ...m,
    destType: nextDestType,
    approved: false,
    riskAcknowledged: false,
    // Stale engine fidelity must not greenwash the new dest type until re-stamp.
    fidelity: narrowing ? "lossy_cast" : undefined,
    fidelityReason: narrowing
      ? `Destination type ${nextDestType} may collapse fidelity from ${m.inferredType || "source"}`
      : undefined,
    typeNarrowing: narrowing || undefined,
    requiresReview: narrowing || m.requiresReview,
  };
}

export function isIntentionalOmit(m: EditableMapping): boolean {
  return m.transform === "omit" || m.engineTransform === "omit" || Boolean((m as { intentionalOmit?: boolean }).intentionalOmit);
}

export function applyTransformChange(m: EditableMapping, next: MappingTransform): EditableMapping {
  if (next === "omit") {
    return {
      ...m,
      transform: "omit",
      engineTransform: "omit",
      target: "",
      approved: true,
      requiresReview: false,
      riskAcknowledged: false,
      reason: "Intentionally omitted from transfer",
    };
  }
  const restoring = m.transform === "omit";
  return {
    ...m,
    transform: next,
    // Operator override — drop pipeline engineTransform so UI→engine is authoritative.
    engineTransform: next === "none" ? undefined : UI_TO_ENGINE_TRANSFORM[next],
    target: restoring && !String(m.target || "").trim() ? m.source : m.target,
    reason: restoring && m.reason === "Intentionally omitted from transfer" ? undefined : m.reason,
    approved: false,
    riskAcknowledged: false,
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
  pendingDestSchema = false,
): number {
  // Pending / unknown destination — never inflate toward create-new certainty.
  if (pendingDestSchema) return Math.min(confidence, 0.55);
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
  destColumns?: string[],
): EditableMapping[] {
  const destKnown = destColumns !== undefined;
  const destSet = destKnown
    ? new Set(destColumns.map((c) => c.toLowerCase()))
    : null;
  const pendingDest = destKnown && destSet!.size === 0;

  return columns.map((col) => {
    const target = normalizeMappingTarget(col.column_name, col);
    const sampleVal = sampleRows?.find((r) => r[col.column_name] != null)?.[col.column_name];
    const existsInDestination = destSet ? destSet.has(target.toLowerCase()) : undefined;
    // Empty dest list → schema pending (not invent create-new). Missing target on a
    // known dest → create-compatible new. Unknown dest → cap like create-new (never 95%).
    const createNew = Boolean(destSet && !pendingDest && existsInDestination === false);
    const conf = boostIdentityConfidence(
      col.column_name,
      target,
      col.confidence,
      createNew || !destKnown,
      pendingDest,
    );
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
      // Never invent Approve from confidence — pipeline fidelity must stamp first.
      approved: false,
      isPii: col.is_pii,
      reason: specialty
        ? `${inferred} — identity payload (no invented cast/dim)`
        : structish
          ? "STRUCT/JSON — choose JSON blob, flatten keys, or deep flatten"
          : arrayish
            ? "ARRAY — serialize as JSON/list or explode rows (Map policy)"
            : pendingDest
              ? "Identity match — destination schema not loaded yet"
              : createNew
                ? "Identity match — will create destination column"
                : (col.semantic_type || col.inferred_type || "Semantic match"),
      transform: col.is_pii
        ? "hash_pii"
        : specialty
          ? "identity_specialty"
          : structish || arrayish
            ? "parse_json"
            : "none",
      engineTransform: structish || arrayish ? "json" : undefined,
      requiresReview: specialty || structish || arrayish || pendingDest || conf < 0.9 || undefined,
      structPolicy: structish || arrayish ? "store_as_json" : undefined,
      existsInDestination,
      createNew: createNew || undefined,
      assignmentStrategy: pendingDest
        ? "pending_dest_schema"
        : createNew
          ? "create_compatible_new"
          : undefined,
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
      const pendingDest = safe.assignmentStrategy === "pending_dest_schema";
      const omitted = isIntentionalOmit(safe);
      // Missing dest column (when schema is known) is create-new even if createNew flag lagged.
      const isCreateNew = !omitted && Boolean(
        safe.createNew
        || safe.assignmentStrategy === "create_compatible_new"
        || (safe.existsInDestination === false && !pendingDest),
      );
      return {
        source: safe.source,
        target: omitted ? "" : safe.target,
        confidence: omitted
          ? 1
          : (
          isCreateNew
            ? Math.min(safe.confidence, 0.93)
            : pendingDest
              ? Math.min(safe.confidence, 0.55)
              : safe.confidence
        ),
        reason: safe.reason || "User reviewed",
        // user_override only after explicit Approve / Accept risk — never confidence bootstrap.
        user_override: Boolean(safe.riskAcknowledged) || (safe.approved && !enumBool && !mappingRequiresRiskAck(safe)),
        transform: omitted ? "omit" : uiTransformToEngine(safe.transform, safe.engineTransform),
        intentional_omit: omitted || undefined,
        target_type: omitted
          ? undefined
          : m.existsInDestination
          ? (m.destType || safe.destType || safe.inferredType)
          : (safe.destType || safe.inferredType),
        source_type: safe.inferredType,
        requires_review: omitted ? false : Boolean((safe.requiresReview || enumBool) && !safe.approved),
        score_gap: safe.scoreGap ?? 1,
        semantic_role: safe.semanticRole,
        create_new: isCreateNew,
        assignment_strategy:
          omitted
            ? "intentional_omit"
            : safe.assignmentStrategy
          || (isCreateNew ? "create_compatible_new" : undefined),
        struct_policy: omitted ? undefined : safe.structPolicy,
        struct_derived: safe.structDerived || undefined,
        struct_parent: safe.structParent,
        fidelity: omitted ? undefined : safe.fidelity,
        type_narrowing: omitted ? undefined : Boolean(safe.typeNarrowing) || undefined,
        risk_acknowledged: omitted ? undefined : Boolean(safe.riskAcknowledged) || undefined,
      };
    });
  }
  // No editable map — treat identity as create-compatible new and cap confidence.
  // Never emit uncapped ≥95% identity as if destination schema was proven.
  return columns.map((col) => {
    const target = normalizeMappingTarget(col.column_name, col);
    return {
      source: col.column_name,
      target,
      confidence: boostIdentityConfidence(col.column_name, target, col.confidence, true),
      reason: col.semantic_type || col.inferred_type || "Semantic match",
      user_override: false,
      requires_review: true,
      create_new: true,
      assignment_strategy: "create_compatible_new",
      source_type: col.inferred_type || col.semantic_type,
      target_type: col.inferred_type || col.semantic_type,
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
    fidelity?: string;
    fidelity_reason?: string;
    type_narrowing?: boolean;
  }>,
  sampleRows?: Record<string, unknown>[],
  destColumns?: string[],
  threshold = 0.75,
  destSchema?: Record<string, string>,
): EditableMapping[] {
  const destSet = new Set((destColumns ?? []).map((c) => c.toLowerCase()));
  // Never invent plan-level create-new from empty dest columns alone.
  // Only honor explicit pipeline strategies / create_new flags.
  const destTypeByLower = new Map(
    Object.entries(destSchema || {}).map(([k, v]) => [k.toLowerCase(), v]),
  );
  return mappings.map((m) => {
    const sampleVal = sampleRows?.find((r) => r[m.source] != null)?.[m.source];
    const existsInDest = destSet.has(m.target.toLowerCase());
    const liveDestType = destTypeByLower.get(m.target.toLowerCase());
    const pendingDest = m.assignment_strategy === "pending_dest_schema";
    const rowCreateNew =
      !pendingDest
      && (Boolean(m.create_new)
        || m.assignment_strategy === "create_compatible_new"
        || m.assignment_strategy === "identity_passthrough");
    const conf = boostIdentityConfidence(
      m.source,
      m.target,
      m.confidence,
      rowCreateNew,
      pendingDest,
    );
    const requiresReview = Boolean(m.requires_review) || pendingDest;
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
    const engineFidelity = (m.fidelity || "").trim().toLowerCase();
    const lossyFidelity =
      engineFidelity === "lossy_cast"
      || engineFidelity === "mutate"
      || Boolean(m.type_narrowing);
    // Fail-closed: exact-name identity must still clear confidence + fidelity.
    // Never auto-approve lossy/mutate/narrowing — clients stress-test type remaps.
    const autoApproved =
      !requiresReview
      && !specialty
      && !structish
      && !arrayish
      && !pendingDest
      && !lossyFidelity
      && !m.struct_derived
      && conf >= threshold;
    const base: EditableMapping = {
      source: m.source,
      target: m.target,
      confidence: conf,
      inferredType: sourceType,
      destType,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: autoApproved,
      isPii: m.is_pii,
      reason: specialty && !(m.reasoning || "").toLowerCase().includes("identity")
        ? [m.reasoning, `${sourceType || destType} — identity payload (dim/SRID not rewritten)`].filter(Boolean).join(" · ")
        : structish && !m.reasoning
          ? "STRUCT/JSON — choose JSON blob, flatten keys, or deep flatten"
          : arrayish && !m.reasoning
            ? "ARRAY — serialize as JSON/list or explode rows (Map policy)"
            : m.reasoning,
      existsInDestination: existsInDest,
      requiresReview: requiresReview || specialty || structish || arrayish || lossyFidelity || conf < threshold,
      scoreGap: m.score_gap,
      transform: uiTf === "none" && (structish || arrayish) ? "parse_json" : uiTf,
      engineTransform: engineTf || (structish || arrayish ? "json" : undefined),
      semanticRole: m.semantic_role,
      createNew: rowCreateNew,
      assignmentStrategy: m.assignment_strategy,
      structPolicy: structPolicy ?? (arrayish ? "store_as_json" : undefined),
      fidelity: engineFidelity || undefined,
      fidelityReason: m.fidelity_reason || undefined,
      typeNarrowing: Boolean(m.type_narrowing),
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
  intentionalOmit: number;
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
  const intentionalOmit = mappings.filter((m) => isIntentionalOmit(m)).length;
  const active = mappings.filter((m) => !isIntentionalOmit(m));
  const needsReview = active.filter((m) => m.requiresReview && !m.approved).length;
  const lowConfidence = active.filter((m) => m.confidence < threshold && !m.approved).length;
  const unmappedTarget = active.filter((m) => !String(m.target || "").trim()).length;
  const specialtyIdentity = active.filter(
    (m) => m.transform === "identity_specialty" || isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType),
  ).length;
  const existingTypeConflict = active.filter(
    (m) => isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m),
  ).length;
  const ready = mappings.filter(
    (m) =>
      (isIntentionalOmit(m) && m.approved)
      || (m.approved && String(m.target || "").trim() && !m.requiresReview),
  ).length;
  const weak =
    total === 0
    || unmappedTarget > 0
    || needsReview > 0
    || lowConfidence > 0
    || existingTypeConflict > 0;

  let headline = "Map looks ready";
  let detail = `${ready}/${total} mappings approved for Validate.`;
  if (intentionalOmit > 0 && !weak) {
    detail = `${ready}/${total} ready · ${intentionalOmit} intentionally omitted.`;
  }
  if (total === 0) {
    headline = "No mappings yet";
    detail = "Run analysis or rematch before Validate — Execute will fail with an empty map.";
  } else if (unmappedTarget > 0) {
    headline = `${unmappedTarget} mapping(s) missing a destination column`;
    detail = "Set a target, or choose Transform → Omit for intentional exclusions.";
  } else if (existingTypeConflict > 0) {
    headline = `${existingTypeConflict} existing-column type conflict(s)`;
    detail = "Remap to a compatible column or ALTER the destination — Map Widen cannot change DDL.";
  } else if (needsReview > 0 || lowConfidence > 0) {
    // One row can be both requiresReview and low-confidence — count unique mappings.
    const reviewCount = active.filter(
      (m) => !m.approved && (m.requiresReview || m.confidence < threshold),
    ).length;
    headline = `${reviewCount} mapping(s) need review`;
    detail = `Accept risk on lossy/specialty rows, or Approve eligible rows only (threshold ${(threshold * 100).toFixed(0)}%).`;
  } else if (specialtyIdentity > 0) {
    headline = `${specialtyIdentity} specialty type(s) use identity`;
    detail = "VECTOR / INTERVAL / GEOGRAPHY travel as identity payloads — Accept risk required before Validate clears G4.";
  } else if (intentionalOmit > 0) {
    headline = `${intentionalOmit} column(s) intentionally omitted`;
    detail = `${ready - intentionalOmit} columns transfer · ${intentionalOmit} excluded by Map policy.`;
  }

  return {
    total,
    ready,
    needsReview,
    lowConfidence,
    unmappedTarget,
    intentionalOmit,
    specialtyIdentity,
    existingTypeConflict,
    weak: weak || specialtyIdentity > 0,
    headline,
    detail,
  };
}
