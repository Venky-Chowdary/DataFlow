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
  { id: "cast_integer", label: "Parse integer", detail: "Integer write guard — unparseable cells quarantine (not precision invent)" },
  { id: "cast_number", label: "Parse decimal", detail: "Numeric write guard — unparseable cells quarantine (not precision invent)" },
  { id: "cast_boolean", label: "Parse boolean", detail: "Boolean write guard — unparseable cells quarantine" },
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

export interface CreateNewTypeRisk {
  kind: string;
  severity?: "info" | "warn" | "block" | string;
  message: string;
}

/** Engine-stamped Map review kinds — keep aligned with ``semantic_mapper``. */
export type MappingReviewKind =
  | "measure_kind"
  | "entity_identity"
  | "dest_collision"
  | "identity_leaf"
  | "temporal_polarity"
  | "lossy"
  | "create_new"
  | "generic";

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
  /** Sample-aware array class from Map pipeline (array_of_object, …). */
  structuralClass?: string;
  /** Ranked strategy recommendations from the engine. */
  arrayStrategies?: ArrayStrategyOption[];
  /** Operator-approved child table contract for normalize/hybrid. */
  childTableSpec?: ChildTableSpec;
  /** Proposed child spec while policy is still JSON (one-click apply). */
  proposedChildTableSpec?: ChildTableSpec;
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
   * Create-new precision / width / TZ risks stamped by the mapping pipeline
   * (`assess_create_new_type_risk`). Visible before Validate/write — never invent
   * green when the engine stamped a risk.
   */
  createNewRisks?: CreateNewTypeRisk[];
  /**
   * Operator explicitly accepted precision/type loss for this row.
   * Required by G4 for lossy_cast / typeNarrowing — bare Approve is not enough.
   * Alone this is NOT an execution contract — see riskContract.
   */
  riskAcknowledged?: boolean;
  /**
   * Signed Migration Risk Contract (or draft fields signed on Validate).
   * Execute-approve requires a continue-policy contract — boolean ack is incomplete.
   */
  riskContract?: MigrationRiskContractDraft;
  /** Underscore-path collisions from STRUCT flatten — fail-closed, operator-visible. */
  flattenCollisions?: { flat: string; paths: string[][] }[];
  /**
   * Sample-aware column profile from Map pipeline (null%, min/max, observed p/s).
   * Engine-stamped — never invent client-side invent from sample string alone.
   */
  columnProfile?: ColumnProfile;
  /**
   * Engine Map review kind from ``semantic_mapper.classify_review_kind``.
   * False-friends stay off Approve eligible until Confirm this pair / Remap.
   */
  reviewKind?: MappingReviewKind;
  /** Operator explicitly confirmed a false-friend pair (not Approve eligible). */
  falseFriendConfirmed?: boolean;
}

/** Compact profiler strip from ``mapping_quality.column_profile_for_map``. */
export interface ColumnProfile {
  null_rate?: number;
  unique_ratio?: number;
  min?: number;
  max?: number;
  observed_precision?: number;
  observed_scale?: number;
  numeric_kind?: string;
  suggested_carrier?: string;
  ieee_signals?: string[];
  likely_identifier?: boolean;
  likely_email?: boolean;
  likely_uuid?: boolean;
  likely_date?: boolean;
  likely_boolean?: boolean;
  semantic_pattern_score?: number;
}

/** Operator-facing one-line Map strip from engine column_profile. */
export function formatColumnProfileStrip(profile?: ColumnProfile | null): string | null {
  if (!profile || typeof profile !== "object") return null;
  const parts: string[] = [];
  if (typeof profile.null_rate === "number" && Number.isFinite(profile.null_rate)) {
    parts.push(`null ${(profile.null_rate * 100).toFixed(0)}%`);
  }
  if (
    typeof profile.min === "number"
    && typeof profile.max === "number"
    && Number.isFinite(profile.min)
    && Number.isFinite(profile.max)
  ) {
    const fmt = (n: number) =>
      Math.abs(n) >= 1000 ? n.toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(n);
    parts.push(`${fmt(profile.min)}–${fmt(profile.max)}`);
  }
  if (
    typeof profile.observed_precision === "number"
    && typeof profile.observed_scale === "number"
  ) {
    parts.push(`p${profile.observed_precision},s${profile.observed_scale}`);
  } else if (profile.suggested_carrier) {
    parts.push(String(profile.suggested_carrier));
  }
  if (profile.numeric_kind === "ieee_float") {
    parts.push("IEEE float");
  } else if (profile.numeric_kind === "fixed_decimal") {
    parts.push("fixed");
  } else if (profile.numeric_kind === "integer") {
    parts.push("int");
  }
  return parts.length ? parts.join(" · ") : null;
}

/** Draft / signed Migration Risk Contract — mirrors apps/api migration_risk_contract. */
export type ExecutionPolicy =
  | "FAIL_JOB"
  | "STOP_TABLE"
  | "STOP_COLUMN"
  | "QUARANTINE_ROW"
  | "SKIP_ROW"
  | "RETRY"
  | "CAST_AND_CONTINUE"
  | "TRANSFORM_AND_CONTINUE"
  | "ABORT_TRANSACTION";

/** Policies that unlock Validate→Execute (must match BE CONTINUE_POLICIES). */
export const CONTINUE_EXECUTION_POLICIES = new Set<ExecutionPolicy>([
  "QUARANTINE_ROW",
  "SKIP_ROW",
  "CAST_AND_CONTINUE",
  "TRANSFORM_AND_CONTINUE",
  "STOP_COLUMN",
]);

/** Operator-selectable policies — labels match runtime semantics (no placeholders). */
export const EXECUTION_POLICY_OPTIONS: Array<{
  id: ExecutionPolicy;
  label: string;
  continueUnlock: boolean;
}> = [
  { id: "FAIL_JOB", label: "FAIL_JOB — abort the job write", continueUnlock: false },
  { id: "STOP_TABLE", label: "STOP_TABLE — abort this table/stream write", continueUnlock: false },
  { id: "ABORT_TRANSACTION", label: "ABORT_TRANSACTION — abort txn (or fail if none)", continueUnlock: false },
  { id: "RETRY", label: "RETRY — one re-attempt then abort", continueUnlock: false },
  { id: "STOP_COLUMN", label: "STOP_COLUMN — omit bad column; write other columns", continueUnlock: true },
  { id: "QUARANTINE_ROW", label: "QUARANTINE_ROW — holdout row to DLQ for replay", continueUnlock: true },
  {
    id: "SKIP_ROW",
    label: "SKIP_ROW — drop from primary (audit only; not DLQ / not replayable)",
    continueUnlock: true,
  },
  {
    id: "CAST_AND_CONTINUE",
    label: "CAST_FAIL_QUARANTINE — cast fail → holdout (value not written)",
    continueUnlock: true,
  },
  { id: "TRANSFORM_AND_CONTINUE", label: "TRANSFORM_AND_CONTINUE — transform fail → quarantine", continueUnlock: true },
];

export type MigrationRiskContractDraft = {
  risk_id?: string;
  severity?: string;
  root_cause?: string;
  column: string;
  source_type: string;
  destination_type: string;
  transform?: string | null;
  rows_sampled?: number;
  estimated_rows?: number | null;
  expected_failure_pct?: number | null;
  expected_precision_loss?: boolean;
  expected_truncation?: boolean;
  expected_nulls?: boolean;
  execution_policy: ExecutionPolicy;
  quarantine_policy?: string;
  retry_policy?: string;
  rollback_strategy?: string;
  approved_by: string;
  approved_at?: string;
  reason: string;
  signature?: string;
  proof_pack_ref?: string | null;
  mapping_hash?: string;
  plan_id?: string | null;
  migration_id?: string;
  table?: string;
  loss_classification?: string;
  target?: string;
  version?: number;
};

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
  | "explode_rows"
  | "normalize_child_table"
  | "hybrid_json_and_child";

export type ChildTableSpec = {
  child_table: string;
  parent_key_columns: string[];
  ordinal_column?: string;
  columns: Array<{ name: string; type: string }>;
  keep_parent_json?: boolean;
};

export type ArrayStrategyOption = {
  id: StructPolicy;
  label: string;
  fidelity?: string;
  recommended?: boolean;
  detail?: string;
  child_table_spec?: ChildTableSpec;
};

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
    label: "JSON column",
    detail: "Keep ARRAY as one JSON/list column — lossless document wire (default)",
  },
  {
    id: "hybrid_json_and_child",
    label: "Hybrid (JSON + child)",
    detail: "Parent JSON for fidelity + normalized child table for SQL analytics",
  },
  {
    id: "normalize_child_table",
    label: "Normalize child table",
    detail: "Relational child table (parent JSON kept fail-closed unless cleared)",
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
  /\b(vector|halfvec|sparsevec|interval|geography|geometry|geopoint|geojson|sdo_geometry|st_geometry|inet|cidr|macaddr8?|hstore|citext|objectid|xml|xmltype|tsvector|tsquery|pg_lsn|ltree|hierarchyid|rowversion|sql_variant|uniqueidentifier|ipv[46]|oid|jsonpath|regclass|name|user-defined|user_defined|anydata|hllsketch|money|smallmoney|long\s+raw|decfloat|smalldatetime|enum8|enum16|nothing|dynamic|aggregatefunction|simpleaggregatefunction)\b/i;
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

/** True when the pipeline stamped create-new precision/width/TZ risk. */
export function hasCreateNewTypeRisk(m: EditableMapping): boolean {
  return Boolean(m.createNewRisks && m.createNewRisks.length > 0);
}

/** Short chip label for the highest-severity create-new risk. */
export function createNewRiskChipLabel(m: EditableMapping): string | null {
  if (!hasCreateNewTypeRisk(m)) return null;
  const kinds = new Set((m.createNewRisks || []).map((r) => (r.kind || "").toLowerCase()));
  if (kinds.has("timezone_polarity")) return "TZ risk";
  if (kinds.has("varchar_width_cap") || kinds.has("varchar_narrow")) return "width risk";
  if (kinds.has("precision_collapse")) return "precision";
  if (kinds.has("uuid_domain")) return "UUID domain";
  if (kinds.has("objectid_domain")) return "ObjectId domain";
  if (kinds.has("lossy_coercion")) return "create-new risk";
  return "create-new risk";
}

export function createNewRiskDetail(m: EditableMapping): string {
  return (m.createNewRisks || []).map((r) => r.message).filter(Boolean).join(" · ");
}

/**
 * Prefer engine-stamped fidelity / create-new risks over client heuristics.
 * Map, intelligence panel, and PairList must show the same chip.
 */
export function engineStampedRiskChip(m: EditableMapping): {
  label: string;
  detail: string;
  severity: "block" | "warn";
} | null {
  // Safe normalize (email/trim/case) is Approve-tier — do not scare with mutate/cast chips.
  if (isSafeNormalizeMapping(m)) return null;
  const fidelity = (m.fidelity || "").toLowerCase();
  if (fidelity === "lossy_cast" || fidelity === "mutate" || fidelity === "cast") {
    return {
      label: fidelity === "lossy_cast" ? "lossy" : fidelity === "mutate" ? "mutate" : "cast",
      detail: m.fidelityReason || `${m.inferredType || "?"} → ${m.destType || "?"}`,
      severity: fidelity === "lossy_cast" ? "block" : "warn",
    };
  }
  if (hasCreateNewTypeRisk(m)) {
    const hasBlock = (m.createNewRisks || []).some(
      (r) => (r.severity || "").toLowerCase() === "block",
    );
    return {
      label: createNewRiskChipLabel(m) || "create-new risk",
      detail: createNewRiskDetail(m) || m.fidelityReason || "Create-new type risk",
      severity: hasBlock ? "block" : "warn",
    };
  }
  return null;
}

/** Safe normalizations — trim/case/email/phone — not precision or semantic risk.
 * Include engine ``trim_id`` (pipeline id preserved on Validate) alongside UI ``trim``. */
const SAFE_NORMALIZE_TRANSFORMS = new Set<string>([
  "trim",
  "trim_id",
  "lower",
  "upper",
  "email",
  "phone",
  "strip_controls",
]);

/**
 * Operator-facing ack tier for Map status actions.
 * - approve: harmless / preserve (or safe normalize) — no "risk" language
 * - review: reversible cast / ISO / widen-to-text — needs eyes, not scare wording
 * - accept_risk: lossy / irreversible / STRUCT expand / specialty transport
 */
export type MappingAckTier = "approve" | "review" | "accept_risk";

export function isSafeNormalizeMapping(m: EditableMapping): boolean {
  if (isIntentionalOmit(m)) return false;
  if (m.typeNarrowing || (m.fidelity || "").toLowerCase() === "lossy_cast") return false;
  if (hasCreateNewTypeRisk(m)) return false;
  if (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType)) return false;
  if (m.transform === "identity_specialty") return false;
  if (
    m.structPolicy === "flatten_top_level_keys"
    || m.structPolicy === "flatten_deep"
    || m.structPolicy === "explode_rows"
    || m.structDerived
  ) {
    return false;
  }
  const t = (m.transform || "").toLowerCase();
  return SAFE_NORMALIZE_TRANSFORMS.has(t);
}

export function mappingAckTier(m: EditableMapping): MappingAckTier {
  if (isIntentionalOmit(m) || !mappingRequiresRiskAck(m)) return "approve";
  if (isSafeNormalizeMapping(m)) return "approve";
  const fidelity = (m.fidelity || "").toLowerCase();
  if (fidelity === "lossy_cast" || m.typeNarrowing) return "accept_risk";
  if (hasCreateNewTypeRisk(m)) {
    const hasBlock = (m.createNewRisks || []).some(
      (r) => (r.severity || "").toLowerCase() === "block",
    );
    return hasBlock ? "accept_risk" : "review";
  }
  if (
    isSpecialtyLogicalType(m.inferredType)
    || isSpecialtyLogicalType(m.destType)
    || m.transform === "identity_specialty"
    || m.structDerived
    || m.structPolicy === "flatten_top_level_keys"
    || m.structPolicy === "flatten_deep"
    || m.structPolicy === "explode_rows"
  ) {
    return "accept_risk";
  }
  // cast / mutate (non-safe) / carrier widen → Review
  return "review";
}

export function mappingAckLabel(m: EditableMapping): string {
  const tier = mappingAckTier(m);
  if (tier === "accept_risk") return "Sign Risk Contract";
  if (tier === "review") return "Review";
  return "Approve";
}

/** True when a signed/draft contract uses a continue policy that clears Validate. */
export function mappingHasClearingRiskContract(m: EditableMapping): boolean {
  const pol = String(m.riskContract?.execution_policy || "").toUpperCase() as ExecutionPolicy;
  return Boolean(
    m.riskAcknowledged
    && m.riskContract
    && CONTINUE_EXECUTION_POLICIES.has(pol),
  );
}

export function mappingAckDoneLabel(m: EditableMapping): string {
  if (m.riskAcknowledged && mappingRequiresRiskAck(m)) {
    const policy = m.riskContract?.execution_policy;
    if (policy === "CAST_AND_CONTINUE") return "Contracted · cast";
    if (policy) return `Contracted · ${policy}`;
    return mappingAckTier(m) === "accept_risk" ? "Risk Contract signed" : "Reviewed";
  }
  return "Ready";
}

/** Lossy, mutate, cast (quarantine path), specialty, create-new risk, or STRUCT expand — G4 needs risk_acknowledged. */
export function mappingRequiresRiskAck(m: EditableMapping): boolean {
  if (isIntentionalOmit(m)) return false;
  // Safe normalize (email/trim/case) is not fidelity risk — Approve is enough.
  if (isSafeNormalizeMapping(m)) return false;
  const fidelity = (m.fidelity || "").toLowerCase();
  // cast = type path holds but unparseable values quarantine — never silent Ready.
  if (fidelity === "lossy_cast" || fidelity === "mutate" || fidelity === "cast" || m.typeNarrowing) {
    return true;
  }
  if (hasCreateNewTypeRisk(m)) return true;
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
  // Airbyte hole: Approve-all must not clear qty≠amt / user≠customer / dest fold.
  if (isFalseFriendReview(m) && !m.falseFriendConfirmed) return true;
  return false;
}

export const FALSE_FRIEND_REVIEW_KINDS: ReadonlySet<MappingReviewKind> = new Set([
  "measure_kind",
  "entity_identity",
  "dest_collision",
  "identity_leaf",
  "temporal_polarity",
]);

const REVIEW_KIND_SET: ReadonlySet<string> = new Set([
  "measure_kind",
  "entity_identity",
  "dest_collision",
  "identity_leaf",
  "temporal_polarity",
  "lossy",
  "create_new",
  "generic",
]);

const FALSE_FRIEND_KIND_ORDER: MappingReviewKind[] = [
  "dest_collision",
  "measure_kind",
  "identity_leaf",
  "temporal_polarity",
  "entity_identity",
];

export interface MappingReviewKindMeta {
  kind: MappingReviewKind;
  chip: string;
  noun: string;
  detail: string;
  primaryLabel: string;
  confirmLabel: string;
}

const REVIEW_KIND_META: Record<MappingReviewKind, Omit<MappingReviewKindMeta, "kind">> = {
  measure_kind: {
    chip: "qty≠amt",
    noun: "quantity≠amount pair(s)",
    detail: "Count is not money. Remap the destination column — do not write quantity into amount.",
    primaryLabel: "Remap dest",
    confirmLabel: "Confirm this pair",
  },
  entity_identity: {
    chip: "user≠customer",
    noun: "user≠customer pair(s)",
    detail: "Different entities share an id leaf. Remap to the intended destination — user is not customer.",
    primaryLabel: "Remap dest",
    confirmLabel: "Confirm this pair",
  },
  dest_collision: {
    chip: "dest collision",
    noun: "destination identifier collision(s)",
    detail: "Two destination names fold to the same identifier (UserID vs userid). Remap to the intended column.",
    primaryLabel: "Remap dest",
    confirmLabel: "Confirm this pair",
  },
  identity_leaf: {
    chip: "sku≠id",
    noun: "sku≠product_id pair(s)",
    detail: "Identity kinds differ (sku vs id/key). Remap to the matching destination column.",
    primaryLabel: "Remap dest",
    confirmLabel: "Confirm this pair",
  },
  temporal_polarity: {
    chip: "created≠updated",
    noun: "created≠updated pair(s)",
    detail: "Created and updated are opposite time polarity. Remap — do not write one into the other.",
    primaryLabel: "Remap dest",
    confirmLabel: "Confirm this pair",
  },
  lossy: {
    chip: "lossy",
    noun: "lossy type pair(s)",
    detail: "Type path narrows or mutates. Sign a Risk Contract — Approve eligible will not clear this.",
    primaryLabel: "Sign Risk Contract",
    confirmLabel: "Approve",
  },
  create_new: {
    chip: "create-new",
    noun: "create-new column(s)",
    detail: "This source will ADD a destination column. Confirm the name and type before Validate.",
    primaryLabel: "Review dest name",
    confirmLabel: "Approve",
  },
  generic: {
    chip: "review",
    noun: "mapping(s)",
    detail: "Engine held this pair below the auto-approve floor. Remap or confirm the pair.",
    primaryLabel: "Remap dest",
    confirmLabel: "Approve",
  },
};

export function mappingReviewKindMeta(kind: MappingReviewKind): MappingReviewKindMeta {
  return { kind, ...(REVIEW_KIND_META[kind] || REVIEW_KIND_META.generic) };
}

function parseReviewKindFromReason(reason?: string): MappingReviewKind | null {
  const text = (reason || "").toLowerCase();
  if (!text) return null;
  if (text.includes("destination identifier collision")) return "dest_collision";
  if (text.includes("measure-kind mismatch")) return "measure_kind";
  if (text.includes("identity leaf mismatch")) return "identity_leaf";
  if (text.includes("temporal polarity")) return "temporal_polarity";
  if (text.includes("entity qualifier conflict") || text.includes("conflicting entity qualifiers")) {
    return "entity_identity";
  }
  return null;
}

export function classifyMappingReview(m: EditableMapping): MappingReviewKind | null {
  if (isIntentionalOmit(m) || m.approved || m.falseFriendConfirmed) return null;
  if (m.reviewKind && REVIEW_KIND_SET.has(m.reviewKind)) return m.reviewKind;
  const fromReason = parseReviewKindFromReason(m.reason);
  if (fromReason) return fromReason;
  if (!m.requiresReview) return null;
  if (mappingRequiresRiskAck(m)) return "lossy";
  if (m.createNew) return "create_new";
  return "generic";
}

export function isFalseFriendReview(m: EditableMapping): boolean {
  const kind = classifyMappingReview(m);
  return kind != null && FALSE_FRIEND_REVIEW_KINDS.has(kind);
}

/** Per-row confirm of a false-friend. Approve eligible must not call this. */
export function confirmFalseFriendMapping(m: EditableMapping): EditableMapping {
  if (mappingRequiresRiskAck(m) && !m.riskAcknowledged) {
    return { ...m, approved: false, requiresReview: true };
  }
  if (isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m)) {
    return { ...m, approved: false, requiresReview: true };
  }
  return {
    ...m,
    approved: true,
    requiresReview: false,
    falseFriendConfirmed: true,
    reason: [m.reason, "Operator confirmed proposed pair"].filter(Boolean).join(" · "),
  };
}

/**
 * Validate-step confirm for G15. Named sources win; empty list confirms every
 * unconfirmed false-friend. Approve-all must not call this.
 */
export function confirmFalseFriendsBySource(
  mappings: EditableMapping[],
  sources?: string[] | null,
): {
  mappings: EditableMapping[];
  confirmed: string[];
  blocked: string[];
  unmatched: string[];
} {
  const named = (sources || []).map((s) => String(s || "").trim()).filter(Boolean);
  const want = new Set(named.map((s) => s.toLowerCase()));
  const confirmed: string[] = [];
  const blocked: string[] = [];
  const seen = new Set<string>();

  const next = mappings.map((m) => {
    const src = String(m.source || "").trim();
    if (!src) return m;
    const key = src.toLowerCase();
    const targeted = want.size === 0
      ? isFalseFriendReview(m) && !m.falseFriendConfirmed
      : want.has(key);
    if (!targeted) return m;
    if (!isFalseFriendReview(m) || m.falseFriendConfirmed) return m;
    seen.add(key);
    const stamped = confirmFalseFriendMapping(m);
    if (stamped.falseFriendConfirmed) {
      confirmed.push(src);
      return stamped;
    }
    blocked.push(src);
    return stamped;
  });

  const unmatched = named.filter((s) => !seen.has(s.toLowerCase()));
  return { mappings: next, confirmed, blocked, unmatched };
}

/** Operator changed the dest name — drop stale false-friend kind until rematch. */
export function applyOperatorRemapDest(m: EditableMapping, target: string): EditableMapping {
  const next = String(target || "").trim();
  const baseReason = (m.reason || "")
    .replace(/(?: · )?Operator remapped dest to .+$/u, "")
    .replace(/(?: · )?Operator cleared dest$/u, "")
    .trim();
  return {
    ...m,
    target: next,
    approved: false,
    requiresReview: true,
    reviewKind: "generic",
    falseFriendConfirmed: false,
    reason: [baseReason, next ? `Operator remapped dest to ${next}` : "Operator cleared dest"]
      .filter(Boolean)
      .join(" · "),
  };
}

/**
 * Explicit operator acceptance of fidelity / structural risk.
 * Emits a Migration Risk Contract draft. ``executionPolicy`` is required —
 * no hidden CAST_AND_CONTINUE default (Enterprise GA).
 * Server signs on Validate; boolean riskAcknowledged alone does not unlock Execute.
 */
export function acknowledgeMappingRisk(
  m: EditableMapping,
  opts?: {
    executionPolicy?: ExecutionPolicy;
    approvedBy?: string;
    reason?: string;
    rowsSampled?: number;
    estimatedRows?: number | null;
    migrationId?: string;
    table?: string;
    planId?: string;
  },
): EditableMapping {
  if (!mappingRequiresRiskAck(m)) {
    return approveMappingHonestly(m);
  }
  const policy = opts?.executionPolicy;
  if (!policy || !EXECUTION_POLICY_OPTIONS.some((p) => p.id === policy)) {
    return {
      ...m,
      approved: false,
      requiresReview: true,
      reason: [m.reason, "Choose an execution policy before signing Risk Contract"]
        .filter(Boolean)
        .join(" · "),
    };
  }
  const fidelity = (m.fidelity || "").toLowerCase();
  const ackNote =
    fidelity === "mutate"
      ? "Operator acknowledged value-mutating transform"
      : fidelity === "cast"
        ? "Operator acknowledged cast/quarantine path"
        : hasCreateNewTypeRisk(m)
          ? "Operator acknowledged create-new type risk"
          : m.structDerived || m.structPolicy === "flatten_top_level_keys" || m.structPolicy === "flatten_deep" || m.structPolicy === "explode_rows"
            ? "Operator acknowledged STRUCT/ARRAY expand policy"
            : isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType) || m.transform === "identity_specialty"
              ? "Operator acknowledged specialty identity transport"
              : "Operator acknowledged type/precision loss risk";
  const actor = (opts?.approvedBy || "map-operator").trim() || "map-operator";
  const why = (opts?.reason || ackNote).trim() || ackNote;
  const srcType = m.inferredType || "UNKNOWN";
  const dstType = m.destType || m.inferredType || "UNKNOWN";
  const lossClassification =
    fidelity === "mutate" || fidelity === "lossy_cast" || fidelity === "cast"
      ? fidelity
      : m.typeNarrowing
        ? "truncation"
        : "precision_loss";
  const clears = CONTINUE_EXECUTION_POLICIES.has(policy);
  const riskContract: MigrationRiskContractDraft = {
    column: m.source,
    target: m.target || m.source,
    source_type: srcType,
    destination_type: dstType,
    transform: m.transform || null,
    rows_sampled: opts?.rowsSampled ?? 0,
    estimated_rows: opts?.estimatedRows ?? null,
    expected_precision_loss: true,
    expected_truncation: Boolean(m.typeNarrowing),
    expected_nulls: fidelity === "cast" || policy === "QUARANTINE_ROW",
    execution_policy: policy,
    // CAST/TRANSFORM continue = quarantine holdout (not silent NULL). Stamp the
    // quarantine policy explicitly so Validate/Execute never look like "signed
    // contract = write anyway" when cells still go to DLQ.
    quarantine_policy:
      policy === "QUARANTINE_ROW"
        || policy === "CAST_AND_CONTINUE"
        || policy === "TRANSFORM_AND_CONTINUE"
        ? "QUARANTINE_ROW_on_failure"
        : policy === "STOP_COLUMN"
          ? "omit_column_on_failure"
          : "holdout_rejected_rows",
    retry_policy: "none",
    rollback_strategy: "DOCUMENT_ONLY",
    approved_by: actor,
    reason: why,
    root_cause: `${srcType} → ${dstType} fidelity risk`,
    severity: "high",
    migration_id: opts?.migrationId || opts?.planId || "",
    plan_id: opts?.planId || opts?.migrationId || null,
    table: opts?.table || "",
    loss_classification: lossClassification,
    version: 1,
  };
  return {
    ...m,
    riskAcknowledged: true,
    riskContract,
    // Fail-closed policies stamp the contract but do not Approve/unlock Execute.
    approved: clears,
    requiresReview: !clears,
    transform:
      (isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType))
      && (!m.transform || m.transform === "none")
        ? "identity_specialty"
        : m.transform,
    reason: [m.reason, createNewRiskDetail(m) || m.fidelityReason, ackNote, `policy=${policy}`].filter(Boolean).join(" · "),
  };
}

/**
 * Merge Validate-echoed Kernel target_type stamps onto Map rows.
 * Additive / create-new columns must show the same stamp Execute will refuse without.
 */
export function mergeStampedTargetTypes(
  mappings: EditableMapping[],
  stamped: Array<{
    source?: string;
    target?: string;
    target_type?: string;
    create_new?: boolean;
    assignment_strategy?: string;
  }> | null | undefined,
): EditableMapping[] {
  if (!stamped?.length) return mappings;
  const bySourceTarget = new Map<string, (typeof stamped)[number]>();
  // Source-only fallback only when that source maps to exactly one stamp —
  // never first-wins across fan-out (source→many targets).
  const bySource = new Map<string, (typeof stamped)[number] | "ambiguous">();
  for (const row of stamped) {
    const src = String(row.source || "").trim();
    const tgt = String(row.target || "").trim();
    const tt = String(row.target_type || "").trim();
    if (!src || !tt) continue;
    bySourceTarget.set(`${src}\0${tgt}`, row);
    const prev = bySource.get(src);
    if (prev === undefined) bySource.set(src, row);
    else if (prev !== "ambiguous" && prev !== row) bySource.set(src, "ambiguous");
  }
  return mappings.map((m) => {
    const srcHit = bySource.get(m.source);
    const hit =
      bySourceTarget.get(`${m.source}\0${m.target || m.source}`)
      || (srcHit && srcHit !== "ambiguous" ? srcHit : undefined);
    if (!hit?.target_type) return m;
    const stampedType = String(hit.target_type).trim();
    if (!stampedType) return m;
    const nextStrategy = String(hit.assignment_strategy || "").trim();
    const stampedCreateNew =
      typeof hit.create_new === "boolean" ? hit.create_new : undefined;
    // Bind-existing must clear stale createNew + mark dest present — otherwise
    // buildPreflightMappings re-emits create_new via existsInDestination===false.
    let createNew = m.createNew;
    let existsInDestination = m.existsInDestination;
    if (stampedCreateNew === true || nextStrategy === "create_compatible_new") {
      createNew = true;
      existsInDestination = false;
    } else if (
      stampedCreateNew === false
      || nextStrategy === "bind_existing"
    ) {
      createNew = false;
      existsInDestination = true;
    }
    return {
      ...m,
      destType: stampedType,
      createNew,
      existsInDestination,
      assignmentStrategy: nextStrategy || m.assignmentStrategy,
      reason: [
        m.reason,
        m.destType && m.destType !== stampedType
          ? `Validate stamped ${stampedType}`
          : "",
      ].filter(Boolean).join(" · "),
    };
  });
}

/** Merge Validate-echoed signed risk contracts onto Map rows (risk_id + signature). */
export function mergeSignedRiskContracts(
  mappings: EditableMapping[],
  signed: Array<{
    source?: string;
    target?: string;
    risk_contract?: MigrationRiskContractDraft | Record<string, unknown>;
    risk_acknowledged?: boolean;
  }> | null | undefined,
): EditableMapping[] {
  if (!signed?.length) return mappings;
  const bySourceTarget = new Map<string, (typeof signed)[number]>();
  const bySource = new Map<string, (typeof signed)[number]>();
  for (const row of signed) {
    const src = String(row.source || "").trim();
    const tgt = String(row.target || "").trim();
    if (!src || !row.risk_contract) continue;
    bySourceTarget.set(`${src}\0${tgt}`, row);
    if (!bySource.has(src)) bySource.set(src, row);
  }
  return mappings.map((m) => {
    const hit =
      bySourceTarget.get(`${m.source}\0${m.target || m.source}`)
      || bySource.get(m.source);
    if (!hit?.risk_contract) return m;
    const contract = hit.risk_contract as MigrationRiskContractDraft;
    const pol = String(contract.execution_policy || "").toUpperCase() as ExecutionPolicy;
    const clears = CONTINUE_EXECUTION_POLICIES.has(pol);
    return {
      ...m,
      riskContract: { ...m.riskContract, ...contract },
      riskAcknowledged: hit.risk_acknowledged !== false,
      approved: clears ? true : m.approved,
      requiresReview: clears ? false : m.requiresReview,
    };
  });
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
  if (isFalseFriendReview(m) && !m.falseFriendConfirmed) {
    return { ...m, approved: false, requiresReview: true };
  }
  if (isEnumToBooleanConflict(m) && canWidenMapping(m)) {
    return { ...widenMappingToVarchar(m), approved: true, requiresReview: false };
  }
  if (mappingRequiresRiskAck(m)) {
    // GA: only continue-policy Risk Contracts approve — FAIL_JOB/STOP_* do not unlock.
    if (mappingHasClearingRiskContract(m)) {
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
 * Normalize/hybrid attach ``childTableSpec`` from engine proposals (operator-approved).
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
  const normalizing =
    policy === "normalize_child_table" || policy === "hybrid_json_and_child";
  const fromStrategy = parent.arrayStrategies?.find((s) => s.id === policy);
  const childSpec = normalizing
    ? (fromStrategy?.child_table_spec
      || parent.childTableSpec
      || parent.proposedChildTableSpec)
    : undefined;
  const nextParent: EditableMapping = {
    ...withoutDerived[parentIdx],
    structPolicy: policy,
    approved: false,
    childTableSpec: childSpec,
    // Leaving flatten clears collision metadata so the Map table does not stick.
    flattenCollisions: flattenish ? withoutDerived[parentIdx].flattenCollisions : undefined,
    requiresReview: flattenish || exploding || normalizing
      ? true
      : withoutDerived[parentIdx].flattenCollisions?.length
        ? false
        : withoutDerived[parentIdx].requiresReview,
    reason: exploding
      ? "ARRAY explode — one output row per element (capped); parent array kept"
      : policy === "hybrid_json_and_child"
        ? "ARRAY hybrid — parent JSON + normalized child table"
        : policy === "normalize_child_table"
          ? "ARRAY normalize — child table from profiled keys; parent JSON kept fail-closed"
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
    const collisions = [...collisionFlats].map((flat) => ({
      flat,
      paths: flatOwners.get(flat) || [],
    }));
    next[parentIdx] = {
      ...nextParent,
      approved: false,
      requiresReview: true,
      flattenCollisions: collisions,
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
  // Pending/incomplete dest schema is not proven create-new — never invent Widen.
  if (m.assignmentStrategy === "pending_dest_schema") return false;
  return m.existsInDestination === false;
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
  fidelity?: string,
  transform?: string,
): number {
  // Pending / unknown destination — never inflate toward create-new certainty.
  if (pendingDestSchema) return Math.min(confidence, 0.55);
  const norm = normalizeMappingTarget(source);
  const exact = norm === target || source.toLowerCase() === target.toLowerCase();
  const fid = (fidelity || "").toLowerCase();
  const tf = (transform || "").toLowerCase();
  let next = confidence;
  if (exact) {
    if (createNew) {
      // Vary create-new identity by fidelity — avoid a flat 93% wall.
      if (fid === "lossy_cast") next = Math.min(Math.max(confidence, 0.62), 0.74);
      else if (fid === "mutate" && !SAFE_NORMALIZE_TRANSFORMS.has(tf)) {
        next = Math.min(Math.max(confidence, 0.78), 0.88);
      } else if (fid === "mutate") {
        next = Math.min(Math.max(confidence, 0.88), 0.94);
      } else if (fid === "cast") {
        next = Math.min(Math.max(confidence, 0.8), 0.9);
      } else if (fid === "preserve" || !fid) {
        next = Math.min(Math.max(confidence, 0.9), 0.96);
      } else {
        next = Math.min(Math.max(confidence, 0.86), 0.93);
      }
    } else {
      next = Math.max(confidence, fid === "lossy_cast" ? 0.7 : 0.95);
    }
  } else if (fid === "lossy_cast") {
    next = Math.min(confidence, 0.72);
  } else if (fid === "cast") {
    next = Math.min(Math.max(confidence, 0.7), 0.88);
  }
  return next;
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
      // Create-new / ADD must carry a destType stamp — Execute refuse Map VARCHAR
      // invent under partial Studio when target_type is blank (Excel→PG cliff).
      // Catalog DDL wins over sample semantic_type (BIGINT invented from 1,2,150000).
      destType: createNew && !pendingDest ? (col.inferred_type || inferred) : undefined,
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
        enumBool && m.existsInDestination === false
          ? widenMappingToVarchar(m)
          : enumBool && m.existsInDestination === true
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
        // Existing dest: never invent target_type from inferredType when Map
        // left destType blank (partial Studio honesty — write path fail-closes).
        // Create-new may still preview from inferredType / destType stamp.
        target_type: omitted
          ? undefined
          : isCreateNew
            ? (safe.destType || safe.inferredType)
            : (m.destType || safe.destType || undefined),
        source_type: safe.inferredType,
        requires_review: omitted
          ? false
          : Boolean(
              (safe.requiresReview || enumBool || (!isCreateNew && !(m.destType || safe.destType)))
              && !safe.approved,
            ),
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
        structural_class: omitted ? undefined : safe.structuralClass,
        child_table_spec: omitted ? undefined : safe.childTableSpec,
        fidelity: omitted ? undefined : safe.fidelity,
        type_narrowing: omitted ? undefined : Boolean(safe.typeNarrowing) || undefined,
        risk_acknowledged: omitted ? undefined : Boolean(safe.riskAcknowledged) || undefined,
        risk_contract: omitted ? undefined : (safe.riskContract || undefined),
        review_kind: omitted ? undefined : (safe.reviewKind || classifyMappingReview(safe) || undefined),
        // Engine G15 only clears false-friend on this flag — not Approve / user_override.
        false_friend_confirmed: omitted ? undefined : Boolean(safe.falseFriendConfirmed) || undefined,
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
    structural_class?: string;
    array_strategies?: ArrayStrategyOption[];
    child_table_spec?: ChildTableSpec;
    proposed_child_table_spec?: ChildTableSpec;
    fidelity?: string;
    fidelity_reason?: string;
    type_narrowing?: boolean;
    create_new_risks?: Array<{ kind?: string; severity?: string; message?: string }>;
    column_profile?: ColumnProfile | Record<string, unknown> | null;
    review_kind?: string;
    false_friend_confirmed?: boolean;
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
      (m.fidelity || "").trim().toLowerCase() || undefined,
      (m.transform || "").trim().toLowerCase() || undefined,
    );
    const sourceType = m.source_type;
    // Partial Studio: destSchema loaded but this target missing → never invent
    // destType from source_type (false-green Map before write fail-close).
    const destSchemaLoaded = Object.keys(destSchema || {}).length > 0;
    let destType = liveDestType || m.target_type || undefined;
    if (!destType && (rowCreateNew || !destSchemaLoaded)) {
      destType = m.source_type;
    }
    const destTypeGap = destSchemaLoaded && !destType && !rowCreateNew && !pendingDest;
    const requiresReview = Boolean(m.requires_review) || pendingDest || destTypeGap;
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
      m.struct_policy === "normalize_child_table" ||
      m.struct_policy === "hybrid_json_and_child" ||
      m.struct_policy === "store_as_json"
        ? m.struct_policy
        : structish
          ? "store_as_json"
          : arrayish
            ? "store_as_json"
            : undefined;
    const engineFidelity = (m.fidelity || "").trim().toLowerCase();
    const createNewRisks: CreateNewTypeRisk[] | undefined = Array.isArray(m.create_new_risks)
      && m.create_new_risks.length > 0
      ? m.create_new_risks
          .filter((r) => r && (r.kind || r.message))
          .map((r) => ({
            kind: String(r.kind || "create_new_risk"),
            severity: r.severity || "warn",
            message: String(r.message || ""),
          }))
      : undefined;
    const rawProfile = m.column_profile && typeof m.column_profile === "object"
      ? m.column_profile as Record<string, unknown>
      : null;
    const columnProfile: ColumnProfile | undefined = rawProfile
      ? {
          null_rate: typeof rawProfile.null_rate === "number" ? rawProfile.null_rate : undefined,
          unique_ratio: typeof rawProfile.unique_ratio === "number" ? rawProfile.unique_ratio : undefined,
          min: typeof rawProfile.min === "number" ? rawProfile.min : undefined,
          max: typeof rawProfile.max === "number" ? rawProfile.max : undefined,
          observed_precision:
            typeof rawProfile.observed_precision === "number"
              ? rawProfile.observed_precision
              : undefined,
          observed_scale:
            typeof rawProfile.observed_scale === "number"
              ? rawProfile.observed_scale
              : undefined,
          numeric_kind: rawProfile.numeric_kind != null
            ? String(rawProfile.numeric_kind)
            : undefined,
          suggested_carrier: rawProfile.suggested_carrier != null
            ? String(rawProfile.suggested_carrier)
            : undefined,
          ieee_signals: Array.isArray(rawProfile.ieee_signals)
            ? rawProfile.ieee_signals.map(String)
            : undefined,
          likely_identifier: Boolean(rawProfile.likely_identifier) || undefined,
          likely_email: Boolean(rawProfile.likely_email) || undefined,
          likely_uuid: Boolean(rawProfile.likely_uuid) || undefined,
          likely_date: Boolean(rawProfile.likely_date) || undefined,
          likely_boolean: Boolean(rawProfile.likely_boolean) || undefined,
          semantic_pattern_score:
            typeof rawProfile.semantic_pattern_score === "number"
              ? rawProfile.semantic_pattern_score
              : undefined,
        }
      : undefined;
    const lossyFidelity =
      engineFidelity === "lossy_cast"
      || engineFidelity === "mutate"
      || engineFidelity === "cast"
      || Boolean(m.type_narrowing)
      || Boolean(createNewRisks?.length);
    // Fail-closed: never invent Approve from confidence. Operator must Approve
    // (or Approve-all for eligible rows). Ready ≡ approved only.
    const autoApproved = false;
    const friendConfirmed = Boolean(m.false_friend_confirmed);
    const base: EditableMapping = {
      source: m.source,
      target: m.target,
      confidence: conf,
      inferredType: sourceType,
      destType,
      sample: sampleVal != null ? String(sampleVal) : undefined,
      approved: friendConfirmed || autoApproved,
      falseFriendConfirmed: friendConfirmed || undefined,
      isPii: m.is_pii,
      reason: specialty && !(m.reasoning || "").toLowerCase().includes("identity")
        ? [m.reasoning, `${sourceType || destType} — identity payload (dim/SRID not rewritten)`].filter(Boolean).join(" · ")
        : structish && !m.reasoning
          ? "STRUCT/JSON — choose JSON blob, flatten keys, or deep flatten"
          : arrayish && !m.reasoning
            ? "ARRAY — JSON default; optional normalize/hybrid/explode (Map policy)"
            : m.reasoning,
      existsInDestination: existsInDest,
      requiresReview: friendConfirmed
        ? false
        : requiresReview || specialty || structish || arrayish || lossyFidelity || conf < threshold,
      scoreGap: m.score_gap,
      transform: uiTf === "none" && (structish || arrayish) ? "parse_json" : uiTf,
      engineTransform: engineTf || (structish || arrayish ? "json" : undefined),
      semanticRole: m.semantic_role,
      createNew: rowCreateNew,
      assignmentStrategy: m.assignment_strategy,
      structPolicy: structPolicy ?? (arrayish ? "store_as_json" : undefined),
      structuralClass: m.structural_class,
      arrayStrategies: Array.isArray(m.array_strategies) ? m.array_strategies : undefined,
      childTableSpec: m.child_table_spec,
      proposedChildTableSpec: m.proposed_child_table_spec,
      fidelity: engineFidelity || undefined,
      fidelityReason: m.fidelity_reason || undefined,
      typeNarrowing: Boolean(m.type_narrowing),
      createNewRisks,
      structDerived: Boolean(m.struct_derived),
      structParent: m.struct_parent,
      columnProfile,
      reviewKind: m.review_kind && REVIEW_KIND_SET.has(m.review_kind)
        ? (m.review_kind as MappingReviewKind)
        : parseReviewKindFromReason(m.reasoning) || undefined,
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
  if (mode === "balanced" || mode === "migration") return 0.75;
  if (mode === "maximum") return 0.95;
  if (mode === "discovery") return 0;
  // strict | audit | unknown → fail closed to strict floor
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
  falseFriendCount: number;
  falseFriendKinds: Partial<Record<MappingReviewKind, number>>;
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
  const needsReview = active.filter(
    (m) => !m.approved || (mappingRequiresRiskAck(m) && !m.riskAcknowledged),
  ).length;
  const lowConfidence = active.filter((m) => m.confidence < threshold && !m.approved).length;
  const unmappedTarget = active.filter((m) => !String(m.target || "").trim()).length;
  const specialtyIdentity = active.filter(
    (m) =>
      (m.transform === "identity_specialty" || isSpecialtyLogicalType(m.inferredType) || isSpecialtyLogicalType(m.destType))
      && mappingRequiresRiskAck(m)
      && !m.riskAcknowledged,
  ).length;
  const existingTypeConflict = active.filter(
    (m) => isExistingEnumBooleanConflict(m) || isExistingDestTypeOverride(m),
  ).length;
  const falseFriendKinds: Partial<Record<MappingReviewKind, number>> = {};
  for (const m of active) {
    const kind = classifyMappingReview(m);
    if (kind && FALSE_FRIEND_REVIEW_KINDS.has(kind)) {
      falseFriendKinds[kind] = (falseFriendKinds[kind] || 0) + 1;
    }
  }
  const falseFriendCount = Object.values(falseFriendKinds).reduce((s, n) => s + (n || 0), 0);
  // Ready ≡ operator-approved (and Accept risk when required) — never confidence alone.
  const ready = mappings.filter((m) => {
    if (mappingRequiresRiskAck(m) && !m.riskAcknowledged) return false;
    if (isIntentionalOmit(m)) return Boolean(m.approved);
    return Boolean(m.approved && String(m.target || "").trim());
  }).length;
  const weak =
    total === 0
    || unmappedTarget > 0
    || needsReview > 0
    || lowConfidence > 0
    || existingTypeConflict > 0
    || specialtyIdentity > 0
    || falseFriendCount > 0;

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
  } else if (falseFriendCount > 0) {
    const parts = FALSE_FRIEND_KIND_ORDER
      .filter((k) => (falseFriendKinds[k] || 0) > 0)
      .map((k) => `${falseFriendKinds[k]} ${mappingReviewKindMeta(k).noun}`);
    headline = `${parts.join(" · ")} need confirm`;
    detail = "Approve eligible will not clear these. Remap the destination column, or Confirm this pair on the row.";
  } else if (needsReview > 0 || lowConfidence > 0) {
    // One row can be both requiresReview and low-confidence — count unique mappings.
    const reviewCount = active.filter(
      (m) => !m.approved && (m.requiresReview || m.confidence < threshold),
    ).length;
    headline = `${reviewCount} mapping(s) need review`;
    detail = `Accept risk on lossy/specialty rows, or Approve eligible rows only (threshold ${(threshold * 100).toFixed(0)}%).`;
  } else if (specialtyIdentity > 0) {
    headline = `${specialtyIdentity} specialty type(s) need Accept risk`;
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
    falseFriendCount,
    falseFriendKinds,
    weak,
    headline,
    detail,
  };
}
