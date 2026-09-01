/**
 * Canonical preflight gate catalog — single source for Validate UI + local preflight.
 * IDs MUST match backend GateId values in packages/preflight and preflight_service.
 */

export interface GateCatalogEntry {
  id: string;
  label: string;
  icon: string;
  rule: string;
  /** Older UI / local-preflight ids that still appear in stored results. */
  aliases?: string[];
}

export const GATE_CATALOG: GateCatalogEntry[] = [
  { id: "g1_source", label: "Source readable", icon: "database", rule: "Source endpoint connects and rows can be read." },
  { id: "g2_destination", label: "Destination write access", icon: "server", rule: "Destination is reachable and privilege metadata proves write/create (or soft-falls back when the catalog is unavailable)." },
  {
    id: "g3_schema_contract",
    label: "Schema contract",
    icon: "layers",
    rule: "Source and target schemas are compatible.",
    aliases: ["g3_schema"],
  },
  {
    id: "g4_mapping_confidence",
    label: "Column mappings",
    icon: "sparkle",
    rule: "Every column maps above the confidence threshold.",
    aliases: ["g4_mapping"],
  },
  {
    id: "g5_dry_run",
    label: "Sample dry-run",
    icon: "code",
    rule: "Sample rows pass the same transforms writers use.",
    aliases: ["g5_transform"],
  },
  {
    id: "g9_data_integrity",
    label: "Data integrity",
    icon: "shield",
    rule: "Encoding, required nulls, identity-key duplicates, and financial precision on the Validate sample.",
  },
  {
    id: "g6_target_ddl",
    label: "Target DDL",
    icon: "scan",
    rule: "Any required CREATE / ALTER statements are valid.",
    aliases: ["g6_ddl"],
  },
  { id: "g7_capacity", label: "Staging capacity", icon: "trend", rule: "Destination has headroom for the row volume." },
  {
    id: "g8_reconciliation",
    label: "Sample reconciliation",
    icon: "activity",
    rule: "Pre-write sample: identity mappings keep values; identity-key uniqueness holds. Post-load checksum runs after Execute.",
  },
  { id: "g9_sync_contract", label: "Sync contract", icon: "transfer", rule: "Cursor and primary-key contract satisfy the sync mode." },
  { id: "g10_schema_policy", label: "Schema change policy", icon: "gate", rule: "Detected drift is allowed by the schema policy." },
  { id: "g11_validation_posture", label: "Validation posture", icon: "lock", rule: "Overall posture meets the selected validation mode." },
  { id: "schema_drift", label: "Schema drift", icon: "alert", rule: "Live source/destination schema no longer matches the saved mapping contract." },
  {
    id: "g13_source_coverage",
    label: "Source column coverage",
    icon: "layers",
    rule: "Every source column is mapped to a destination column or declared an intentional omission — unaccounted columns block instead of being dropped.",
  },
  {
    id: "g14_destination_requirements",
    label: "Destination required columns",
    icon: "layers",
    rule: "Every NOT NULL destination column is filled by a mapping, a DEFAULT, or an identity/generated value — otherwise the write is refused before it starts.",
  },
  {
    id: "g15_dest_exists_shape",
    label: "Dest-exists shape",
    icon: "layers",
    rule: "Existing-table shape is classified once (equal / source-superset / dest-superset / overlap). Writes are name-addressed — never source-positional. Dest-only columns stay off SET.",
  },
  {
    id: "g16_field_reduction",
    label: "Field reduction governance",
    icon: "layers",
    rule: "Every source column not carried to the destination holds a typed reduction reason. A reason that claims a fact about the data (empty / constant) is checked against the Validate sample and blocks when the sample disproves it; archive_only must name the archive. Sample evidence — never population proof.",
  },
  {
    id: "g19_dest_schema_replacement",
    label: "Destination schema replacement",
    icon: "layers",
    rule: "A full refresh drops and recreates the destination from the source shape. Where an existing column declares a carrier the source would overflow, the replacement is named and blocks — a signed Migration Risk Contract records it instead of hiding it.",
  },
  {
    id: "g18_cdc_snapshot_mode",
    label: "CDC snapshot mode",
    icon: "transfer",
    rule: "snapshot_mode=never without a stored watermark blocks at Validate (same kernel Execute uses). initial / when_needed snapshot. CDC remains at-least-once upsert.",
  },
  {
    id: "g3f_population_fit",
    label: "Population fit",
    icon: "scan",
    rule: "Bounded destination carriers (DECIMAL(p,s) / VARCHAR(n) / sized INTEGER) are decided on the rows this run actually holds, with the write path's own fit predicates. A finding proves those values cannot be written; a clean scan is population proof only when every source row was scanned.",
  },
  {
    id: "constraint_fk",
    label: "Foreign key coverage",
    icon: "shield",
    rule: "Destination FK columns must be mapped (or FK risk acknowledged). Schema metadata coverage only — not population orphan proof.",
  },
  {
    id: "g21_control_totals",
    label: "Control totals",
    icon: "activity",
    rule: "Declared monetary columns get an independent source SUM compared to the destination SUM after write. A row count is not a ledger balance. Browser sample SUM is not that proof.",
  },
  {
    id: "g22_dest_referential_integrity",
    label: "Destination referential integrity",
    icon: "shield",
    rule: "After write, every source relationship is dest-enforced or anti-join scanned with zero orphans. Schema FK coverage and a sample orphan probe are not dest population proof.",
  },
];

export const CORE_ENGINE_GATE_IDS = [
  "g1_source",
  "g2_destination",
  "g3_schema_contract",
  "g4_mapping_confidence",
  "g5_dry_run",
  "g9_data_integrity",
  "g6_target_ddl",
  "g7_capacity",
  "g8_reconciliation",
] as const;

/**
 * What the engine is *doing* in each core stage, in the order it runs them.
 *
 * "Engine running G1–G9" told an operator nothing: a client watching a million
 * rows migrate could not tell which internal id was inspecting their data, or
 * that the wait was work rather than a hang. These remain the stage *names*.
 * The running ticker must not walk them on a timer — that looped 1/9…9/9
 * during a long 1M scan. Live progress is rows scanned + wall clock.
 */
export const ENGINE_STAGES: { id: string; stage: string; running: string }[] = [
  { id: "g1_source", stage: "Source acquisition", running: "Reading source catalog and sample" },
  { id: "g2_destination", stage: "Destination probe", running: "Probing destination privileges" },
  { id: "g3_schema_contract", stage: "Schema contract diff", running: "Diffing source and destination schemas" },
  { id: "g4_mapping_confidence", stage: "Semantic mapping", running: "Assigning columns by semantic score" },
  { id: "g5_dry_run", stage: "Transform dry-run", running: "Running writer transforms on sample rows" },
  { id: "g9_data_integrity", stage: "Integrity scan", running: "Scanning encoding, nulls, identity and precision" },
  { id: "g6_target_ddl", stage: "DDL compilation", running: "Compiling destination CREATE / ALTER plan" },
  { id: "g7_capacity", stage: "Capacity estimate", running: "Estimating destination headroom and runtime" },
  { id: "g8_reconciliation", stage: "Reconciliation contract", running: "Binding the post-load checksum contract" },
];

export function engineStageLabel(id: string): string {
  const canonical = canonicalizeGateId(id);
  return ENGINE_STAGES.find((s) => s.id === canonical)?.stage ?? gateLabel(id);
}

const ALIAS_TO_CANONICAL: Record<string, string> = {};
for (const entry of GATE_CATALOG) {
  for (const alias of entry.aliases ?? []) {
    ALIAS_TO_CANONICAL[alias] = entry.id;
  }
}

export function canonicalizeGateId(id: string): string {
  return ALIAS_TO_CANONICAL[id] ?? id;
}

export function gateCatalogEntry(id: string): GateCatalogEntry {
  const canonical = canonicalizeGateId(id);
  const hit = GATE_CATALOG.find((g) => g.id === canonical);
  if (hit) return hit;
  return {
    id,
    label: id.replace(/^g\d+_/, "").replace(/_/g, " "),
    icon: "gate",
    rule: "Validation rule enforced before transfer.",
  };
}

export function gateLabel(id: string): string {
  return gateCatalogEntry(id).label;
}

/**
 * Title for one blocker card / rail line.
 *
 * Proof-bundle blockers are positionally numbered (`proof_0`), not gates, so the
 * gate catalog can only spell their internal id back at the operator. Name the
 * cause from its own message instead — an operator must never be told the reason
 * they cannot execute is "proof 0".
 */
export function isInternalGateId(id: string): boolean {
  return /^proof_\d+$/i.test(String(id || ""));
}

export function blockerTitle(id: string, message?: string): string {
  if (!isInternalGateId(id)) return gateLabel(id);
  const text = String(message || "").trim();
  if (!text) return "Transfer proof blocker";
  const clause = text.split(/[.;\n]|\s—\s/)[0].trim() || text;
  return clause.length > 72 ? `${clause.slice(0, 69).trimEnd()}…` : clause;
}
