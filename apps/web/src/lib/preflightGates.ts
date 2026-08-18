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
