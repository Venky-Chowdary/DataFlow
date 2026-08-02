/**
 * Canonical Transfer Studio constants — single source for sync / schema / validation.
 * Keep IDs aligned with apps/api/services/preflight_service.py allowed sets.
 */

export type SyncModeId =
  | "full_refresh_overwrite"
  | "full_refresh_append"
  | "incremental_append"
  | "incremental_deduped"
  | "cdc"
  | "scd2"
  | "mirror";

export type SchemaPolicyId =
  | "manual_review"
  | "propagate_columns"
  | "propagate_all"
  | "pause_on_change"
  | "type_locked";

export type ValidationModeId = "balanced" | "strict" | "maximum";
export type DateLocaleId = "" | "DMY" | "MDY";

export const SYNC_MODES: { id: SyncModeId; label: string; detail: string }[] = [
  { id: "full_refresh_overwrite", label: "Full overwrite", detail: "Drop/replace destination, then load the full snapshot." },
  { id: "full_refresh_append", label: "Full append", detail: "Keep existing rows; append the full snapshot (100k + 100k → 200k)." },
  { id: "incremental_append", label: "Incremental append", detail: "Cursor-based new rows only — never rewrites history." },
  { id: "incremental_deduped", label: "Incremental deduped", detail: "Cursor + primary key upserts for a final table." },
  { id: "cdc", label: "CDC", detail: "Log-based changes with cursor + key; at-least-once upsert until proven otherwise." },
  { id: "scd2", label: "SCD Type 2", detail: "Versioned history with valid-from / valid-to; requires primary key." },
  { id: "mirror", label: "Mirror", detail: "Keep destination in sync with soft-deletes for missing keys; requires primary key." },
];

export const SCHEMA_POLICIES: { id: SchemaPolicyId; label: string; detail: string }[] = [
  {
    id: "manual_review",
    label: "Manual approval",
    detail: "Detect drift; keep the approved contract until you review (safest default).",
  },
  {
    id: "propagate_columns",
    label: "Propagate columns",
    detail: "Auto-add new destination columns on transfer (type changes still need review).",
  },
  {
    id: "propagate_all",
    label: "Propagate columns (all streams)",
    detail: "Same additive ADD COLUMN behavior as Propagate columns (not type auto-rewrite). Incompatible type changes still need review.",
  },
  {
    id: "pause_on_change",
    label: "Pause on drift",
    detail: "Stop scheduled runs when schema changes — best for production warehouses.",
  },
  {
    id: "type_locked",
    label: "Type locked",
    detail: "Reject type changes at the destination — fail closed on incompatible casts.",
  },
];

export const VALIDATION_MODES: { id: ValidationModeId; label: string; threshold: string }[] = [
  { id: "strict", label: "Strict", threshold: "0.85" },
  { id: "maximum", label: "Maximum", threshold: "0.95" },
  { id: "balanced", label: "Balanced", threshold: "0.75" },
];

/** Fallback when the schedules API does not return sync_modes — never invent "incremental". */
export const DEFAULT_SYNC_MODE_IDS: SyncModeId[] = SYNC_MODES.map((m) => m.id);

/** Destinations that honor SCD2 / mirror streaming paths in the engine. */
export const SQL_HISTORY_SYNC_DESTS = new Set([
  "postgresql",
  "mysql",
  "sqlite",
  "snowflake",
  "bigquery",
  "redshift",
  "generic_sql",
  "sqlserver",
  "mssql",
  "oracle",
  "duckdb",
]);

/** Sources that can drive CDC (log / change-stream) in production. */
export const CDC_CAPABLE_SOURCES = new Set([
  "postgresql",
  "mysql",
  "sqlserver",
  "mssql",
  "oracle",
  "mongodb",
  "azure_sql_database",
  "microsoft_sql_server",
  "amazon_rds_sql_server",
  "amazon_rds_postgresql",
  "amazon_rds_mysql",
  "amazon_aurora_postgresql",
  "amazon_aurora_mysql",
]);

/**
 * Sync modes the operator may pick for this route — hide engine-unsupported
 * combinations so client deploy cannot select a mode that fails at Execute.
 */
export function availableSyncModes(opts: {
  destDriver: string;
  sourceDriver: string;
  sourceKind: "file" | "database" | "cloud" | string;
  isMultiStream: boolean;
}): { id: SyncModeId; label: string; detail: string }[] {
  const dest = (opts.destDriver || "").toLowerCase();
  const src = (opts.sourceDriver || "").toLowerCase();
  const fileish = opts.sourceKind === "file" || opts.sourceKind === "cloud";
  return SYNC_MODES.filter((mode) => {
    if (mode.id === "scd2" || mode.id === "mirror") {
      if (opts.isMultiStream) return false;
      if (!dest || !SQL_HISTORY_SYNC_DESTS.has(dest)) return false;
    }
    if (mode.id === "cdc") {
      if (fileish) return false;
      if (src && !CDC_CAPABLE_SOURCES.has(src)) return false;
    }
    return true;
  });
}

export const SYNC_MODE_META: Record<string, { label: string; detail: string }> = Object.fromEntries(
  SYNC_MODES.map((m) => [m.id, { label: m.label, detail: m.detail }]),
);

export const DATE_LOCALES: { id: DateLocaleId; label: string; detail: string }[] = [
  { id: "", label: "Auto", detail: "Infer day/month order from source sample; fail closed if ambiguous." },
  { id: "DMY", label: "DMY (day/month/year)", detail: "European / Indian / Australian date order." },
  { id: "MDY", label: "MDY (month/day/year)", detail: "United States date order." },
];
