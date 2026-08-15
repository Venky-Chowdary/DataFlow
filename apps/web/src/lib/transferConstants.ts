/**
 * Canonical Transfer Studio constants — single source for sync / schema / validation.
 * Keep IDs aligned with apps/api/services/preflight_service.py allowed sets.
 */

/** Must match apps/api/services/coercion_probe.py PREFLIGHT_SAMPLE_LIMIT (Validate≡Execute). */
export const PREFLIGHT_SAMPLE_LIMIT = 500;

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

export type ValidationModeId =
  | "balanced"
  | "strict"
  | "maximum"
  | "migration"
  | "discovery"
  | "audit";

export const VALIDATION_MODES: {
  id: ValidationModeId;
  label: string;
  threshold: string;
  detail: string;
}[] = [
  {
    id: "strict",
    label: "Strict",
    threshold: "0.85",
    detail: "Fail on fidelity risks unless an approved Migration Risk Contract continues the column.",
  },
  {
    id: "maximum",
    label: "Maximum",
    threshold: "0.95",
    detail: "Strict posture with a higher mapping confidence floor.",
  },
  {
    id: "balanced",
    label: "Balanced",
    threshold: "0.75",
    detail: "Allow approved risks; Gate-8 may use sample assurance when digests diverge.",
  },
  {
    id: "migration",
    label: "Migration",
    threshold: "0.75",
    detail: "Warn on recoverable issues; unrecoverable fidelity still hard-blocks.",
  },
  {
    id: "discovery",
    label: "Discovery",
    threshold: "report",
    detail: "Report-only — never unlocks Execute / never writes.",
  },
  {
    id: "audit",
    label: "Audit",
    threshold: "0.85",
    detail: "Hard-block audit trail — never writes.",
  },
];

export type DateLocaleId = "" | "DMY" | "MDY";

export const SYNC_MODES: { id: SyncModeId; label: string; detail: string }[] = [
  { id: "full_refresh_overwrite", label: "Full overwrite", detail: "Drop/replace destination, then load the full snapshot. Destroys existing rows." },
  { id: "full_refresh_append", label: "Full append", detail: "Keep existing rows; insert the full snapshot again (best for “load more” of a whole file)." },
  { id: "incremental_append", label: "Incremental append", detail: "Cursor-based new rows only — requires a cursor column; never rewrites history." },
  { id: "incremental_deduped", label: "Incremental deduped", detail: "Cursor + primary key upserts when the table already has keys you may update." },
  { id: "cdc", label: "CDC", detail: "Log-based changes with cursor + key; at-least-once upsert until proven otherwise." },
  { id: "scd2", label: "SCD Type 2", detail: "Versioned history with valid-from / valid-to; requires primary key." },
  { id: "mirror", label: "Mirror", detail: "Soft-delete dest keys missing from the source (_deleted). Physical COUNT(*) stays; active population is COUNT(*) WHERE NOT _deleted. Requires primary key." },
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
  sourceReadMode?: string;
}): { id: SyncModeId; label: string; detail: string }[] {
  const dest = (opts.destDriver || "").toLowerCase();
  const src = (opts.sourceDriver || "").toLowerCase();
  const fileish = opts.sourceKind === "file" || opts.sourceKind === "cloud";
  const callable = opts.sourceReadMode === "procedure" || opts.sourceReadMode === "query";
  return SYNC_MODES.filter((mode) => {
    if (mode.id === "scd2" || mode.id === "mirror") {
      if (opts.isMultiStream) return false;
      if (!dest || !SQL_HISTORY_SYNC_DESTS.has(dest)) return false;
    }
    if (mode.id === "cdc") {
      if (fileish || callable) return false;
      if (src && !CDC_CAPABLE_SOURCES.has(src)) return false;
    }
    return true;
  });
}

export const SYNC_MODE_META: Record<string, { label: string; detail: string }> = Object.fromEntries(
  SYNC_MODES.map((m) => [m.id, { label: m.label, detail: m.detail }]),
);
SYNC_MODE_META.full_refresh_mirror = SYNC_MODE_META.mirror;

/** Operator-facing sync mode — engine aliases like full_refresh_mirror stay Mirror. */
export function formatSyncModeLabel(mode?: string | null): string {
  const raw = String(mode || "").trim();
  if (!raw) return "—";
  return SYNC_MODE_META[raw]?.label ?? raw.replace(/_/g, " ");
}

export function formatSchemaPolicyLabel(policy?: string | null): string {
  const raw = String(policy || "").trim();
  if (!raw) return "—";
  return SCHEMA_POLICIES.find((p) => p.id === raw)?.label ?? raw.replace(/_/g, " ");
}

export function formatValidationModeLabel(mode?: string | null): string {
  const raw = String(mode || "").trim();
  if (!raw) return "—";
  return VALIDATION_MODES.find((v) => v.id === raw)?.label ?? raw.replace(/_/g, " ");
}

export const DATE_LOCALES: { id: DateLocaleId; label: string; detail: string }[] = [
  { id: "", label: "Auto", detail: "Infer day/month order from source sample; fail closed if ambiguous." },
  { id: "DMY", label: "DMY (day/month/year)", detail: "European / Indian / Australian date order." },
  { id: "MDY", label: "MDY (month/day/year)", detail: "United States date order." },
];

/** Single operator-facing copy for SCD2/mirror + multi-stream block (rank 74). */
export const MULTI_STREAM_SCD2_MIRROR_BLOCK =
  "Multi-stream SCD2/mirror is not supported. Switch to full/incremental/CDC, or select a single table.";

export function multiStreamScd2MirrorBlockCopy(kind: "alert" | "toast" | "schedule" | "execute" = "alert"): string {
  if (kind === "schedule") {
    return "SCD2/mirror cannot be scheduled for multiple streams — use full/incremental/CDC or a single stream.";
  }
  if (kind === "toast" || kind === "execute") {
    return MULTI_STREAM_SCD2_MIRROR_BLOCK;
  }
  return MULTI_STREAM_SCD2_MIRROR_BLOCK;
}

export const MULTI_STREAM_SCD2_PRIMARY_CTA = "Switch to full append";

