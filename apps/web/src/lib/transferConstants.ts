/**
 * Canonical Transfer Studio constants — single source for sync / schema / validation.
 * Keep IDs aligned with apps/api/services/preflight_service.py allowed sets.
 */

import { destSchemaIsRecreatedOnOverwrite } from "./destSchemaRecreate";

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
  | "type_locked"
  | "block_all";

/**
 * Classic wizard sessions start on manual approval so a new column pauses
 * until the operator approves it — Execute does not ADD COLUMN. Schedules
 * keep their own default. Stored jobs keep the policy they saved.
 */
export const DEFAULT_SCHEMA_POLICY: SchemaPolicyId = "manual_review";

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
export type NumberLocaleId = "" | "US" | "EU";

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
    detail: "Validate blocks on new/renamed columns until you Acknowledge or remap. Execute does not ADD COLUMN.",
  },
  {
    id: "propagate_columns",
    label: "Propagate columns",
    detail: "Validate auto-maps additive columns. Execute ADD COLUMN. Type narrow still pauses.",
  },
  {
    id: "propagate_all",
    label: "Propagate columns (all streams)",
    detail: "Same ADD COLUMN kernel as propagate columns — every selected stream on this job. Does not rewrite destination types.",
  },
  {
    id: "pause_on_change",
    label: "Pause on drift",
    detail: "Any detected change — including additive — pauses Validate and scheduled beats.",
  },
  {
    id: "type_locked",
    label: "Type locked",
    detail: "Widen and dest type changes pause. New columns need review. No silent cast.",
  },
  {
    id: "block_all",
    label: "Block all changes",
    detail: "Any source change blocks until you Accept drift for this run. Destination-only columns do not block.",
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
 * Per destination driver, the sync modes Validate's destination gates allow,
 * from ``GET /transfer/capabilities`` ``destination_sync_modes`` (CLM-8).
 * ``notes`` are honest captions for a mode that runs, but not as its name
 * suggests (an "overwrite" into HubSpot updates records by key).
 */
export type DestinationSyncModeTable = Record<
  string,
  { sync_modes: string[]; notes?: Record<string, string> }
>;

let _destinationSyncModes: DestinationSyncModeTable | null = null;

/** Install the capabilities table (Studio boot / Transfer page refresh). */
export function setDestinationSyncModes(table: unknown): void {
  if (!table || typeof table !== "object") return;
  const out: DestinationSyncModeTable = {};
  for (const [dest, row] of Object.entries(table as Record<string, unknown>)) {
    const modes = (row as { sync_modes?: unknown })?.sync_modes;
    if (!Array.isArray(modes)) continue;
    const notes = (row as { notes?: unknown })?.notes;
    out[dest.toLowerCase()] = {
      sync_modes: modes.map((m) => String(m)),
      notes: notes && typeof notes === "object" ? (notes as Record<string, string>) : {},
    };
  }
  _destinationSyncModes = Object.keys(out).length ? out : null;
}

export function getDestinationSyncModes(): DestinationSyncModeTable | null {
  return _destinationSyncModes;
}

/**
 * Offline fallback when capabilities are unreachable: the destination rules
 * that do not need the server. Kafka is append only; CDC needs a destination
 * that can apply a DELETE (services/cdc_destination_support.py).
 */
const APPEND_ONLY_DESTS = new Set(["kafka", "apache_kafka"]);
const CDC_DELETE_DESTS = new Set([
  "postgresql", "redshift", "mysql", "sqlite", "generic_sql", "mongodb", "oracle", "sqlserver",
  "mssql", "snowflake", "bigquery", "duckdb", "databricks", "iceberg", "apache_iceberg",
  "pgvector", "qdrant", "weaviate", "pinecone",
]);
const APPEND_MODES = new Set<SyncModeId>(["full_refresh_append", "incremental_append"]);

/**
 * Sync modes the operator may pick for this route — hide engine-unsupported
 * combinations so client deploy cannot select a mode that fails at Execute.
 * The destination side intersects with ``destination_sync_modes`` from
 * ``/transfer/capabilities``; a parity test (tests/test_clm8_destination_sync_modes_parity.py)
 * holds that table to Validate's gates.
 */
export function availableSyncModes(opts: {
  destDriver: string;
  sourceDriver: string;
  sourceKind: "file" | "database" | "cloud" | string;
  isMultiStream: boolean;
  sourceReadMode?: string;
  destWriteMode?: string;
  /** Override the installed capabilities table (tests, previews). */
  destinationSyncModes?: DestinationSyncModeTable | null;
}): { id: SyncModeId; label: string; detail: string }[] {
  const dest = (opts.destDriver || "").toLowerCase();
  const src = (opts.sourceDriver || "").toLowerCase();
  const fileish = opts.sourceKind === "file" || opts.sourceKind === "cloud";
  const callable = opts.sourceReadMode === "procedure" || opts.sourceReadMode === "query"
    || opts.destWriteMode === "procedure" || opts.destWriteMode === "query";
  const table = opts.destinationSyncModes === undefined ? _destinationSyncModes : opts.destinationSyncModes;
  const row = dest && table ? table[dest] : undefined;
  const allowed = row ? new Set(row.sync_modes) : null;
  return SYNC_MODES.filter((mode) => {
    if (allowed && !allowed.has(mode.id)) return false;
    if (!allowed && dest) {
      if (APPEND_ONLY_DESTS.has(dest) && !APPEND_MODES.has(mode.id)) return false;
      if (mode.id === "cdc" && !CDC_DELETE_DESTS.has(dest)) return false;
    }
    if (mode.id === "scd2" || mode.id === "mirror") {
      if (callable) return false;
      if (!dest || !SQL_HISTORY_SYNC_DESTS.has(dest)) return false;
    }
    if (mode.id === "cdc") {
      if (fileish || callable) return false;
      if (src && !CDC_CAPABLE_SOURCES.has(src)) return false;
    }
    return true;
  }).map((mode) => {
    const note = row?.notes?.[mode.id];
    return note ? { ...mode, detail: `${mode.detail} ${note}` } : mode;
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

export const NUMBER_LOCALES: { id: NumberLocaleId; label: string; detail: string }[] = [
  { id: "", label: "Auto", detail: "Parse $1,000.00 and €2.000,50. Refuse a lone 1,234 / 1.234 — that is US thousands or EU decimal." },
  { id: "US", label: "US (1,234.56)", detail: "Comma thousands, dot decimal." },
  { id: "EU", label: "EU (1.234,56)", detail: "Dot thousands, comma decimal." },
];

/**
 * Destination-aware sync caption. Full append into a leftover empty table is
 * still dest-exists — live NUMBER/VARCHAR widths bind; Map does not ALTER them.
 */
export function syncModeHonestyLine(
  mode: string,
  destTableExists: boolean | null,
  destDbType?: string | null,
  destKind?: string | null,
): string {
  // A file export writes a new artifact per run; table prose does not apply.
  if (destKind === "file_export") {
    return (
      "Writes a new export file for this run. No table is created, appended to "
      + "or altered, and earlier export files are left unchanged."
    );
  }
  if (mode === "full_refresh_append") {
    if (destTableExists === true) {
      return (
        "Inserts into the existing table. Live destination types bind — Full append "
        + "does not CREATE a new table and does not ALTER NUMBER/VARCHAR widths. "
        + "An empty leftover table still exists. Existing destination key constraints still apply. "
        + "Each run inserts its full snapshot again: on a table with no enforced key, a rerun "
        + "stores another copy of every row already there (Validate counts them). Use upsert, "
        + "deduped append or overwrite to keep one copy."
      );
    }
    if (destTableExists === false) {
      if (["mysql", "mariadb"].includes(String(destDbType || "").toLowerCase())) {
        return "Table is missing: plain append creates no PRIMARY KEY. A selected identity key alone does not change DDL. "
          + "A server that requires a primary key rejects keyless CREATE. Validate checks the effective write policy.";
      }
      return (
        "Table is missing — first write CREATE TABLE from Map types, then insert. "
        + "This is create-new, not append into leftover DDL."
      );
    }
    return (
      "Keep existing rows; insert the full snapshot again. Confirm whether the "
      + "destination object exists before Execute — leftover empty tables still count as existing."
    );
  }
  if (mode === "full_refresh_overwrite") {
    if (destTableExists === true) {
      if (destSchemaIsRecreatedOnOverwrite(destDbType)) {
        return (
          "Drops and recreates the existing table from the source shape. "
          + "Destroys current rows and the declared carriers. "
          + "Validate names a replacement that would overflow a declared carrier (G19) "
          + "and blocks Execute until you remap or sign a Migration Risk Contract."
        );
      }
      return (
        "Replaces rows in the existing table. Destroys current data. "
        + "Does not by itself ALTER live column widths — widen in the warehouse or map to a new column."
      );
    }
    return "Drop/replace destination, then load the full snapshot. Destroys existing rows if the object already exists.";
  }
  return SYNC_MODES.find((m) => m.id === mode)?.detail ?? "";
}

/**
 * Schema-policy caption — matches ``resolve_schema_evolution`` (Validate + Execute SSOT).
 * Hard type-narrow always pauses, including under propagate_* (not Airbyte airbyte_meta).
 */
export function schemaPolicyHonestyLine(policy: string): string {
  switch ((policy || "").trim().toLowerCase()) {
    case "propagate_columns":
      return (
        "Validate auto-maps additive columns; Execute issues ADD COLUMN. "
        + "Type narrow, PK, and dest-only NOT NULL still pause — not a silent rewrite."
      );
    case "propagate_all":
      return (
        "Same ADD COLUMN kernel as propagate columns — every selected stream on this job. "
        + "Does not rewrite destination types. Hard-breaking changes still pause."
      );
    case "pause_on_change":
      return (
        "Any detected change — including additive — pauses Validate and scheduled beats. "
        + "Nothing is written until you change policy or remap."
      );
    case "type_locked":
      return (
        "Widen and destination type changes pause. New columns need review. "
        + "Execute does not silent-cast."
      );
    case "block_all":
      return (
        "Any source change — including a nullable add — blocks until you "
        + "Accept drift for this run. Destination-only columns stay an audit note. "
        + "Hard type-narrow cannot be accepted away."
      );
    case "manual_review":
    default:
      return (
        "Validate blocks on new or renamed columns until you Acknowledge or remap. "
        + "Execute does not ADD COLUMN. Hard type-narrow always pauses."
      );
  }
}


