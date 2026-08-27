/**
 * Named data / migration rules when Jobs opens Transfer Studio.
 *
 * Restore the job's spoken posture. Never invent skip_preflight or
 * propagate_all from a missing field. A job that actually ran
 * propagate_all may restore it — that was an operator choice.
 */

import {
  SCHEMA_POLICIES,
  VALIDATION_MODES,
  type SchemaPolicyId,
  type ValidationModeId,
} from "./transferConstants";

const VALIDATION_IDS = new Set<string>(VALIDATION_MODES.map((m) => m.id));
const SCHEMA_IDS = new Set<string>(SCHEMA_POLICIES.map((p) => p.id));

export type StudioDataRules = {
  validationMode: ValidationModeId | "";
  schemaPolicy: SchemaPolicyId | "";
};

export function namedStudioValidationMode(raw?: string | null): ValidationModeId | "" {
  const mode = String(raw || "").trim().toLowerCase();
  return VALIDATION_IDS.has(mode) ? (mode as ValidationModeId) : "";
}

export function namedStudioSchemaPolicy(raw?: string | null): SchemaPolicyId | "" {
  const policy = String(raw || "").trim().toLowerCase();
  return SCHEMA_IDS.has(policy) ? (policy as SchemaPolicyId) : "";
}

export function schemaPolicyBackfills(policy: string): boolean {
  return policy === "propagate_columns" || policy === "propagate_all";
}

/**
 * SQL table writers only — same set as ``services.pre_ingestion_staging._STAGING_SUPPORTED``.
 * Mongo / files / SaaS cannot host ``{table}_df_staging``.
 */
const STAGING_SUPPORTED_DESTS = new Set([
  "sqlite",
  "postgresql",
  "postgres",
  "mysql",
  "sqlserver",
  "mssql",
  "oracle",
  "snowflake",
  "redshift",
  "generic_sql",
  "duckdb",
  "bigquery",
]);

export function writeViaStagingSupported(destType?: string | null): boolean {
  const dest = String(destType || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  return STAGING_SUPPORTED_DESTS.has(dest);
}

export function jobStudioDataRules(job: {
  validation_mode?: string;
  schema_policy?: string;
  transfer_request?: { validation_mode?: string; schema_policy?: string };
} | null | undefined): StudioDataRules {
  const req = job?.transfer_request;
  return {
    validationMode: namedStudioValidationMode(req?.validation_mode || job?.validation_mode),
    schemaPolicy: namedStudioSchemaPolicy(req?.schema_policy || job?.schema_policy),
  };
}

/** Persist Studio data / migration rules onto a new pipeline. Never skip_preflight. */
export function studioSchedulePolicies(input: {
  validationMode?: string;
  schemaPolicy?: string;
  backfillNewFields?: boolean;
  writeViaStaging?: boolean;
  priorityColumn?: string;
  priorityDirection?: "asc" | "desc" | string;
  rowLimit?: number;
  syncMode?: string;
  snapshotMode?: string;
}): {
  validation_mode?: string;
  schema_policy?: string;
  backfill_new_fields: boolean;
  write_via_staging: boolean;
  priority_column: string;
  priority_direction: "asc" | "desc";
  row_limit: number;
  snapshot_mode?: string;
} {
  const validationMode = namedStudioValidationMode(input.validationMode);
  const schemaPolicy = namedStudioSchemaPolicy(input.schemaPolicy);
  const direction = input.priorityDirection === "asc" ? "asc" : "desc";
  const limit = Math.max(0, Number(input.rowLimit || 0) || 0);
  const out: {
    validation_mode?: string;
    schema_policy?: string;
    backfill_new_fields: boolean;
    write_via_staging: boolean;
    priority_column: string;
    priority_direction: "asc" | "desc";
    row_limit: number;
    snapshot_mode?: string;
  } = {
    backfill_new_fields: Boolean(input.backfillNewFields) && schemaPolicyBackfills(schemaPolicy),
    write_via_staging: Boolean(input.writeViaStaging),
    priority_column: String(input.priorityColumn || "").trim(),
    priority_direction: direction,
    row_limit: limit,
  };
  if (validationMode) out.validation_mode = validationMode;
  if (schemaPolicy) out.schema_policy = schemaPolicy;
  if (String(input.syncMode || "").trim().toLowerCase() === "cdc") {
    const mode = String(input.snapshotMode || "initial").trim().toLowerCase().replace(/-/g, "_");
    out.snapshot_mode = mode || "initial";
  }
  return out;
}
