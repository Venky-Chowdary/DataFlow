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
}): {
  validation_mode?: string;
  schema_policy?: string;
  backfill_new_fields: boolean;
} {
  const validationMode = namedStudioValidationMode(input.validationMode);
  const schemaPolicy = namedStudioSchemaPolicy(input.schemaPolicy);
  const out: {
    validation_mode?: string;
    schema_policy?: string;
    backfill_new_fields: boolean;
  } = {
    backfill_new_fields: Boolean(input.backfillNewFields) && schemaPolicyBackfills(schemaPolicy),
  };
  if (validationMode) out.validation_mode = validationMode;
  if (schemaPolicy) out.schema_policy = schemaPolicy;
  return out;
}
