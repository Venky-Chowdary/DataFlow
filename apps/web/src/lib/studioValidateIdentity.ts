/**
 * Validate contract identity — Advanced knobs that Execute actually consumes
 * must invalidate a prior green Validate. Map/sync/dest were already in the
 * key; locales, staging, CDC, priority, and recipe were not.
 *
 * Studio SSOT. TransferPage must not keep a second JSON shape.
 */

export type ValidateMappingIdentity = {
  source: string;
  target: string;
  transform?: string;
  engineTransform?: string;
  approved?: boolean;
  createNew?: boolean;
  assignmentStrategy?: string;
  destType?: string;
};

export type ValidateStreamFieldIdentity = {
  cursorField?: string;
  primaryKeyField?: string;
  cursorSemantics?: string;
};

export type ValidateShapeStepIdentity = {
  op?: string;
  column?: string;
  enabled?: boolean;
};

export type ValidateContractInput = {
  syncMode: string;
  primaryKeyField: string;
  cursorField: string;
  validationMode: string;
  schemaPolicy: string;
  targetCollection: string;
  destType: string;
  targetDb: string;
  destKindMode: string;
  destSchema: string;
  mappings: ValidateMappingIdentity[];
  dateLocale?: string;
  numberLocale?: string;
  backfillNewFields?: boolean;
  writeViaStaging?: boolean;
  deliveryGuarantee?: string;
  allowAppendOnly?: boolean;
  snapshotMode?: string;
  priorityColumn?: string;
  priorityDirection?: string;
  rowLimit?: number;
  cursorSemantics?: string;
  streamFields?: Record<string, ValidateStreamFieldIdentity>;
  shapeRecipeHash?: string;
  shapeSteps?: ValidateShapeStepIdentity[];
  multiSubnetFailover?: boolean;
  cdcRowFilter?: string;
  schemaDriftAcknowledged?: boolean;
};

function mappingFingerprint(maps: ValidateMappingIdentity[]): Array<Array<string | boolean>> {
  return maps.map((m) => [
    m.source,
    m.target,
    m.transform ?? "",
    m.engineTransform ?? "",
    Boolean(m.approved),
    Boolean(m.createNew),
    m.assignmentStrategy ?? "",
    m.destType ?? "",
  ]);
}

function streamFieldsFingerprint(
  fields: Record<string, ValidateStreamFieldIdentity> | undefined,
): Record<string, [string, string, string]> {
  return Object.fromEntries(
    Object.entries(fields || {})
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, f]) => [
        name,
        [f.cursorField || "", f.primaryKeyField || "", f.cursorSemantics || ""] as [
          string,
          string,
          string,
        ],
      ]),
  );
}

function shapeFingerprint(input: ValidateContractInput): string {
  const hash = String(input.shapeRecipeHash || "").trim();
  if (hash) return hash;
  return JSON.stringify(
    (input.shapeSteps || []).map((s) => [s.op || "", s.column || "", s.enabled !== false]),
  );
}

/** Stable JSON identity for Validate≡Execute. Changing any field must change the key. */
export function buildValidateContractKey(input: ValidateContractInput): string {
  return JSON.stringify({
    syncMode: input.syncMode,
    primaryKeyField: input.primaryKeyField,
    cursorField: input.cursorField,
    validationMode: input.validationMode,
    schemaPolicy: input.schemaPolicy,
    targetCollection: String(input.targetCollection || "").trim(),
    destType: input.destType,
    targetDb: input.targetDb,
    destKindMode: input.destKindMode,
    destSchema: String(input.destSchema || "").trim(),
    mappings: mappingFingerprint(input.mappings),
    dateLocale: input.dateLocale || "",
    numberLocale: input.numberLocale || "",
    backfillNewFields: Boolean(input.backfillNewFields),
    writeViaStaging: Boolean(input.writeViaStaging),
    deliveryGuarantee: input.deliveryGuarantee || "at_least_once",
    allowAppendOnly: Boolean(input.allowAppendOnly),
    snapshotMode: input.snapshotMode || "",
    priorityColumn: input.priorityColumn || "",
    priorityDirection: input.priorityDirection || "desc",
    rowLimit: Number(input.rowLimit || 0),
    cursorSemantics: input.cursorSemantics || "",
    streamFields: streamFieldsFingerprint(input.streamFields),
    shape: shapeFingerprint(input),
    multiSubnetFailover: Boolean(input.multiSubnetFailover),
    cdcRowFilter: input.cdcRowFilter || "all",
    schemaDriftAcknowledged: Boolean(input.schemaDriftAcknowledged),
  });
}

export function validateContractStillHolds(previous: string | null | undefined, next: string): boolean {
  return Boolean(previous) && previous === next;
}

/** Validate proof hashes Execute already consumes — Studio→Schedule must not drop them. */
export type ValidateProofBundle = {
  decision_artifact_hash?: string;
  decision_artifact?: { content_hash?: string };
  ddl_identity?: { ddl_identity_hash?: string };
};

export function studioScheduleValidateIdentity(preflight: {
  proof_bundle?: ValidateProofBundle;
} | null | undefined): {
  approved_decision_artifact_hash: string;
  approved_ddl_identity_hash: string;
} {
  const bundle = preflight?.proof_bundle;
  const decision = String(
    bundle?.decision_artifact_hash || bundle?.decision_artifact?.content_hash || "",
  ).trim();
  const ddl = String(bundle?.ddl_identity?.ddl_identity_hash || "").trim();
  return {
    approved_decision_artifact_hash: decision,
    approved_ddl_identity_hash: ddl,
  };
}

/** Read-only display of a persisted hash. Empty is honest — not a green invent. */
export function shortHash(value?: string | null): string {
  const hash = String(value || "").trim();
  if (!hash) return "";
  if (hash.length <= 12) return hash;
  return `${hash.slice(0, 8)}…${hash.slice(-4)}`;
}

export type ValidateIdentityView = {
  pinned: boolean;
  shapeHash: string;
  decisionHash: string;
  ddlHash: string;
  shapeSteps: number;
  missing: string[];
};

/** Operator-facing Validate≡Execute stamps. Display only — never PATCH empties. */
export function formatValidateIdentitySummary(input: {
  shape_recipe?: unknown;
  approved_shape_recipe_hash?: string | null;
  approved_decision_artifact_hash?: string | null;
  approved_ddl_identity_hash?: string | null;
} | null | undefined): ValidateIdentityView {
  const shapeHash = String(input?.approved_shape_recipe_hash || "").trim();
  const decisionHash = String(input?.approved_decision_artifact_hash || "").trim();
  const ddlHash = String(input?.approved_ddl_identity_hash || "").trim();
  const recipe = input?.shape_recipe;
  const steps = recipe && typeof recipe === "object" && Array.isArray((recipe as { steps?: unknown }).steps)
    ? (recipe as { steps: unknown[] }).steps
    : [];
  const missing: string[] = [];
  if (!shapeHash) missing.push("shape");
  if (!decisionHash) missing.push("decision");
  if (!ddlHash) missing.push("ddl");
  return {
    pinned: Boolean(shapeHash || decisionHash || ddlHash),
    shapeHash,
    decisionHash,
    ddlHash,
    shapeSteps: steps.length,
    missing,
  };
}
