/**
 * Named data / migration rules on a Pilot plan or Confirm preview.
 *
 * Never invents skip_preflight or propagate_all. Empty means the operator
 * did not speak a posture and the engine default stands.
 */

export type NamedDataRules = {
  validationMode: string;
  schemaPolicy: string;
};

export function namedDataRules(
  raw: { validation_mode?: string; schema_policy?: string } | null | undefined,
): NamedDataRules {
  return {
    validationMode: String(raw?.validation_mode || "").trim(),
    schemaPolicy: String(raw?.schema_policy || "").trim(),
  };
}

export function mergeNamedDataRules(
  ...records: Array<{ validation_mode?: string; schema_policy?: string } | null | undefined>
): NamedDataRules {
  let validationMode = "";
  let schemaPolicy = "";
  for (const rec of records) {
    const next = namedDataRules(rec);
    if (next.validationMode) validationMode = next.validationMode;
    if (next.schemaPolicy) schemaPolicy = next.schemaPolicy;
  }
  return { validationMode, schemaPolicy };
}
