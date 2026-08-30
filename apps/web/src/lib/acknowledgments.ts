/**
 * One owner for the operator attestation text sent with Validate and Execute.
 *
 * An acknowledgment only counts when it carries who accepted the risk and why,
 * so a sticky acknowledgment re-sent on a later run must restate its standing
 * reason rather than arrive empty and be refused by the API.
 */
export type AcknowledgmentFlags = {
  compliance?: boolean;
  schemaDrift?: boolean;
  fkRisk?: boolean;
};

export function standingAcknowledgmentReason(flags: AcknowledgmentFlags): string {
  return [
    flags.compliance ? "PII/compliance acknowledged on Validate" : "",
    flags.schemaDrift ? "Schema drift acknowledged on Validate" : "",
    flags.fkRisk ? "FK risk acknowledged on Validate" : "",
  ]
    .filter(Boolean)
    .join("; ");
}
