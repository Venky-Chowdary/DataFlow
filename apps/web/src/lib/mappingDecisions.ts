/**
 * Operator decision durability across mapping regeneration.
 *
 * Map re-derives mappings whenever the step or destination schema changes. That
 * regeneration used to overwrite `approved` / `riskAcknowledged` / `riskContract`,
 * so an operator who went back to Map to clear a Validate blocker lost every
 * approval and could never reach Execute.
 *
 * An approval is scoped to the facts it was given for. Decisions are carried
 * forward only while the decision fingerprint is unchanged; if the type path,
 * fidelity verdict, transform, or create-new posture moved, the row must be
 * decided again rather than inheriting a stale signature.
 */

import {
  CONTINUE_EXECUTION_POLICIES,
  acknowledgeMappingRisk,
  mappingRequiresRiskAck,
  type EditableMapping,
  type ExecutionPolicy,
} from "./mapping";

/** Facts an operator decision is scoped to — any change invalidates the ack. */
export function mappingDecisionFingerprint(m: EditableMapping): string {
  return [
    m.source,
    m.target || m.source,
    (m.inferredType || "").toUpperCase(),
    (m.destType || "").toUpperCase(),
    (m.fidelity || "").toLowerCase(),
    m.transform || "none",
    m.structPolicy || "",
    m.typeNarrowing ? "narrow" : "",
    m.createNew ? "create" : "bind",
    (m.createNewRisks || [])
      .map((r) => `${r.kind}:${r.severity ?? ""}`)
      .sort()
      .join(","),
  ].join("\u0000");
}

/**
 * Re-apply prior approvals / signed Risk Contracts onto regenerated mappings.
 *
 * Only rows whose fingerprint is byte-identical inherit a decision, so a
 * re-derived mapping with different type facts is never silently auto-approved.
 */
export function carryOperatorDecisions(
  next: EditableMapping[],
  prior: EditableMapping[] | undefined | null,
): EditableMapping[] {
  if (!prior?.length || !next.length) return next;
  const decided = new Map<string, EditableMapping>();
  for (const p of prior) {
    if (!p.approved && !p.riskAcknowledged) continue;
    decided.set(mappingDecisionFingerprint(p), p);
  }
  if (!decided.size) return next;
  return next.map((m) => {
    const hit = decided.get(mappingDecisionFingerprint(m));
    if (!hit) return m;
    return {
      ...m,
      approved: hit.approved,
      requiresReview: hit.requiresReview,
      riskAcknowledged: hit.riskAcknowledged,
      riskContract: hit.riskContract,
    };
  });
}

/** Rows whose risk is value-dependent hold out the offending row; the rest
 *  quarantine only on an actual cast failure. Both keep bad data out of the
 *  destination and out of the silent-drop path. */
export function holdoutPolicyFor(m: EditableMapping): ExecutionPolicy {
  const fidelity = (m.fidelity || "").toLowerCase();
  if (fidelity === "cast" || fidelity === "lossy_cast" || m.typeNarrowing) {
    return "QUARANTINE_ROW";
  }
  return "CAST_AND_CONTINUE";
}

export interface HoldoutResult {
  mappings: EditableMapping[];
  /** `source → target` labels that were signed. */
  signed: string[];
}

/**
 * Validate's forward door: sign a holdout Risk Contract for every row that is
 * blocking on unacknowledged fidelity risk, without leaving Validate.
 *
 * This does not make the risk disappear — it records an auditable contract that
 * failing rows go to the DLQ for replay instead of being written lossily.
 */
export function holdOutRowsAndContinue(
  mappings: EditableMapping[],
  opts?: {
    approvedBy?: string;
    reason?: string;
    rowsSampled?: number;
    estimatedRows?: number | null;
    planId?: string;
    table?: string;
  },
): HoldoutResult {
  const signed: string[] = [];
  const out = mappings.map((m) => {
    if (!mappingRequiresRiskAck(m)) return m;
    const policy = String(m.riskContract?.execution_policy || "") as ExecutionPolicy;
    if (m.riskAcknowledged && m.riskContract && CONTINUE_EXECUTION_POLICIES.has(policy)) {
      return m;
    }
    signed.push(m.target ? `${m.source} → ${m.target}` : m.source);
    return acknowledgeMappingRisk(m, {
      ...opts,
      executionPolicy: holdoutPolicyFor(m),
      reason:
        opts?.reason
        || "Operator chose to run with failing rows held out in quarantine for replay",
    });
  });
  return { mappings: out, signed };
}
