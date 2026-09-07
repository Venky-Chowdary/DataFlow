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
 *
 * A declared reduction is the exception: it is a decision about the source
 * field itself, so it survives regeneration by source name together with its
 * G16 evidence. So is a hand-picked destination carrier: the operator chose it
 * for that column, and re-deriving the inferred type over it discarded the
 * choice on the way back from Validate.
 */

import {
  CONTINUE_EXECUTION_POLICIES,
  acknowledgeMappingRisk,
  applyDestTypeChange,
  applyTransformChange,
  isIntentionalOmit,
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
  const omitted = new Map<string, EditableMapping>();
  const crosswalks = new Map<string, { table: Record<string, string>; system?: string }>();
  const declaredTypes = new Map<string, string>();
  for (const p of prior) {
    if (isIntentionalOmit(p)) omitted.set(p.source, p);
    if (p.destTypeDeclared && !isIntentionalOmit(p)) {
      declaredTypes.set(p.source, p.destTypeDeclared);
    }
    if (p.codeCrosswalk && Object.keys(p.codeCrosswalk).length && !isIntentionalOmit(p)) {
      crosswalks.set(p.source, { table: p.codeCrosswalk, system: p.codeCrosswalkSystem });
    }
    if (!p.approved && !p.riskAcknowledged) continue;
    decided.set(mappingDecisionFingerprint(p), p);
  }
  if (
    !decided.size
    && !omitted.size
    && !crosswalks.size
    && !declaredTypes.size
    && !prior.some((p) => typeof p.controlTotal === "boolean")
  ) {
    return next;
  }
  const controlTotals = new Map<string, boolean>();
  for (const p of prior) {
    if (typeof p.controlTotal === "boolean") controlTotals.set(p.source, p.controlTotal);
  }
  return next.map((m) => {
    const dropped = omitted.get(m.source);
    if (dropped) return carryReduction(m, dropped);
    // Replay the declaration before the fingerprint is read: an acknowledgement
    // signed for the declared carrier belongs to the row that carries it, and
    // fingerprinting the inferred type first would discard that signature too.
    const declared = declaredTypes.get(m.source);
    const restored = declared && declared !== m.destTypeDeclared
      ? applyDestTypeChange(m, declared)
      : m;
    const hit = decided.get(mappingDecisionFingerprint(restored));
    const priorWalk = crosswalks.get(m.source);
    let nextRow = restored;
    if (priorWalk && !isIntentionalOmit(m)) {
      nextRow = { ...nextRow, codeCrosswalk: priorWalk.table, codeCrosswalkSystem: priorWalk.system };
    }
    const controlTotal = controlTotals.has(m.source) ? controlTotals.get(m.source) : nextRow.controlTotal;
    if (!hit && controlTotal === nextRow.controlTotal) return nextRow;

    return {
      ...nextRow,
      ...(hit
        ? {
            approved: hit.approved,
            requiresReview: hit.requiresReview,
            riskAcknowledged: hit.riskAcknowledged,
            riskContract: hit.riskContract,
          }
        : {}),
      controlTotal,
    };
  });
}

/**
 * Replay a declared reduction onto a regenerated row.
 *
 * An omission is a decision about the source field, not about a destination
 * type path, so it survives regeneration by source name — unlike a risk
 * acknowledgement, which is scoped to the type facts it was signed for. It is
 * carried outside the fingerprint on purpose: regeneration proposes a target
 * for the column again, which changes the fingerprint, and dropping the
 * decision there silently deleted the reduction reason, the archive reference
 * and the named accepter the operator had recorded for G16.
 */
function carryReduction(m: EditableMapping, prior: EditableMapping): EditableMapping {
  return {
    ...applyTransformChange(m, "omit"),
    omitReason: prior.omitReason,
    omitReasonText: prior.omitReasonText,
    archiveReference: prior.archiveReference,
    retentionUntil: prior.retentionUntil,
    omitApprovedBy: prior.omitApprovedBy,
  };
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
