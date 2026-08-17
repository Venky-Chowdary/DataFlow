/**
 * Why Map refuses to continue — per row, in operator words, with the exit.
 *
 * A count ("1 column(s) need Approve or Accept risk") is not an explanation: on
 * a row where Approve is refused by design and a signed contract does not clear
 * it, that count sends the operator round a loop. Every blocker here names the
 * column, the destination-side reason, and the action that actually clears it —
 * including "you cannot clear this from Map" when that is the truth.
 *
 * The blocker set is derived from the same predicate that gates the Continue
 * button, so the reason shown and the reason enforced cannot drift.
 */
import { needsMappingReview } from "./columnWorkbench";
import {
  CONTINUE_EXECUTION_POLICIES,
  EXECUTION_POLICY_OPTIONS,
  classifyMappingReview,
  isExistingDestTypeOverride,
  isExistingEnumBooleanConflict,
  isFalseFriendReview,
  isIntentionalOmit,
  mappingRequiresRiskAck,
  mappingReviewKindMeta,
  type EditableMapping,
  type ExecutionPolicy,
} from "./mapping";

export type MapBlockerCode =
  | "no_destination"
  | "existing_dest_type_override"
  | "existing_enum_boolean"
  | "false_friend"
  | "fail_closed_contract"
  | "risk_ack_required"
  | "approval_required";

export interface MapBlocker {
  code: MapBlockerCode;
  source: string;
  /** One line naming the column and the types involved. */
  title: string;
  /** What clears it. Never "Approve" when Approve is refused by design. */
  action: string;
  /** False when no Map-only action clears it (needs ALTER or a remap). */
  clearableFromMap: boolean;
}

const CONTINUE_POLICY_NAMES = EXECUTION_POLICY_OPTIONS
  .filter((p) => p.continueUnlock)
  .map((p) => p.id)
  .join(" / ");

function typePath(m: EditableMapping): string {
  return `${m.inferredType || "unknown"} → ${m.destType || m.inferredType || "unknown"}`;
}

/** Execution policy on the row's contract, when one is stamped. */
export function contractExecutionPolicy(m: EditableMapping): ExecutionPolicy | null {
  const policy = String(m.riskContract?.execution_policy || "").toUpperCase();
  return EXECUTION_POLICY_OPTIONS.some((p) => p.id === policy)
    ? (policy as ExecutionPolicy)
    : null;
}

/** The single reason this row holds Validate, or null when it does not. */
export function mappingBlocker(
  m: EditableMapping,
  threshold: number,
): MapBlocker | null {
  if (!needsMappingReview(m, threshold)) return null;

  if (isIntentionalOmit(m)) {
    return {
      code: "approval_required",
      source: m.source,
      title: `${m.source} is marked omit and not written`,
      action: "Approve the omit to confirm the column is intentionally excluded.",
      clearableFromMap: true,
    };
  }

  if (!String(m.target || "").trim()) {
    return {
      code: "no_destination",
      source: m.source,
      title: `${m.source} has no destination column`,
      action: "Type a destination column name, or set Transform → Omit to exclude it.",
      clearableFromMap: true,
    };
  }

  if (isExistingEnumBooleanConflict(m)) {
    return {
      code: "existing_enum_boolean",
      source: m.source,
      title: `${m.source} carries status labels into an existing BOOLEAN column (${typePath(m)})`,
      action:
        "Remap to a VARCHAR column or ALTER the destination — Map Widen cannot change existing DDL.",
      clearableFromMap: false,
    };
  }

  if (isExistingDestTypeOverride(m)) {
    return {
      code: "existing_dest_type_override",
      source: m.source,
      title: `${m.source} asks for a type change on an existing physical column (stays ${m.destType || "as-is"})`,
      action: `Remap to a compatible column, ALTER the destination, or set the type select back to ${m.destType || "the live type"} to withdraw the ALTER request.`,
      clearableFromMap: false,
    };
  }

  const policy = contractExecutionPolicy(m);
  if (m.riskAcknowledged && policy && !CONTINUE_EXECUTION_POLICIES.has(policy)) {
    return {
      code: "fail_closed_contract",
      source: m.source,
      title: `${m.source} contract is signed with ${policy}, which stops the write by design`,
      action: `Re-sign with a continue policy (${CONTINUE_POLICY_NAMES}) to run with the loss recorded, or fix the type path.`,
      clearableFromMap: true,
    };
  }

  if (isFalseFriendReview(m) && !m.falseFriendConfirmed) {
    const meta = mappingReviewKindMeta(classifyMappingReview(m) || "generic");
    return {
      code: "false_friend",
      source: m.source,
      title: `${m.source} → ${m.target} — ${meta.noun}`,
      action: meta.detail,
      clearableFromMap: true,
    };
  }

  if (mappingRequiresRiskAck(m) && !m.riskAcknowledged) {
    return {
      code: "risk_ack_required",
      source: m.source,
      title: `${m.source} loses fidelity (${typePath(m)})${m.fidelityReason ? ` — ${m.fidelityReason}` : ""}`,
      action: "Choose an execution policy on the row, then Sign Risk Contract.",
      clearableFromMap: true,
    };
  }

  return {
    code: "approval_required",
    source: m.source,
    title: `${m.source} → ${m.target} is not approved yet (${typePath(m)})`,
    action: "Approve the row, or edit the destination column first.",
    clearableFromMap: true,
  };
}

export function mapBlockers(
  mappings: EditableMapping[],
  threshold: number,
): MapBlocker[] {
  return mappings
    .map((m) => mappingBlocker(m, threshold))
    .filter((b): b is MapBlocker => b !== null);
}

export interface MapBlockerSummary {
  blockers: MapBlocker[];
  /** Rows the operator can clear without touching destination DDL. */
  clearableFromMap: number;
  headline: string;
  /** Multi-line reason + action, first blockers named. Empty when clear. */
  detail: string;
}

const DETAIL_LIMIT = 3;

export function mapBlockerSummary(
  mappings: EditableMapping[],
  threshold: number,
): MapBlockerSummary {
  const blockers = mapBlockers(mappings, threshold);
  if (blockers.length === 0) {
    return {
      blockers,
      clearableFromMap: 0,
      headline: "Continue to Validate",
      detail: "",
    };
  }
  const clearableFromMap = blockers.filter((b) => b.clearableFromMap).length;
  const needsDdl = blockers.length - clearableFromMap;
  const headline = needsDdl > 0
    ? `${blockers.length} column(s) hold Validate · ${needsDdl} need a destination change`
    : `${blockers.length} column(s) hold Validate`;
  const detail = [
    ...blockers.slice(0, DETAIL_LIMIT).map((b) => `${b.title} — ${b.action}`),
    blockers.length > DETAIL_LIMIT
      ? `+${blockers.length - DETAIL_LIMIT} more in the Review filter.`
      : "",
  ].filter(Boolean).join("\n");
  return { blockers, clearableFromMap, headline, detail };
}
