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
  assumeTimezoneAwaitingZone,
  EXECUTION_POLICY_OPTIONS,
  classifyMappingReview,
  isExistingDestTypeOverride,
  isExistingEnumBooleanConflict,
  isDestSchemaPending,
  isFalseFriendReview,
  isIntentionalOmit,
  mappingRequiresRiskAck,
  mappingReviewKindMeta,
  type EditableMapping,
  type ExecutionPolicy,
} from "./mapping";

export type MapBlockerCode =
  | "no_destination"
  | "dest_schema_unloaded"
  | "assume_timezone_zone_missing"
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

/**
 * Source → destination types as measured.
 *
 * The destination side is never back-filled from the source: printing
 * ``VARCHAR(16777216) → VARCHAR(16777216)`` for a column whose destination type
 * was never read invents the fact the message is supposed to report.
 */
function typePath(m: EditableMapping): string {
  const dest = String(m.destType || "").trim();
  return `${m.inferredType || "unknown"} → ${dest || "destination type not loaded"}`;
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

  // One root cause, before any per-column type talk: the destination schema is
  // unknown, so no type verdict on this row is measurable and no Map action —
  // Approve, Widen, or a signed Risk Contract — can make it measurable.
  if (isDestSchemaPending(m)) {
    return {
      code: "dest_schema_unloaded",
      source: m.source,
      title: `${m.source} → ${m.target || m.source} has no destination type yet`,
      action:
        "Reload the destination schema. If the table does not exist yet, the probe "
        + "proves it absent and the column becomes a CREATE — Datawrap will not "
        + "guess the destination type from the source.",
      clearableFromMap: false,
    };
  }

  // The operator chose to declare the source zone but has not named one. The
  // engine cannot pick a default: inventing UTC is the loss this control exists
  // to prevent.
  if (assumeTimezoneAwaitingZone(m)) {
    return {
      code: "assume_timezone_zone_missing",
      source: m.source,
      title: `${m.source} declares a source zone but none is named (${typePath(m)})`,
      action:
        "Type the IANA zone the source recorded these timestamps in "
        + "(e.g. UTC, Europe/Berlin) in the transform cell.",
      clearableFromMap: true,
    };
  }

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
  /**
   * One line per distinct cause with the columns it covers, so ten columns
   * sharing one root cause read as one line instead of ten paragraphs that
   * push the mapping grid off screen.
   */
  groups: MapBlockerGroup[];
  /** True when every blocker is the destination schema not being loaded. */
  destSchemaUnloadedOnly: boolean;
}

export interface MapBlockerGroup {
  code: MapBlockerCode;
  /** Columns held by this cause, in mapping order. */
  columns: string[];
  /** Cause, stated once for the whole group. */
  title: string;
  /** The action that clears the whole group. */
  action: string;
  clearableFromMap: boolean;
}

const DETAIL_LIMIT = 3;
const GROUP_COLUMN_LIMIT = 6;

const GROUP_TITLES: Partial<Record<MapBlockerCode, string>> = {
  dest_schema_unloaded: "destination column type not read yet",
  assume_timezone_zone_missing: "source zone declared but not named",
  no_destination: "no destination column",
  risk_ack_required: "lossy type path needs a signed Risk Contract",
  approval_required: "not approved yet",
  existing_dest_type_override: "changes an existing destination column type",
  existing_enum_boolean: "existing destination domain conflict",
  false_friend: "name matches but meaning does not",
  fail_closed_contract: "signed contract stops the write by design",
};

/** Collapse per-column blockers into one line per cause. */
export function groupMapBlockers(blockers: MapBlocker[]): MapBlockerGroup[] {
  const byCode = new Map<MapBlockerCode, MapBlocker[]>();
  for (const b of blockers) {
    const list = byCode.get(b.code);
    if (list) list.push(b);
    else byCode.set(b.code, [b]);
  }
  return [...byCode.entries()].map(([code, list]) => {
    const columns = list.map((b) => b.source);
    const shown = columns.slice(0, GROUP_COLUMN_LIMIT).join(", ");
    const rest = columns.length - GROUP_COLUMN_LIMIT;
    const cause = GROUP_TITLES[code];
    const title = list.length === 1 || !cause
      ? list[0].title
      : `${list.length} column(s) — ${cause}: ${shown}${rest > 0 ? ` +${rest} more` : ""}`;
    return {
      code,
      columns,
      title,
      action: list[0].action,
      clearableFromMap: list.every((b) => b.clearableFromMap),
    };
  });
}

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
      groups: [],
      destSchemaUnloadedOnly: false,
    };
  }
  const clearableFromMap = blockers.filter((b) => b.clearableFromMap).length;
  const needsDdl = blockers.length - clearableFromMap;
  const groups = groupMapBlockers(blockers);
  const destSchemaUnloadedOnly = blockers.every(
    (b) => b.code === "dest_schema_unloaded",
  );
  const headline = destSchemaUnloadedOnly
    ? `Destination schema not loaded — ${blockers.length} column(s) have no destination type yet`
    : needsDdl > 0
      ? `${blockers.length} column(s) hold Validate · ${needsDdl} need a destination change`
      : `${blockers.length} column(s) hold Validate`;
  const detail = [
    ...groups.slice(0, DETAIL_LIMIT).map((g) => `${g.title} — ${g.action}`),
    groups.length > DETAIL_LIMIT
      ? `+${groups.length - DETAIL_LIMIT} more cause(s) in the Review filter.`
      : "",
  ].filter(Boolean).join("\n");
  return { blockers, clearableFromMap, headline, detail, groups, destSchemaUnloadedOnly };
}
