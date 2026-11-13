import { buildPreflightMappings, editableFromPipelineMappings, type EditableMapping } from "./mapping";
import type { PlanMappingRow } from "./planConcurrency";

/**
 * The Transfer Studio's side of a plan conflict, kept out of TransferPage so
 * it can be tested: what "mine" is, how the latest revision becomes editor
 * rows, when Approve may run, and when the Validate-panel note is cleared.
 */

/**
 * The user's mappings exactly as a save would send them: the same
 * `buildPreflightMappings` Validate uses, so omits stay omits (target "",
 * intentional_omit and their reasons), transforms, overrides and risk
 * contracts travel, and "Keep mine" saves what the editor shows.
 */
export function conflictMineFromEditor(editor: EditableMapping[]): PlanMappingRow[] {
  return buildPreflightMappings([], editor) as unknown as PlanMappingRow[];
}

export interface EditorContext {
  sampleRows?: Record<string, unknown>[];
  destColumns?: string[];
  threshold: number;
  destSchema?: Record<string, string>;
}

/**
 * Editor rows for "Use latest", keeping the operator decisions the revision
 * records: an approved row (user_override, or an explicit operator_target)
 * stays approved and is marked operatorTarget, so carryOperatorDecisions
 * keeps it on the next regeneration instead of the mapper re-proposing it;
 * an intentional omit stays an omit with its reasons.
 */
export function editorFromLatestRevision(rows: PlanMappingRow[], ctx: EditorContext): EditableMapping[] {
  const base = editableFromPipelineMappings(
    rows.map((m) => {
      const omitted = Boolean(m.intentional_omit) || m.transform === "omit";
      return {
        ...(m as Record<string, unknown>),
        source: m.source,
        target: omitted ? (m.target || m.source) : m.target,
        confidence: typeof m.confidence === "number" ? m.confidence : 1,
      } as Parameters<typeof editableFromPipelineMappings>[0][number];
    }),
    ctx.sampleRows,
    ctx.destColumns,
    ctx.threshold,
    ctx.destSchema,
  );
  return base.map((row, i) => {
    const wire = rows[i];
    const omitted = Boolean(wire.intentional_omit) || wire.transform === "omit";
    if (omitted) {
      return {
        ...row,
        transform: "omit",
        approved: true,
        intentionalOmit: true,
        omitReason: (wire.omit_reason as EditableMapping["omitReason"]) || row.omitReason,
        omitReasonText: wire.omit_reason_text || row.omitReasonText,
      } as EditableMapping;
    }
    const decided = Boolean(wire.user_override) || Boolean(wire.operator_target);
    return decided
      ? { ...row, approved: true, operatorTarget: true, riskAcknowledged: Boolean(wire.risk_acknowledged) || row.riskAcknowledged }
      : row;
  });
}

/**
 * Approve only the revision this page saved and Validate checked. `checked`
 * is Validate's `mapping_version`; `saved` is the head this page's own sync
 * returned. Anything else (another user's save landed before Validate read
 * the plan) must not be approved or stamped as the on-screen mappings.
 */
export function approveDecision(checked: unknown, saved: number | null): { approve: boolean; expectedVersion: number | null } {
  if (typeof checked !== "number" || saved == null || checked !== saved) {
    return { approve: false, expectedVersion: null };
  }
  return { approve: true, expectedVersion: checked };
}

/** Events that decide whether the Validate panel's conflict note still applies. */
export type PlanConflictNoteEvent =
  | { kind: "conflict"; note: string }
  | { kind: "validate-started" }
  | { kind: "validate-succeeded" }
  | { kind: "plan-changed" }
  | { kind: "resolved" };

export function planConflictNoteReducer(note: string, event: PlanConflictNoteEvent): string {
  switch (event.kind) {
    case "conflict":
      return event.note;
    case "validate-started":
    case "validate-succeeded":
    case "plan-changed":
    case "resolved":
      return "";
    default:
      return note;
  }
}
