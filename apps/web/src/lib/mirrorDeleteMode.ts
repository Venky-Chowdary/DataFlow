/**
 * Mirror delete behaviour, worded once (owner decision, round 8).
 *
 * Soft delete is the default: destination rows whose key left the source are
 * kept and flagged `_deleted`. A hard delete is an explicit `delete_mode: "hard"`
 * on the stream contract and is announced before the run.
 *
 * These sentences are the engine's own (apps/api/services/mirror_delete_notice.py
 * and the mirror ledger note in row_conservation.py). An API test reads this
 * file and fails if a sentence drifts, so Studio, schedules, MCP and the job
 * ledger say the same thing.
 */

export type MirrorDeleteMode = "soft" | "hard";

export const MIRROR_SOFT_DELETE_TEXT =
  "Soft delete (default): destination rows whose key is missing from the source are kept and flagged _deleted (with _deleted_at). Physical COUNT(*) stays; the active population is COUNT(*) WHERE NOT _deleted.";

export const MIRROR_HARD_DELETE_WARNING =
  "Hard delete declared: destination rows whose key is missing from the source will be physically deleted (DELETE). They cannot be recovered from the destination. Remove delete_mode=hard to keep them as _deleted rows instead.";

/** What the finished run's ledger says happened to the missing keys. */
export const MIRROR_LEDGER_HARD_TEXT =
  "Keys missing from the source were physically deleted (hard delete declared).";

export const MIRROR_LEDGER_SOFT_TEXT =
  "Keys missing from the source stay as _deleted rows; physical COUNT(*) does not drop.";

export const MIRROR_HARD_DELETE_WARNING_ID = "mirror_hard_delete";

export function normalizeMirrorDeleteMode(value: unknown): MirrorDeleteMode {
  return String(value ?? "").trim().toLowerCase() === "hard" ? "hard" : "soft";
}

/** The delete mode stream contracts declare: hard when any mirror stream says so. */
export function contractsDeleteMode(contracts: unknown): MirrorDeleteMode {
  if (!Array.isArray(contracts)) return "soft";
  return contracts.some(
    (c) =>
      c
      && typeof c === "object"
      && (c as Record<string, unknown>).selected !== false
      && normalizeMirrorDeleteMode(
        (c as Record<string, unknown>).delete_mode ?? (c as Record<string, unknown>).delete_strategy,
      ) === "hard",
  )
    ? "hard"
    : "soft";
}

/** The plan-time sentence for a mode: the soft explanation or the hard-delete warning. */
export function mirrorDeleteText(mode: MirrorDeleteMode): string {
  return mode === "hard" ? MIRROR_HARD_DELETE_WARNING : MIRROR_SOFT_DELETE_TEXT;
}

/** The ledger sentence for a finished mirror run. */
export function mirrorLedgerText(mode: unknown): string {
  return normalizeMirrorDeleteMode(mode) === "hard" ? MIRROR_LEDGER_HARD_TEXT : MIRROR_LEDGER_SOFT_TEXT;
}

/** The hard-delete warning a Validate / preflight result carries, if any. */
export function preflightHardDeleteWarning(preflight: unknown): string | null {
  if (!preflight || typeof preflight !== "object") return null;
  const pf = preflight as Record<string, unknown>;
  const notice = pf.mirror_delete as Record<string, unknown> | undefined;
  if (notice && notice.physical_delete === true) {
    return String(notice.message || MIRROR_HARD_DELETE_WARNING);
  }
  const warnings = Array.isArray(pf.warnings) ? pf.warnings : [];
  for (const w of warnings) {
    if (w && typeof w === "object" && (w as Record<string, unknown>).id === MIRROR_HARD_DELETE_WARNING_ID) {
      return String((w as Record<string, unknown>).message || MIRROR_HARD_DELETE_WARNING);
    }
  }
  return null;
}
