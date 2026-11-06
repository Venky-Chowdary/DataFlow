/**
 * QA browser 3: a CSV value that does not fit its column (row 642,
 * `31/02/2025`) sent Studio back to Source — the operator lost the Validate
 * context and had no way to load the good rows. The decision belongs on
 * Validate: hold the bad rows out in quarantine (Balanced — each row keeps its
 * reason, the rest load), or go and fix the file.
 */
import type { CsvValidationReport } from "./types";

export interface FileSourceIssueHold {
  title: string;
  message: string;
  issues: string[];
  issueCount: number;
  /** Balanced can hold every one of these out (numeric / calendar refusals). */
  canHoldOut: boolean;
}

/**
 * Modes whose run holds a refused cell's row out instead of refusing the load —
 * the API's one policy (services/validation_mode_contract.mode_holds_out_refusal):
 * lenient holds out every per-cell cast refusal; balanced holds out numeric and
 * invalid-calendar refusals on a file. Neither is sent back to Source. (Before
 * round 14 Studio sent lenient back to Source too, although lenient's own gates
 * pass these cells — the two disagreed.)
 */
const HOLDS_OUT = new Set(["balanced", "lenient"]);

/** The remedy, worded as the API's explain action words it (one remedy, not two). */
export const HOLD_OUT_LABEL = "Hold out these rows in quarantine (Balanced validation) and load the rest";

/** A refusal Balanced holds out on a file: a bad number or a non-existent date/time. */
export function isBalancedHoldoutIssue(issue: string): boolean {
  return /invalid (?:integer|decimal|number)\b/i.test(issue) || isCalendarHoldoutFinding(issue);
}

export function fileSourceIssueHold(
  validation: CsvValidationReport | null | undefined,
  validationMode: string,
): FileSourceIssueHold | null {
  if (!validation || validation.ok) return null;
  if (HOLDS_OUT.has(String(validationMode || "").toLowerCase())) return null;
  const listed = (validation.issues || []).filter(Boolean);
  const issueCount = Number(validation.issue_count) || listed.length || 0;
  const issues = listed.slice(0, 5);
  // Only when every issue is one Balanced holds out — and every issue is listed,
  // so none we cannot see is a different kind.
  const canHoldOut = listed.length > 0 && listed.length >= issueCount && listed.every(isBalancedHoldoutIssue);
  return {
    title: `${issueCount.toLocaleString()} value(s) in the file do not fit their column`,
    message: canHoldOut
      ? "Strict validation refuses the whole file for them. Balanced validation holds exactly "
        + "these rows out in quarantine, each with its reason, and loads the rest — or fix the "
        + "file and upload it again."
      : "Strict validation refuses the whole file for them, and holding the rows out does not "
        + "apply to every one of these values. Fix the file and upload it again, or remap the column.",
    issues,
    issueCount,
    canHoldOut,
  };
}

/**
 * A finding the write path holds out row by row: a value naming no real
 * calendar day (the API's ``invalid_calendar_reason`` wording). Quarantine is
 * the remedy — the column's type is right, so "Remap columns" is the loop QA
 * hit (QA browser 3).
 */
export function isCalendarHoldoutFinding(text: string): boolean {
  return /not a real calendar date|is not a real (?:calendar )?(?:date|clock)/i.test(String(text || ""));
}
