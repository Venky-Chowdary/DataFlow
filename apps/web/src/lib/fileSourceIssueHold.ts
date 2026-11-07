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
  /** Studio's mode that holds every issue out ("" = none): the API decides (review B4). */
  holdoutMode: "balanced" | "";
  canHoldOut: boolean;
}

/**
 * Modes whose run holds a refused cell's row out instead of refusing the load —
 * the API's one policy (services/validation_mode_contract.mode_holds_out_refusal):
 * lenient holds out every per-cell cast refusal; balanced holds out a
 * non-existent date or time on a file. Neither is sent back to Source. (Before
 * round 14 Studio sent lenient back to Source too, although lenient's own gates
 * pass these cells — the two disagreed.)
 */
const HOLDS_OUT = new Set(["balanced", "lenient"]);

/** The remedy, worded as the API's explain action words it (one remedy, not two). */
export function holdOutLabel(mode: "balanced"): string {
  const name = mode === "balanced" ? "Balanced" : mode;
  return `Hold out these rows in quarantine (switches the whole run to ${name} validation) and load the rest`;
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
  // The API's per-report answer (csv_validator.holdout_modes), never a second rule here.
  const modes = validation.holdout_modes || {};
  // Studio offers the modes it runs; Lenient is an API / MCP mode.
  const holdoutMode: "balanced" | "" = modes.balanced ? "balanced" : "";
  const canHoldOut = holdoutMode !== "";
  return {
    title: `${issueCount.toLocaleString()} value(s) in the file do not fit their column`,
    message: canHoldOut
      ? "Strict validation refuses the whole file for them. Balanced validation holds exactly "
        + "these rows out in quarantine, each with its reason, and loads the rest — or fix the "
        + "file and upload it again."
      : modes.lenient
        ? "Strict validation refuses the whole file for them, and Balanced does not hold these "
          + "values out (only Lenient validation, from the API or MCP, does). Fix the file and "
          + "upload it again, or remap the column."
        : "Strict validation refuses the whole file for them, and no validation mode holds every "
          + "one of these values out. Fix the file and upload it again, or remap the column.",
    issues,
    issueCount,
    holdoutMode,
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
