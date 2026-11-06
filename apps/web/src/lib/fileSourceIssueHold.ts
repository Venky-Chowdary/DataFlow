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
}

/** Modes whose run holds a bad cell's row out instead of refusing the load. */
const HOLDS_OUT = new Set(["balanced", "lenient"]);

export function fileSourceIssueHold(
  validation: CsvValidationReport | null | undefined,
  validationMode: string,
): FileSourceIssueHold | null {
  if (!validation || validation.ok) return null;
  if (HOLDS_OUT.has(String(validationMode || "").toLowerCase())) return null;
  const issueCount = Number(validation.issue_count) || validation.issues?.length || 0;
  const issues = (validation.issues || []).filter(Boolean).slice(0, 5);
  return {
    title: `${issueCount.toLocaleString()} value(s) in the file do not fit their column`,
    message:
      "Strict validation refuses the whole file for them. Switch to Balanced to hold exactly "
      + "these rows out in quarantine, each with its reason, and load the rest — or fix the "
      + "file and upload it again.",
    issues,
    issueCount,
  };
}
