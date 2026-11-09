/**
 * Jobs list selection — persist the operator pick across list polls.
 * Deep-link focus applies once per job id; polling must not steal it.
 */

export function shouldApplyInitialJobFocus(
  initialJobId: string | undefined,
  lastApplied: string | null,
  jobIds: string[],
): boolean {
  if (!initialJobId) return false;
  if (lastApplied === initialJobId) return false;
  return jobIds.includes(initialJobId);
}

/** True while a ``#/jobs?jobId=`` focus has not been applied to the list yet. */
export function deepLinkPending(initialJobId: string | undefined, lastApplied: string | null): boolean {
  return Boolean(initialJobId) && lastApplied !== initialJobId;
}

export interface JobDeepLink {
  jobId?: string;
  panel?: string;
}

/**
 * The deep link the Jobs page is still honouring. The shell's focus prop is
 * transient (cleared ~800ms after navigate) and can vanish before the job list
 * loads, so the last requested id is latched until a new one arrives.
 */
export function latchDeepLink(prev: JobDeepLink, jobId: string | undefined, panel: string | undefined): JobDeepLink {
  if (!jobId || jobId === prev.jobId) return prev;
  return { jobId, panel };
}

/** Keep the current pick when it is still in the filtered list. */
export function nextListSelection(
  selectedId: string | null,
  filteredIds: string[],
): string | null {
  if (filteredIds.length === 0) return selectedId;
  if (selectedId && filteredIds.includes(selectedId)) return selectedId;
  return filteredIds[0] ?? null;
}

/** Where the selected job's full record read stands. List rows are slim — no reconciliation, preflight, or proof. */
export type JobDetailRead = "pending" | "read" | "failed";

/** One retry before a failed job-record read is reported. */
export const JOB_DETAIL_RETRY_MS = 1500;

/**
 * Status for an evidence panel the shown job does not carry. Absence only
 * means "Not captured" once the full record was read; before that the list
 * row simply never carried it (QA: Gate-8 / Validate "Not captured" until Refresh).
 */
export function absentEvidenceLabel(read: JobDetailRead): string {
  if (read === "read") return "Not captured";
  return read === "pending" ? "Loading…" : "Detail unavailable — Refresh";
}
