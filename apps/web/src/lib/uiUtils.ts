/** Shared UI helpers — single source for status badges and formatting */

/** Success terminal statuses (data landed in the destination). */
export const JOB_SUCCESS_STATUSES = ["completed", "completed_with_quarantine", "completed_with_warnings", "success", "succeeded"] as const;

/** True when the job succeeded — includes the success-with-warnings state. */
export function isJobSuccess(status: string | undefined): boolean {
  return (
    status === "completed"
    || status === "completed_with_quarantine"
    || status === "completed_with_warnings"
    || status === "success"
    || status === "succeeded"
  );
}

/** True when the job will no longer emit progress (success, failure, or cancel). */
export function isJobTerminal(status: string | undefined): boolean {
  return (
    status === "completed"
    || status === "completed_with_quarantine"
    || status === "completed_with_warnings"
    || status === "success"
    || status === "succeeded"
    || status === "failed"
    || status === "cancelled"
  );
}

/**
 * Parked on an operator decision or a fix: the job is not finished, but nothing
 * runs until someone acts, so its clock must not keep counting. Mirrors the
 * API's hold statuses (services/mongodb_service.py).
 */
export function isJobParked(status: string | undefined): boolean {
  return (
    status === "needs_approval"
    || status === "needs_confirmation"
    || status === "needs_review"
    || status === "awaiting_approval"
    || status === "paused"
    || status === "blocked"
  );
}

/**
 * Status the operator should read. A cancel flag wins over a late
 * ``completed`` write: a canceled run must not wear the green success badge.
 */
export function presentedJobStatus(job: { status?: string; cancel_requested?: boolean } | null | undefined): string {
  const status = String(job?.status || "");
  if (job?.cancel_requested || status === "cancelled" || status === "canceled") return "cancelled";
  return status;
}

/** Human-readable label for a job status (never surface the raw enum). */
export function jobStatusLabel(status: string): string {
  switch (status) {
    case "completed":
    case "success":
    case "succeeded":
      return "Completed";
    case "completed_with_quarantine":
      return "Completed with quarantine";
    case "completed_with_warnings":
      return "Completed with warnings";
    case "failed":
      return "Failed";
    case "running":
      return "Running";
    case "pending":
      return "Pending";
    case "cancelled":
      return "Cancelled";
    case "needs_approval":
      return "Needs approval";
    default:
      return status ? status.charAt(0).toUpperCase() + status.slice(1).replace(/_/g, " ") : "Unknown";
  }
}

export function jobStatusBadgeClass(status: string): string {
  if (status === "completed") return "df2-badge df2-badge-live";
  // Success-with-warnings: amber, distinct from clean green and red failure.
  if (status === "completed_with_quarantine" || status === "completed_with_warnings") return "df2-badge df2-badge-warn";
  if (status === "failed") return "df2-badge df2-badge-error";
  if (status === "running" || status === "pending") return "df2-badge df2-badge-run";
  // Parked on a decision: not a failure, but nothing moves until someone answers.
  if (status === "needs_approval") return "df2-badge df2-badge-warn";
  return "df2-badge df2-badge-muted";
}

/** Stable React list key when identities can collide or be missing. */
export function uniqueListKey(
  id: string | number | null | undefined,
  index: number,
  prefix = "row",
): string {
  const raw = String(id ?? "").trim();
  return raw ? `${prefix}:${raw}:${index}` : `${prefix}:${index}`;
}

export function connectorHealthLabel(status: string, lastTestOk?: boolean): string {
  // Probe result wins — a green last_test_ok must never read as "Action needed"
  // just because a stale status string still says error.
  if (lastTestOk === true) return "Ready";
  if (lastTestOk === false || status === "error") return "Action needed";
  return "Ready";
}
