/** Shared UI helpers — single source for status badges and formatting */

/** Success terminal statuses (data landed in the destination). */
export const JOB_SUCCESS_STATUSES = ["completed", "completed_with_quarantine"] as const;

/** True when the job succeeded — includes the success-with-warnings state. */
export function isJobSuccess(status: string | undefined): boolean {
  return status === "completed" || status === "completed_with_quarantine";
}

/** Human-readable label for a job status (never surface the raw enum). */
export function jobStatusLabel(status: string): string {
  switch (status) {
    case "completed":
      return "Completed";
    case "completed_with_quarantine":
      return "Completed with quarantine";
    case "failed":
      return "Failed";
    case "running":
      return "Running";
    case "pending":
      return "Pending";
    case "cancelled":
      return "Cancelled";
    default:
      return status ? status.charAt(0).toUpperCase() + status.slice(1).replace(/_/g, " ") : "Unknown";
  }
}

export function jobStatusBadgeClass(status: string): string {
  if (status === "completed") return "df2-badge df2-badge-live";
  // Success-with-warnings: amber, distinct from clean green and red failure.
  if (status === "completed_with_quarantine") return "df2-badge df2-badge-warn";
  if (status === "failed") return "df2-badge df2-badge-error";
  if (status === "running" || status === "pending") return "df2-badge df2-badge-run";
  return "df2-badge df2-badge-muted";
}

export function connectorHealthLabel(status: string, lastTestOk?: boolean): string {
  if (status === "error" || lastTestOk === false) return "Action needed";
  return "Ready";
}
