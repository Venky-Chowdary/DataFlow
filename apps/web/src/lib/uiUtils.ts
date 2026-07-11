/** Shared UI helpers — single source for status badges and formatting */

export function jobStatusBadgeClass(status: string): string {
  if (status === "completed") return "df2-badge df2-badge-live";
  if (status === "failed") return "df2-badge df2-badge-error";
  if (status === "running" || status === "pending") return "df2-badge df2-badge-run";
  return "df2-badge df2-badge-muted";
}

export function connectorHealthLabel(status: string, lastTestOk?: boolean): string {
  if (status === "error" || lastTestOk === false) return "Action needed";
  return "Ready";
}
