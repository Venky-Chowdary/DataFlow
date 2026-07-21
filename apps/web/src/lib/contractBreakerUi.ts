/** Contract circuit-breaker display helpers for Pipelines / Contracts. */

export function breakerBadgeClass(state: string | null | undefined): string {
  const s = (state || "").toLowerCase();
  if (s === "closed") return "df2-badge-live";
  if (s === "open" || s === "half_open") return "df2-badge-warn";
  return "df2-badge-muted";
}

export function breakerLabel(state: string | null | undefined): string {
  const s = (state || "").trim().toLowerCase();
  if (!s) return "";
  if (s === "half_open") return "Breaker half-open";
  return `Breaker ${s}`;
}

export function breakerBlocksRuns(state: string | null | undefined): boolean {
  const s = (state || "").toLowerCase();
  return s === "open" || s === "half_open";
}

/** List-row signal: only open / half-open so healthy closed does not hide last-run status. */
export function breakerWarnLabel(state: string | null | undefined): string {
  return breakerBlocksRuns(state) ? breakerLabel(state) : "";
}
