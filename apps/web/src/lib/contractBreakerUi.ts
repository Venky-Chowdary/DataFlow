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

/** Studio / schedule bind — same fail-closed copy as Pipelines Run now. */
export function contractBreakerBlocksRun(state: string | null | undefined): string {
  if (!breakerBlocksRuns(state)) return "";
  return "Contract circuit breaker is OPEN. Reset it after you fix the violation, then re-run.";
}

export function isCircuitBreakerFailureText(text: string | null | undefined): boolean {
  const t = String(text || "").toLowerCase();
  return t.includes("circuit_breaker_open")
    || (t.includes("circuit breaker") && t.includes("is open"));
}

/** Parse contract id from engine / enqueue breaker errors. */
export function contractIdFromBreakerFailure(opts: {
  error?: string | null;
  errorDetails?: Record<string, unknown> | null;
}): string {
  const details = opts.errorDetails || {};
  const violations = details.violations;
  if (Array.isArray(violations)) {
    for (const raw of violations) {
      if (!raw || typeof raw !== "object") continue;
      const row = raw as { rule?: string; contract_id?: string };
      if (String(row.rule || "") === "circuit_breaker_open") {
        const id = String(row.contract_id || "").trim();
        if (id) return id;
      }
    }
  }
  const match = String(opts.error || "").match(/circuit breaker for contract\s+([^\s;]+)/i);
  return match ? match[1] : "";
}

/** Dual Run campaign — engine-stamped verdict, display only. */
export function campaignBadgeClass(verdict: string | null | undefined): string {
  const v = (verdict || "").trim().toLowerCase();
  if (v === "cutover_ready") return "df2-badge-live";
  if (v === "diverging") return "df2-badge-warn";
  if (v === "in_progress") return "df2-badge-muted";
  return "df2-badge-muted";
}

export function campaignLabel(verdict: string | null | undefined): string {
  const v = (verdict || "").trim().toLowerCase();
  if (v === "cutover_ready") return "Parallel run ready";
  if (v === "diverging") return "Parallel run diverging";
  if (v === "in_progress") return "Parallel run in progress";
  if (v === "unproven") return "Parallel run unproven";
  return "";
}

/** List-row: only diverging so healthy/in-progress does not hide last-run status. */
export function campaignWarnLabel(verdict: string | null | undefined): string {
  return (verdict || "").trim().toLowerCase() === "diverging" ? campaignLabel(verdict) : "";
}
