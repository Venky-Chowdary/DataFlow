/**
 * Verdict copy for Settings → Audit Logs → Verify chain.
 *
 * A workspace report covers that workspace's own chain (its records and the
 * links between them); another tenant's findings are never part of it (R7).
 * The platform walk is a platform admin's report and names workspaces.
 */

export type ChainVerdictReport = {
  /** null: nothing was walked (no records, or the store could not be read). */
  verified: boolean | null;
  /** verified | findings | nothing_checked | store_unavailable (older APIs omit it). */
  status?: string;
  checked: number;
  /** The newest `checked` of `records_in_scope` were walked; older ones were not. */
  truncated_by_limit?: boolean;
  records_in_scope?: number;
  findings: Array<{ kind?: string; index?: number; event_id?: string; detail?: string; workspace_id?: string | null }>;
  /** Sent by older APIs; never shown — it is not this workspace's verdict. */
  withheld_findings?: number;
  reanchored?: Array<{ event_id?: string; kind?: string }>;
  scope?: "workspace" | "platform";
};

function reanchoredNote(report: ChainVerdictReport): string {
  const n = report.reanchored?.length ?? 0;
  return n ? ` (${n} re-anchored)` : "";
}

/** Nothing walked is never a verdict (A1): no records, or an unreadable store. */
export function chainNotChecked(report: ChainVerdictReport): boolean {
  return report.verified === null || report.status === "nothing_checked" || report.status === "store_unavailable";
}

function windowNote(report: ChainVerdictReport): string {
  if (!report.truncated_by_limit || report.records_in_scope == null) return "";
  return ` (newest ${report.checked} of ${report.records_in_scope})`;
}

export function chainVerdictTitle(report: ChainVerdictReport): string {
  const workspace = report.scope === "workspace";
  if (report.status === "store_unavailable") return "Chain not checked — the audit store could not be read";
  if (chainNotChecked(report)) return "Nothing to verify — no audit records were checked";
  if (report.verified) {
    const lead = workspace ? "This workspace's chain is intact" : "Chain intact";
    return `${lead} — ${report.checked} records re-walked${windowNote(report)}${reanchoredNote(report)}`;
  }
  const findings = report.findings ?? [];
  if (!findings.length) return "Chain verification failed";
  if (workspace) {
    return `Chain verification failed — ${findings.length} record(s) in this workspace`;
  }
  const spaces = new Set(findings.map((f) => f.workspace_id || "(platform)"));
  return `Chain verification failed — ${findings.length} record(s) across ${spaces.size} workspace(s)`;
}

export function chainVerdictToast(report: ChainVerdictReport): string {
  if (report.status === "store_unavailable") return "The audit store could not be read; nothing was verified. Try again.";
  if (chainNotChecked(report)) return "No audit records were found to check, so nothing was verified.";
  if (report.verified) {
    return `${report.checked} records re-walked — none altered or missing.`;
  }
  const n = report.findings?.length ?? 0;
  if (!n) return "The chain does not hold up.";
  return report.scope === "workspace"
    ? `${n} record(s) in this workspace do not hold up.`
    : `${n} record(s) do not hold up.`;
}
