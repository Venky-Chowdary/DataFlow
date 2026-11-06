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
  /**
   * verified | verified_with_exceptions | inconclusive | findings |
   * nothing_checked | store_unavailable (older APIs omit it).
   */
  status?: string;
  /** Records whose signing key this deployment no longer holds (QA r12 #3). */
  unverifiable_key_lost?: { count: number; by_key_id: Record<string, number>; owner_action?: string };
  checked: number;
  /** The newest `checked` of `records_in_scope` were walked; older ones were not. */
  truncated_by_limit?: boolean;
  records_in_scope?: number;
  findings: Array<{ kind?: string; index?: number; event_id?: string; detail?: string; workspace_id?: string | null }>;
  /** Sent by older APIs; never shown — it is not this workspace's verdict. */
  withheld_findings?: number;
  reanchored?: Array<{ event_id?: string; kind?: string }>;
  scope?: "workspace" | "platform";
  /** Tampering a key-history re-anchor recorded; it stays tampering (C4 review 4). */
  prior_tampering?: Array<{ kind?: string; event_id?: string; problem?: string; reanchored_at?: number }>;
  /** Why records naming an unheld key could not be judged (status "inconclusive"). */
  key_history_unreadable?: string | null;
  key_history_tampered?: string | null;
  keyring_tampered?: string | null;
  /** The vault secrets key changed: inconclusive until a platform admin re-anchors. */
  secrets_key_changed?: boolean;
  tail_truncated?: { witnessed_seq?: number; store_head_seq?: number; reason?: string } | null;
  categories?: Record<string, number>;
};

/** Some records name a key whose status could not be judged: neither verified nor tampering. */
export function chainInconclusive(report: ChainVerdictReport): boolean {
  return report.status === "inconclusive";
}

function priorNote(report: ChainVerdictReport): string {
  const n = report.prior_tampering?.length ?? 0;
  return n ? `; ${n} earlier finding(s) recorded at a key-history re-anchor` : "";
}

/** Chain-level tampering with no per-record finding (history, keyring, tail, prior). */
function chainLevelProblem(report: ChainVerdictReport): string {
  if (report.tail_truncated) return report.tail_truncated.reason || "the newest records were removed";
  if (report.keyring_tampered) return report.keyring_tampered;
  if (report.key_history_tampered && !report.secrets_key_changed) return report.key_history_tampered;
  const prior = report.prior_tampering?.length ?? 0;
  if (prior) return `${prior} finding(s) recorded at a key-history re-anchor`;
  return "";
}

function reanchoredNote(report: ChainVerdictReport): string {
  const n = report.reanchored?.length ?? 0;
  return n ? ` (${n} re-anchored)` : "";
}

/** Nothing walked is never a verdict (A1): no records, or an unreadable store. */
export function chainNotChecked(report: ChainVerdictReport): boolean {
  return report.verified === null || report.status === "nothing_checked" || report.status === "store_unavailable";
}

/**
 * Every record that could be checked holds up; some could not be checked
 * because the key that signed them is gone. Not tampering, and not "verified".
 */
export function chainVerifiedWithExceptions(report: ChainVerdictReport): boolean {
  return report.status === "verified_with_exceptions";
}

function lostKeyIds(report: ChainVerdictReport): string {
  const ids = Object.keys(report.unverifiable_key_lost?.by_key_id ?? {});
  const shown = ids.slice(0, 3).join(", ");
  return ids.length > 3 ? `${shown} and ${ids.length - 3} more` : shown;
}

function lostKeyNote(report: ChainVerdictReport): string {
  const n = report.unverifiable_key_lost?.count ?? 0;
  const ids = Object.keys(report.unverifiable_key_lost?.by_key_id ?? {});
  const keys = ids.length === 1 ? "key" : "keys";
  return `${n} record(s) whose key is not held (${keys} ${lostKeyIds(report)})`;
}

function windowNote(report: ChainVerdictReport): string {
  if (!report.truncated_by_limit || report.records_in_scope == null) return "";
  return ` (newest ${report.checked} of ${report.records_in_scope})`;
}

export function chainVerdictTitle(report: ChainVerdictReport): string {
  const workspace = report.scope === "workspace";
  if (report.status === "store_unavailable") return "Chain not checked — the audit store could not be read";
  if (chainNotChecked(report)) return "Nothing to verify — no audit records were checked";
  if (chainVerifiedWithExceptions(report)) {
    const ok = report.checked - (report.unverifiable_key_lost?.count ?? 0);
    // Not "no tampering": those records could not be checked at all.
    return `Verified except ${lostKeyNote(report)} — ${ok} of ${report.checked} records verified${windowNote(report)}`;
  }
  if (chainInconclusive(report)) {
    const n = report.categories?.inconclusive ?? report.findings?.length ?? 0;
    const why = report.secrets_key_changed
      ? "the vault secrets key changed; a platform admin must re-anchor the key history"
      : report.key_history_unreadable || "the key history could not be read";
    return `Not judged — ${n} record(s) name a key whose status cannot be judged: ${why}${windowNote(report)}`;
  }
  if (report.verified) {
    const lead = workspace ? "This workspace's chain is intact" : "Chain intact";
    return `${lead} — ${report.checked} records re-walked${windowNote(report)}${reanchoredNote(report)}`;
  }
  const findings = report.findings ?? [];
  if (!findings.length) {
    const problem = chainLevelProblem(report);
    return problem ? `Chain verification failed — ${problem}` : "Chain verification failed";
  }
  if (workspace) {
    return `Chain verification failed — ${findings.length} record(s) in this workspace${priorNote(report)}`;
  }
  const spaces = new Set(findings.map((f) => f.workspace_id || "(platform)"));
  return `Chain verification failed — ${findings.length} record(s) across ${spaces.size} workspace(s)${priorNote(report)}`;
}

export function chainVerdictToast(report: ChainVerdictReport): string {
  if (report.status === "store_unavailable") return "The audit store could not be read; nothing was verified. Try again.";
  if (chainNotChecked(report)) return "No audit records were found to check, so nothing was verified.";
  if (chainVerifiedWithExceptions(report)) return `Verified except ${lostKeyNote(report)}.`;
  if (chainInconclusive(report)) {
    return report.secrets_key_changed
      ? "Not judged: the vault secrets key changed. A platform admin must re-anchor the key history."
      : "Not judged: some records name a key whose status could not be read. Try again.";
  }
  if (report.verified) {
    return `${report.checked} records re-walked — none altered or missing.`;
  }
  const n = report.findings?.length ?? 0;
  if (!n) {
    const problem = chainLevelProblem(report);
    return problem ? `The chain does not hold up: ${problem}.` : "The chain does not hold up.";
  }
  return report.scope === "workspace"
    ? `${n} record(s) in this workspace do not hold up.`
    : `${n} record(s) do not hold up.`;
}
