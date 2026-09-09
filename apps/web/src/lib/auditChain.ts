/**
 * Verdict copy for Settings → Audit Logs → Verify chain.
 *
 * The HMAC walk is platform-wide. Findings listed to the operator are this
 * workspace's records; other-workspace findings are a count, not event ids.
 */

export type ChainVerdictReport = {
  verified: boolean;
  checked: number;
  findings: Array<{ kind?: string; index?: number; event_id?: string; detail?: string }>;
  withheld_findings?: number;
};

export function chainVerdictTitle(report: ChainVerdictReport): string {
  if (report.verified) {
    return `Chain intact — ${report.checked} records re-walked`;
  }
  const local = report.findings?.length ?? 0;
  const withheld = report.withheld_findings ?? 0;
  if (local && withheld) {
    return `Chain verification failed — ${local} record(s) in this workspace, ${withheld} withheld`;
  }
  if (local) {
    return `Chain verification failed — ${local} record(s) in this workspace`;
  }
  if (withheld) {
    return `Chain verification failed — ${withheld} finding(s) in other workspaces (not shown)`;
  }
  return "Chain verification failed";
}

export function chainVerdictToast(report: ChainVerdictReport): string {
  if (report.verified) {
    return `${report.checked} records re-walked — none altered or missing.`;
  }
  const local = report.findings?.length ?? 0;
  const withheld = report.withheld_findings ?? 0;
  if (local && withheld) {
    return `${local} record(s) in this workspace do not hold up; ${withheld} withheld.`;
  }
  if (local) {
    return `${local} record(s) in this workspace do not hold up.`;
  }
  if (withheld) {
    return `${withheld} finding(s) belong to other workspaces and are not shown.`;
  }
  return "The chain does not hold up.";
}
