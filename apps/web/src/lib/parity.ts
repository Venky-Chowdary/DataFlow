/**
 * Parity Studio: the shapes the API returns and the pure helpers the pages use
 * to present them.
 *
 * Verdicts are deterministic and come from the API. Nothing here decides
 * whether a run passed; these helpers only choose words and tones for what the
 * report already says. Unknown codes (a cause or role the backend adds later)
 * render from the API's own label instead of failing.
 */

/**
 * proven: every table compared in full and nothing differs; no_difference_found: a
 * sample found nothing, which bounds the difference rate but proves nothing.
 */
export type ParityVerdict = "proven" | "no_difference_found" | "explained" | "unexplained" | "unverifiable";
export type ParityCheck = "full_checksum" | "key_diff" | "sampled" | "count_only" | "none";
export type ParityTableStatus = "match" | "explained" | "unexplained" | "unverifiable";
export type ParityDifferenceKind = "missing_in_destination" | "extra_in_destination" | "value" | "duplicate_key";
export type ParityFixAction = "widen_column" | "resync_keys" | "accept" | "rerun";
export type ParityFixStatus = "proposed" | "applied" | "failed" | "dismissed";
export type ParitySignoffStatus = "blocked" | "open" | "signed";
export type ParityHeatCell = "proven" | "sampled" | "explained" | "unexplained_small" | "unexplained_large" | "none";

export interface ParityCounts {
  rows_source: number | null;
  rows_destination: number | null;
  rows_differ: number;
  explained: number;
  unexplained: number;
  /** 0..100, null when unknown. */
  proven_equal_pct: number | null;
}

export interface ParityConfidence {
  method: "full" | "sampled" | "mixed" | "none";
  sampled_rows: number;
  differences_in_sample: number;
  level: number;
  /** Clopper-Pearson one-sided upper bound on the difference rate (0..1). */
  upper_bound_rate: number | null;
  statement: string;
}

export interface ParityTable {
  name: string;
  check: ParityCheck;
  rows_source: number | null;
  rows_destination: number | null;
  proven_equal_pct: number | null;
  differences: number;
  explained: number;
  unexplained: number;
  status: ParityTableStatus;
  upper_bound_rate: number | null;
}

export interface ParityCause {
  /** Open set: whitespace, numeric_scale, ... and codes added later. */
  code: string;
  label: string;
  explained: boolean;
  detail: string;
}

export interface ParityDifference {
  id: string;
  table: string;
  column: string | null;
  kind: ParityDifferenceKind;
  rows: number;
  keys_sample: string[];
  cause: ParityCause;
  /** Null for protected columns: their values never leave the deployment. */
  example: { source: string | null; destination: string | null } | null;
  protected: boolean;
}

export interface ParityExplanation {
  text: string;
  source: "llm" | "template";
  generated_at: string;
}

export interface ParityFix {
  id: string;
  difference_ids: string[];
  action: ParityFixAction;
  title: string;
  detail: string;
  preview: string | null;
  applicable: boolean;
  not_applicable_reason: string | null;
  status: ParityFixStatus;
  approved_by: string | null;
  applied_at: string | null;
  result: string | null;
  /** When a person approved or dismissed it; only on decided fixes. */
  decided_at?: string | null;
  /** The approver's note; only on decided fixes. */
  note?: string | null;
}

export interface ParityRound {
  n: number;
  at: string;
  unexplained_before: number;
  unexplained_after: number;
  fix_id: string;
}

export interface ParityApproval {
  role: string;
  user: string;
  at: string;
  note: string;
}

export interface ParitySignoff {
  required_roles: string[];
  approvals: ParityApproval[];
  status: ParitySignoffStatus;
  blocked_reason: string | null;
  signature: string | null;
  signed_at: string | null;
}

/** What re-reading the named keys on both sides found, per table. */
export interface ParityReread {
  status: "ok" | "failed" | "not_needed";
  keys: number;
  reason?: string;
}

export interface ParityEvidence {
  reconciliation_passed: boolean | null;
  assurance_level: string | null;
  trust_score: number | null;
  protection_proof: string | null;
  transform_proof: string | null;
  /** Keyed by table name. */
  reread: Record<string, ParityReread>;
}

export interface ParityReport {
  job_id: string;
  generated_at: string;
  /** Bumps on every state change; a lower version is older news. */
  version: number;
  verdict: ParityVerdict;
  verdict_reason: string;
  parity: ParityCounts;
  confidence: ParityConfidence;
  tables: ParityTable[];
  differences: ParityDifference[];
  explanation: ParityExplanation | null;
  fixes: ParityFix[];
  rounds: ParityRound[];
  signoff: ParitySignoff;
  evidence: ParityEvidence;
}

export interface ParitySummaryRun {
  job_id: string;
  at: string;
  name: string;
  proven_equal_pct: number | null;
  unexplained: number;
  verdict: ParityVerdict;
  /** This run's parity is an outlier against the route's earlier runs; null when history is too short. */
  drift: boolean | null;
}

export interface ParitySummary {
  generated_at: string;
  kpis: {
    runs: number;
    fully_proven_pct: number | null;
    runs_with_unexplained: number;
    signoffs: number;
    most_drifting_table: string | null;
  };
  /** Oldest first, newest last. */
  runs: ParitySummaryRun[];
  heatmap: { tables: string[]; job_ids: string[]; cells: ParityHeatCell[][] };
}

/** Write fixes (widen, re-sync, re-run) a job may apply or fail before they are switched off. */
export const PARITY_MAX_ROUNDS = 3;

const WRITE_FIX_ACTIONS = new Set<string>(["widen_column", "resync_keys", "rerun"]);

/**
 * Write fixes counted against {@link PARITY_MAX_ROUNDS}: every widen, re-sync
 * or re-run that was applied or failed. A re-run records no round, so this is
 * not the number of rounds.
 */
export function fixAttemptsUsed(report: Pick<ParityReport, "fixes">): number {
  return report.fixes.filter((f) => WRITE_FIX_ACTIONS.has(f.action) && (f.status === "applied" || f.status === "failed")).length;
}

export type ParityTone = "ok" | "warn" | "danger" | "muted";

/** The shared badge class for a tone (the same badges the job list uses). */
export function badgeClass(tone: ParityTone): string {
  if (tone === "ok") return "df2-badge df2-badge-live";
  if (tone === "warn") return "df2-badge df2-badge-warn";
  if (tone === "danger") return "df2-badge df2-badge-error";
  return "df2-badge df2-badge-muted";
}

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/** Verdict pill: what the report concluded, in one short phrase. */
export function verdictPresentation(report: Pick<ParityReport, "verdict" | "parity">): { label: string; tone: ParityTone } {
  switch (report.verdict) {
    case "proven":
      return { label: "Proven equal", tone: "ok" };
    case "no_difference_found":
      // A clean sample bounds the rate; it is not proof of every row.
      return { label: "No difference in sample", tone: "ok" };
    case "explained":
      return { label: "Every difference explained", tone: "ok" };
    case "unexplained": {
      const n = Math.max(0, report.parity.unexplained);
      return { label: `${plural(n, "difference")} unexplained`, tone: "warn" };
    }
    default:
      return { label: "Can't be verified", tone: "muted" };
  }
}

/**
 * A percentage that never rounds up into a claim the data does not make:
 * 99.996% shows as 99.99%, not 100%. Null reads as unknown, not zero.
 */
export function formatParityPct(pct: number | null | undefined): string {
  if (pct == null || !Number.isFinite(pct)) return "Unknown";
  const p = Math.max(0, Math.min(100, pct));
  if (p >= 100) return "100%";
  if (p <= 0) return "0%";
  const decimals = p >= 99 ? 2 : 1;
  const factor = 10 ** decimals;
  const floored = Math.floor(p * factor + 1e-9) / factor;
  return `${Number(floored.toFixed(decimals)).toString()}%`;
}

/**
 * The headline figure is a lower bound unless every table was compared in
 * full: a sampled table contributes its Clopper-Pearson bound, not a count.
 */
export function parityIsLowerBound(report: Pick<ParityReport, "confidence">): boolean {
  return report.confidence.method !== "full";
}

/** Sampled checks state a lower bound, so the figure reads "at least". */
export function tableParityLabel(table: Pick<ParityTable, "check" | "proven_equal_pct">): string {
  const value = formatParityPct(table.proven_equal_pct);
  if (table.proven_equal_pct == null) return value;
  return table.check === "sampled" && table.proven_equal_pct < 100 ? `≥ ${value}` : value;
}

const CHECK_LABELS: Record<ParityCheck, string> = {
  full_checksum: "Full checksum",
  key_diff: "Key diff + values",
  sampled: "Count + sampled values",
  count_only: "Count only",
  none: "Not checked",
};

export function checkLabel(check: string): string {
  return CHECK_LABELS[check as ParityCheck] ?? humanize(check);
}

export function tableStatusPresentation(
  table: Pick<ParityTable, "status" | "unexplained"> & Partial<Pick<ParityTable, "check">>,
): { label: string; tone: ParityTone } {
  switch (table.status) {
    case "match":
      return table.check === "sampled" ? { label: "No difference in sample", tone: "ok" } : { label: "Match", tone: "ok" };
    case "explained":
      return { label: "Explained", tone: "ok" };
    case "unexplained":
      return { label: `${Math.max(1, table.unexplained).toLocaleString("en-US")} to resolve`, tone: "warn" };
    default:
      return { label: "Can't verify", tone: "muted" };
  }
}

/** Bar tone for a table row: amber while anything is unresolved, grey when unknown. */
export function tableBarTone(table: Pick<ParityTable, "status" | "proven_equal_pct">): "brand" | "warn" | "muted" {
  if (table.status === "unverifiable" || table.proven_equal_pct == null) return "muted";
  if (table.status === "unexplained") return "warn";
  return "brand";
}

const KIND_LABELS: Record<ParityDifferenceKind, string> = {
  missing_in_destination: "missing in destination",
  extra_in_destination: "extra in destination",
  value: "value differs",
  duplicate_key: "duplicate key",
};

/** "4 rows · amount", or "2 rows · missing in destination" when no column applies. */
export function differenceTitle(diff: Pick<ParityDifference, "rows" | "column" | "kind">): string {
  const subject = diff.column || KIND_LABELS[diff.kind] || humanize(diff.kind);
  return `${plural(diff.rows, "row")} · ${subject}`;
}

/** Explained / Unexplained, from the deterministic classifier, never the AI. */
export function causeChip(diff: Pick<ParityDifference, "cause">): { label: string; tone: ParityTone } {
  return diff.cause.explained ? { label: "Explained", tone: "ok" } : { label: "Unexplained", tone: "danger" };
}

/** The cause's name, from the API's label first so a new code still reads well. */
export function causeLabel(cause: Pick<ParityCause, "code" | "label">): string {
  const label = (cause.label || "").trim();
  return label || humanize(cause.code || "unexplained");
}

const ROLE_LABELS: Record<string, string> = {
  engineering: "Engineering",
  data_owner: "Data owner",
  compliance: "Compliance",
  finance: "Finance",
  risk: "Risk",
  operations: "Operations",
};

export function roleLabel(role: string): string {
  return ROLE_LABELS[role] ?? humanize(role);
}

/** "data_owner" -> "Data owner". */
export function humanize(code: string): string {
  const text = String(code || "").replace(/[_-]+/g, " ").trim();
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : "";
}

const FIX_STATUS: Record<ParityFixStatus, { label: string; tone: ParityTone }> = {
  proposed: { label: "Proposed", tone: "muted" },
  applied: { label: "Applied", tone: "ok" },
  failed: { label: "Failed", tone: "danger" },
  dismissed: { label: "Dismissed", tone: "muted" },
};

export function fixStatusPresentation(status: string): { label: string; tone: ParityTone } {
  return FIX_STATUS[status as ParityFixStatus] ?? { label: humanize(status), tone: "muted" };
}

/** The approve button's words, so the person knows what pressing it does. */
export function fixApproveLabel(action: string): string {
  if (action === "accept") return "Mark as accepted";
  if (action === "rerun") return "Approve and re-run";
  return "Approve and re-check";
}

/** One sentence shown in the confirmation step before anything is applied. */
export function fixConsequence(fix: Pick<ParityFix, "action" | "difference_ids">): string {
  switch (fix.action) {
    case "widen_column":
      return "Datawrap changes the destination column, then checks the affected rows again.";
    case "resync_keys":
      return "Datawrap re-writes the affected keys with an upsert, then checks them again.";
    case "accept":
      return "The differences are recorded as accepted by you, with your note, and count as explained.";
    case "rerun":
      return "Datawrap starts a re-run of this job. The report updates when that run finishes.";
    default:
      return "Datawrap applies this fix, then checks the affected rows again.";
  }
}

/** Which fixes a person can still act on. */
export function isFixActionable(fix: Pick<ParityFix, "status" | "applicable">): boolean {
  return fix.status === "proposed" && fix.applicable;
}

/**
 * Version-aware merge: an action's response or a refresh only replaces what is
 * on screen when it is at least as new. A slow response must never roll the
 * page back to an older state.
 */
export function newerReport(current: ParityReport | null, incoming: ParityReport): ParityReport {
  if (!current || current.job_id !== incoming.job_id) return incoming;
  return incoming.version >= current.version ? incoming : current;
}

export interface SignoffRow {
  role: string;
  label: string;
  approval: ParityApproval | null;
}

/** One row per required role, with its approval when one is recorded. */
export function signoffRows(signoff: ParitySignoff): SignoffRow[] {
  const roles = signoff.required_roles.length ? signoff.required_roles : [];
  const byRole = new Map<string, ParityApproval>();
  for (const approval of signoff.approvals) {
    if (!byRole.has(approval.role)) byRole.set(approval.role, approval);
  }
  const rows: SignoffRow[] = roles.map((role) => ({ role, label: roleLabel(role), approval: byRole.get(role) ?? null }));
  // An approval for a role that is not required still belongs on the record.
  for (const approval of signoff.approvals) {
    if (!roles.includes(approval.role)) rows.push({ role: approval.role, label: roleLabel(approval.role), approval });
  }
  return rows;
}

/** Roles still waiting for a signature. */
export function unsignedRoles(signoff: ParitySignoff): string[] {
  const signed = new Set(signoff.approvals.map((a) => a.role));
  return signoff.required_roles.filter((role) => !signed.has(role));
}

/** Why sign-off is not possible right now, or "" when it is. */
export function signoffBlockedReason(report: Pick<ParityReport, "signoff" | "parity" | "verdict">): string {
  if (report.signoff.status === "signed") return "";
  if (report.signoff.status === "blocked" || report.parity.unexplained > 0 || report.verdict === "unverifiable") {
    const reason = (report.signoff.blocked_reason || "").trim();
    if (reason) return reason;
    const n = Math.max(0, report.parity.unexplained);
    if (n > 0) return `Sign-off opens when nothing is unexplained. ${plural(n, "difference")} still need a cause or a fix.`;
    return report.verdict === "unverifiable"
      ? "The evidence cannot decide parity, so there is nothing to sign yet."
      : "Sign-off is not open for this report yet.";
  }
  return "";
}

/** "hmac-sha256:9f3c…a71e": enough to compare by eye; the full value is in the title. */
export function shortSignature(signature: string): string {
  const s = signature.trim();
  const colon = s.indexOf(":");
  const prefix = colon > 0 ? s.slice(0, colon + 1) : "";
  const body = colon > 0 ? s.slice(colon + 1) : s;
  return body.length > 12 ? `${prefix}${body.slice(0, 4)}…${body.slice(-4)}` : s;
}

/** Local clock time for an ISO timestamp; the raw value when it does not parse. */
export function formatClock(iso: string | null | undefined): string {
  if (!iso) return "";
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return String(iso);
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return String(iso);
  return new Date(ms).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
}

export function formatCount(n: number | null | undefined): string {
  return n == null || !Number.isFinite(n) ? "Unknown" : n.toLocaleString("en-US");
}

/** Differences grouped by table, in the order the tables are listed. */
export function differencesByTable(report: Pick<ParityReport, "tables" | "differences">): Array<{ table: string; differences: ParityDifference[] }> {
  const order = report.tables.map((t) => t.name);
  const groups = new Map<string, ParityDifference[]>();
  for (const diff of report.differences) {
    const list = groups.get(diff.table) ?? [];
    list.push(diff);
    groups.set(diff.table, list);
  }
  const names = [...groups.keys()].sort((a, b) => {
    const ia = order.indexOf(a);
    const ib = order.indexOf(b);
    return (ia < 0 ? Number.MAX_SAFE_INTEGER : ia) - (ib < 0 ? Number.MAX_SAFE_INTEGER : ib);
  });
  return names.map((table) => ({
    table,
    // Unexplained first: that is what stands between the run and sign-off.
    differences: [...(groups.get(table) ?? [])].sort(
      (a, b) => Number(a.cause.explained) - Number(b.cause.explained) || b.rows - a.rows,
    ),
  }));
}

/** Heatmap cell words, for the legend and each cell's tooltip. */
export const HEAT_CELL_LABELS: Record<ParityHeatCell, string> = {
  proven: "Proven",
  sampled: "No difference in sample",
  explained: "All explained",
  unexplained_small: "Unexplained, small",
  unexplained_large: "Unexplained, large",
  none: "Not checked",
};

export function heatCellLabel(cell: string): string {
  return HEAT_CELL_LABELS[cell as ParityHeatCell] ?? humanize(cell);
}

/**
 * What went wrong and what to do, for any failed Parity Studio request. The
 * API's own reason is kept; the status decides the advice around it.
 */
export function parityErrorMessage(err: unknown, fallback: string): string {
  const status = typeof (err as { status?: unknown })?.status === "number" ? (err as { status: number }).status : 0;
  const raw = err instanceof Error ? err.message.trim() : "";
  const reason = /[.!?]$/.test(raw || fallback) ? raw || fallback : `${raw || fallback}.`;
  if (status === 404) {
    return "This job was not found in this workspace. It may have been deleted, or it belongs to another workspace.";
  }
  if (status === 403) {
    // The workspace check (approve/dismiss) says only what is missing; say who can do it.
    return /workspace write access/i.test(raw)
      ? `${reason} Approving or dismissing a fix needs editor or admin access to this job's workspace. Ask a workspace admin, or an editor to act on it.`
      : reason;
  }
  if (status === 409) return `${reason} The report has been refreshed so you can see its current state.`;
  if (status === 422) return `The request was refused: ${reason}`;
  if (status >= 500) return `${fallback} The server reported an error (HTTP ${status}). Try again in a moment.`;
  if (/timed out|failed to fetch|network/i.test(reason)) {
    return `${fallback} The request did not reach the server. Check your connection and try again.`;
  }
  return reason;
}

/** "4 keys re-read on both sides", or why the re-read did not run, per table. */
export function rereadLabel(reread: ParityEvidence["reread"] | null | undefined): { value: string; tone: "ok" | "warn" | "muted"; detail: string } {
  const entries = Object.entries(reread ?? {});
  if (entries.length === 0) return { value: "Not needed", tone: "muted", detail: "" };
  const failed = entries.filter(([, r]) => r.status === "failed");
  const keys = entries.reduce((n, [, r]) => n + (r.status === "ok" ? Math.max(0, r.keys || 0) : 0), 0);
  const detail = entries
    .map(([table, r]) => `${table}: ${r.status === "ok" ? plural(r.keys, "key") + " re-read" : r.status === "failed" ? `failed${r.reason ? ` (${r.reason})` : ""}` : "not needed"}`)
    .join("; ");
  if (failed.length) return { value: `Failed for ${failed.map(([t]) => t).join(", ")}`, tone: "warn", detail };
  if (keys > 0) return { value: `${plural(keys, "key")} re-read`, tone: "ok", detail };
  return { value: "Not needed", tone: "muted", detail };
}

/**
 * The explanation was written before the latest decision (a fix applied,
 * failed or dismissed, or a re-check round), so it may describe differences
 * that are gone. The verdict above it is always current; the words may not be.
 */
export function explanationIsStale(report: Pick<ParityReport, "explanation" | "fixes" | "rounds">): boolean {
  const written = Date.parse(report.explanation?.generated_at ?? "");
  if (!Number.isFinite(written)) return false;
  const times = [
    ...report.rounds.map((r) => r.at),
    ...report.fixes.flatMap((f) => [f.applied_at, f.decided_at]),
  ]
    .map((t) => Date.parse(t ?? ""))
    .filter((t) => Number.isFinite(t));
  return times.some((t) => t > written);
}
