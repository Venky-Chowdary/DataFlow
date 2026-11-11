/**
 * Reversibility Proof: the exit inventory (Page A) and rehearsal runs (Page B).
 *
 * Verdicts, completeness, durations and the seal are the API's stored evidence
 * (contract sections 3-5). This module types that evidence and words it. Rules
 * it keeps everywhere:
 * - a duration is measured from job clocks or it is "Not measured"; a projection
 *   is always labelled as one, with its basis;
 * - sampled or count-only coverage is never shown as proof;
 * - an anchor that is not write-once says so, and a failed anchor is never green.
 */

import { humanize, type ParityApproval, type ParitySignoff, type ParityTone } from "./parity";

/* ── Contract types ─────────────────────────────────────────── */

export type Criticality = "critical" | "important" | "standard";
export type ReadinessState = "never_rehearsed" | "running" | "proven" | "not_proven" | "incomplete" | "failed";
export type RehearsalVerdict = "running" | "proven" | "not_proven" | "incomplete" | "failed";
export type RehearsalStage = "inventory" | "extract" | "verify" | "seal" | "sign";
export type StageStatus = "done" | "running" | "pending" | "failed";
export type TableStatus = "pending" | "running" | "proven" | "not_proven" | "incomplete" | "failed";
export type ValueMethod = "full_checksum" | "key_diff" | "population_digest" | "sampled" | "count_only" | "none";
export type AnchorStatus = "anchored" | "not_anchored" | "failed";

export interface ExitTable {
  name: string;
  schema: string | null;
  /** Empty: the source catalog's primary key. */
  key_columns: string[];
}

export interface RehearsalSummary {
  id: string;
  n: number;
  started_at: string;
  finished_at: string | null;
  verdict: RehearsalVerdict;
  measured_seconds: number | null;
  window_seconds: number;
  fits_window: boolean | null;
  tables_declared: number;
  tables_proven: number;
  total_bytes: number | null;
  signed: boolean;
}

export interface SystemReadiness {
  state: ReadinessState;
  overdue: boolean;
  next_due_at: string | null;
  last_rehearsal: RehearsalSummary | null;
  last_proven_rehearsal: RehearsalSummary | null;
}

export interface ExitSystem {
  id: string;
  workspace_id: string;
  version: number;
  name: string;
  description: string;
  criticality: Criticality;
  source: { connector_id: string; connector_name: string | null; engine: string | null };
  tables: ExitTable[];
  exit_window_hours: number;
  window_basis: string;
  regulatory_dates: Array<{ date: string; label: string }>;
  rehearsal_every_days: number;
  rehearsal_schedule: { cron: string; timezone: string } | null;
  extract_format: "jsonl" | "csv";
  required_roles: string[];
  running_rehearsal_id?: string | null;
  created_at: string;
  created_by: string;
  updated_at: string;
  updated_by: string;
  readiness: SystemReadiness;
}

/** The writable fields of an ExitSystem (POST body; PATCH takes any subset). */
export interface ExitSystemInput {
  name: string;
  description: string;
  criticality: Criticality;
  source: { connector_id: string };
  tables: ExitTable[];
  exit_window_hours: number;
  window_basis: string;
  regulatory_dates: Array<{ date: string; label: string }>;
  rehearsal_every_days: number;
  rehearsal_schedule: { cron: string; timezone: string } | null;
  extract_format: "jsonl" | "csv";
  required_roles: string[];
}

export interface ExitBoardRow {
  id: string;
  name: string;
  criticality: Criticality;
  engine: string | null;
  tables: number;
  state: ReadinessState;
  overdue: boolean;
  next_due_at: string | null;
  last_rehearsal_at: string | null;
  last_rehearsal_id: string | null;
  measured_seconds: number | null;
  window_seconds: number;
  fits_window: boolean | null;
  completeness_label: string;
}

export interface ExitReadiness {
  generated_at: string;
  kpis: {
    systems: number;
    systems_proven: number;
    overdue: number;
    longest_measured_seconds: number | null;
    longest_measured_system_id: string | null;
    next_regulatory_date: { date: string; label: string; system_id: string } | null;
  };
  systems: ExitBoardRow[];
}

export interface Part {
  table: string;
  name: string;
  storage: "object_store" | "local_file";
  object_key: string | null;
  size_bytes: number;
  sha256: string;
  verified_on_read: boolean;
  upload_parts: number | null;
}

export interface TotalsTriple {
  sum: string;
  min: string;
  max: string;
}

export interface RehearsalDifference {
  kind: "missing_rows" | "extra_rows" | "changed_rows" | "control_total" | "seal" | string;
  rows: number | null;
  rows_at_least: boolean;
  detail: string;
}

export interface RehearsalTable {
  name: string;
  schema?: string | null;
  job_id: string | null;
  status: TableStatus;
  reason: string;
  rows_source: number | null;
  rows_extracted: number | null;
  key_columns: string[];
  key_set: { source_sha256: string | null; extract_sha256: string | null; match: boolean | null };
  row_set: { source_sha256: string | null; extract_sha256: string | null; match: boolean | null };
  control_totals: Array<{ column: string; source: TotalsTriple | null; extract: TotalsTriple | null; match: boolean | null }>;
  value_parity: { method: ValueMethod; parity_verdict: string | null; unexplained: number | null; report: string | null };
  differences: RehearsalDifference[];
  extract_seconds: number | null;
  part: Part | null;
}

export interface TrendPoint {
  rehearsal_id: string;
  n: number;
  at: string;
  measured_seconds: number | null;
  rows: number | null;
  bytes: number | null;
  verdict: RehearsalVerdict;
}

export interface Trend {
  points: TrendPoint[];
  growth_pct_rows: number | null;
  projection: { kind: "projection"; seconds: number; basis: string } | null;
}

export interface AnchorReceipt {
  id: string;
  provider: "stub" | "s3" | string;
  kind: "audit_tip" | "exit_evidence" | string;
  subject: string;
  digest: string;
  tip_hash?: string;
  anchored_at: string;
  event_time?: string | null;
  status: AnchorStatus;
  anchored: boolean;
  location: { bucket: string; key: string; version_id: string | null } | null;
  retention: { mode: "COMPLIANCE" | string; retain_until: string } | null;
  reason: string | null;
  honesty?: string;
}

export interface AnchorVerification {
  status: AnchorStatus;
  digest: string;
  provider: string;
  retention: { mode: string; retain_until: string } | null;
  checked_at: string;
  reason: string | null;
}

export interface ExitApproval extends ParityApproval {
  content_sha256?: string;
}

export interface ExitRehearsal {
  id: string;
  system_id: string;
  system_name: string;
  workspace_id: string;
  n: number;
  version: number;
  trigger: "manual" | "schedule";
  actor: string;
  started_at: string;
  finished_at: string | null;
  verdict: RehearsalVerdict;
  verdict_reason: string;
  timeline: Array<{ stage: RehearsalStage; status: StageStatus; started_at: string | null; finished_at: string | null; seconds: number | null }>;
  completeness: {
    tables_declared: number;
    tables_extracted: number;
    tables_proven: number;
    tables_not_proven: number;
    tables_incomplete: number;
    tables_failed: number;
    /** A running rehearsal carries an empty object until it is verified. */
    value_methods: Partial<Record<ValueMethod, number>>;
    statement: string;
  };
  tables: RehearsalTable[];
  seal: {
    algorithm: string;
    merkle_root: string | null;
    parts_count: number;
    total_bytes: number;
    storage: "object_store" | "local_file" | "mixed" | null;
    parts: Part[];
  };
  duration: {
    basis: "measured";
    measured_seconds: number | null;
    extract_seconds: number | null;
    verify_seconds: number | null;
    window_seconds: number;
    fits_window: boolean | null;
    margin_pct: number | null;
    statement: string;
  };
  trend: Trend;
  signoff: {
    required_roles: string[];
    approvals: ExitApproval[];
    invalidated_approvals: ExitApproval[];
    status: "blocked" | "open" | "signed";
    blocked_reason: string | null;
    signature: string | null;
    signed_at: string | null;
  };
  evidence: { signed: boolean; content_sha256: string | null; anchor: AnchorReceipt | null; export: string };
}

export interface ExitVerifyResult {
  ok: boolean;
  errors: string[];
  kind: string;
  content_sha256: string | null;
  signature_ok: boolean;
  merkle_ok: boolean;
  merkle_root: string | null;
  anchor: AnchorVerification;
}

/* ── Normalising what the API sent ──────────────────────────── */

const EMPTY_HASHES = { source_sha256: null, extract_sha256: null, match: null };

/**
 * A rehearsal with every field the views read. While a rehearsal runs, the API
 * sends each table as {name, status, reason, job_id, key_columns, schema} only;
 * the rest is "not measured yet", which is what null means here — never zero,
 * never a match.
 */
export function normalizeRehearsal(raw: ExitRehearsal): ExitRehearsal {
  const tables = (raw.tables ?? []).map((t) => {
    const partial = t as Partial<RehearsalTable> & Pick<RehearsalTable, "name" | "status">;
    return {
      name: partial.name,
      schema: partial.schema ?? null,
      job_id: partial.job_id ?? null,
      status: partial.status,
      reason: partial.reason ?? "",
      rows_source: partial.rows_source ?? null,
      rows_extracted: partial.rows_extracted ?? null,
      key_columns: partial.key_columns ?? [],
      key_set: partial.key_set ?? { ...EMPTY_HASHES },
      row_set: partial.row_set ?? { ...EMPTY_HASHES },
      control_totals: partial.control_totals ?? [],
      value_parity: partial.value_parity ?? { method: "none" as const, parity_verdict: null, unexplained: null, report: null },
      differences: partial.differences ?? [],
      extract_seconds: partial.extract_seconds ?? null,
      part: partial.part ?? null,
    } satisfies RehearsalTable;
  });
  return {
    ...raw,
    tables,
    timeline: raw.timeline ?? [],
    completeness: { ...raw.completeness, value_methods: raw.completeness?.value_methods ?? {} },
    seal: { ...raw.seal, parts: raw.seal?.parts ?? [] },
    trend: raw.trend ?? { points: [], growth_pct_rows: null, projection: null },
    signoff: {
      ...raw.signoff,
      required_roles: raw.signoff?.required_roles ?? [],
      approvals: raw.signoff?.approvals ?? [],
      invalidated_approvals: raw.signoff?.invalidated_approvals ?? [],
    },
    evidence: raw.evidence ?? { signed: false, content_sha256: null, anchor: null, export: "" },
  };
}

/* ── Presentation ───────────────────────────────────────────── */

export type ExitTone = ParityTone | "run";

/** The shared badge class for a tone, plus the running badge the job list uses. */
export function pillClass(tone: ExitTone): string {
  if (tone === "ok") return "df2-badge df2-badge-live";
  if (tone === "warn") return "df2-badge df2-badge-warn";
  if (tone === "danger") return "df2-badge df2-badge-error";
  if (tone === "run") return "df2-badge df2-badge-run";
  return "df2-badge df2-badge-muted";
}

const STATES: Record<ReadinessState, { label: string; tone: ExitTone }> = {
  proven: { label: "Proven", tone: "ok" },
  not_proven: { label: "Not proven", tone: "warn" },
  incomplete: { label: "Incomplete", tone: "danger" },
  failed: { label: "Failed", tone: "danger" },
  running: { label: "Running", tone: "run" },
  never_rehearsed: { label: "Never rehearsed", tone: "muted" },
};

/**
 * The board's status pill. Overdue outranks the last verdict (the mockup's
 * "Overdue" on a proven system): an old proof is not a current one. The
 * verdict is still shown beside it, so nothing reads from colour alone.
 */
export function readinessPresentation(state: string, overdue = false): { label: string; tone: ExitTone } {
  if (overdue && state !== "running") return { label: "Overdue", tone: "warn" };
  return STATES[state as ReadinessState] ?? { label: humanize(state) || "Unknown", tone: "muted" };
}

export function rehearsalVerdictPresentation(verdict: string): { label: string; tone: ExitTone } {
  return STATES[verdict as ReadinessState] ?? { label: humanize(verdict) || "Unknown", tone: "muted" };
}

const TABLE_STATUS: Record<TableStatus, { label: string; tone: ExitTone }> = {
  proven: { label: "Proven", tone: "ok" },
  not_proven: { label: "Not proven", tone: "warn" },
  incomplete: { label: "Incomplete", tone: "danger" },
  failed: { label: "Failed", tone: "danger" },
  running: { label: "Running", tone: "run" },
  pending: { label: "Pending", tone: "muted" },
};

export function tableStatusPresentation(status: string): { label: string; tone: ExitTone } {
  return TABLE_STATUS[status as TableStatus] ?? { label: humanize(status) || "Unknown", tone: "muted" };
}

const CRITICALITY: Record<Criticality, { label: string; tone: ExitTone }> = {
  critical: { label: "Critical", tone: "danger" },
  important: { label: "Important", tone: "warn" },
  standard: { label: "Standard", tone: "muted" },
};

export function criticalityPresentation(c: string): { label: string; tone: ExitTone } {
  return CRITICALITY[c as Criticality] ?? { label: humanize(c) || "Unset", tone: "muted" };
}

export const CRITICALITIES: Criticality[] = ["critical", "important", "standard"];

const STAGES: Record<RehearsalStage, string> = {
  inventory: "Inventory",
  extract: "Extract",
  verify: "Verify",
  seal: "Seal",
  sign: "Sign-off",
};

export function stageLabel(stage: string): string {
  return STAGES[stage as RehearsalStage] ?? humanize(stage);
}

const STAGE_STATUS: Record<StageStatus, { label: string; tone: ExitTone; icon: string }> = {
  done: { label: "Done", tone: "ok", icon: "check" },
  running: { label: "Running", tone: "run", icon: "activity" },
  pending: { label: "Pending", tone: "muted", icon: "clock" },
  failed: { label: "Failed", tone: "danger", icon: "x" },
};

export function stageStatusPresentation(status: string): { label: string; tone: ExitTone; icon: string } {
  return STAGE_STATUS[status as StageStatus] ?? { label: humanize(status), tone: "muted", icon: "clock" };
}

/** Value-check methods: which ones prove every row, and which never do. */
const METHODS: Record<ValueMethod, { label: string; proves: boolean; note: string }> = {
  full_checksum: { label: "Full checksum", proves: true, note: "every row compared by checksum" },
  key_diff: { label: "Key diff", proves: true, note: "every key compared on both sides" },
  population_digest: { label: "Population digest", proves: true, note: "full-population digest on both sides" },
  sampled: { label: "Sampled", proves: false, note: "a sample bounds nothing here; not proof" },
  count_only: { label: "Count only", proves: false, note: "row counts only; values not compared" },
  none: { label: "Not measured", proves: false, note: "no value check could run" },
};

export const VALUE_METHODS: ValueMethod[] = ["full_checksum", "key_diff", "population_digest", "sampled", "count_only", "none"];

export function valueMethodPresentation(method: string): { label: string; proves: boolean; note: string } {
  return METHODS[method as ValueMethod] ?? { label: humanize(method) || "Unknown", proves: false, note: "not a recognised check" };
}

/**
 * One line per coverage method that was used, in contract order, with whether
 * it proves anything. Sampled and count-only are listed honestly, never folded
 * into a "proven" count.
 */
export function coverageLines(methods: Partial<Record<ValueMethod, number>> | null | undefined): Array<{ method: ValueMethod; count: number; label: string; proves: boolean; note: string }> {
  const out: Array<{ method: ValueMethod; count: number; label: string; proves: boolean; note: string }> = [];
  for (const method of VALUE_METHODS) {
    const count = Number(methods?.[method] ?? 0);
    if (count > 0) out.push({ method, count, ...valueMethodPresentation(method) });
  }
  return out;
}

const ROLE_ORDER = ["engineering", "data_owner", "compliance", "finance", "risk", "operations"];
export const SIGNOFF_ROLES = ROLE_ORDER;
export const DEFAULT_REQUIRED_ROLES = ["engineering", "data_owner", "compliance"];

/* ── Numbers and time ───────────────────────────────────────── */

const MINUTE = 60;
const HOUR = 3600;
const DAY = 86400;

function trimmed(n: number): string {
  return Number.isInteger(n) ? String(n) : n.toFixed(1).replace(/\.0$/, "");
}

/**
 * A measured duration: "42 s", "18 min", "2 h 5 min", "61 h", "4.5 d".
 * Null is "Not measured" — never zero, never an estimate.
 */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "Not measured";
  if (seconds < MINUTE) return `${Math.round(seconds)} s`;
  if (seconds < HOUR) return `${Math.round(seconds / MINUTE)} min`;
  if (seconds < 10 * HOUR) {
    const h = Math.floor(seconds / HOUR);
    const m = Math.round((seconds - h * HOUR) / MINUTE);
    if (m === 60) return `${h + 1} h`;
    return m ? `${h} h ${m} min` : `${h} h`;
  }
  if (seconds < 100 * HOUR) return `${Math.round(seconds / HOUR)} h`;
  return `${trimmed(Math.round((seconds / DAY) * 10) / 10)} d`;
}

/** A window as it was declared: whole days read as days ("30 d"), otherwise hours ("36 h"). */
export function formatWindow(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return "No window";
  if (seconds % DAY === 0) return `${seconds / DAY} d`;
  if (seconds % HOUR === 0) return `${seconds / HOUR} h`;
  return formatDuration(seconds);
}

export interface WindowRatio {
  /** measured / window, unclamped (1.2 = 20% over). */
  ratio: number;
  /** 0..1 for drawing; a run over the window fills the bar. */
  fraction: number;
  over: boolean;
  /** "8% of the window", "120% of the window". */
  label: string;
}

/**
 * How much of the window a measured exit used. Null when nothing was measured
 * or no window is declared: there is no ratio to draw, and none is invented.
 */
export function windowRatio(measured: number | null | undefined, window: number | null | undefined): WindowRatio | null {
  if (measured == null || window == null || !Number.isFinite(measured) || !Number.isFinite(window) || window <= 0 || measured < 0) return null;
  const ratio = measured / window;
  const pct = ratio * 100;
  // Under 1% still shows as "<1%": a fit is not rounded to zero time.
  const pctText = pct > 0 && pct < 1 ? "<1%" : `${pct < 10 ? trimmed(Math.round(pct * 10) / 10) : Math.round(pct)}%`;
  return { ratio, fraction: Math.min(1, ratio), over: ratio > 1, label: `${pctText} of the window` };
}

/** "61 h / 30 d" on the board, or "Not measured". */
export function measuredVsWindowText(measured: number | null | undefined, window: number | null | undefined): string {
  if (measured == null) return "Not measured";
  return `${formatDuration(measured)} / ${formatWindow(window)}`;
}

/** Whether a fits flag, the API's, reads "fits", "does not fit" or nothing. */
export function fitsText(fits: boolean | null | undefined): string {
  if (fits == null) return "Not measured";
  return fits ? "Fits the window" : "Does not fit the window";
}

function dayDiff(fromMs: number, toMs: number): number {
  return Math.floor(Math.abs(toMs - fromMs) / (DAY * 1000));
}

/** True when the next rehearsal date has passed (contract 4.3: now > next_due_at). */
export function isOverdue(nextDueAt: string | null | undefined, now: Date = new Date()): boolean {
  if (!nextDueAt) return false;
  const due = Date.parse(nextDueAt);
  return Number.isFinite(due) && now.getTime() > due;
}

/**
 * "Overdue by 12 days", "Due in 5 days", "Due today". The API's `overdue`
 * flag decides; the date only words it, so the two never disagree on screen.
 */
export function dueText(nextDueAt: string | null | undefined, overdue: boolean, now: Date = new Date()): string {
  if (!nextDueAt) return overdue ? "Overdue" : "";
  const due = Date.parse(nextDueAt);
  if (!Number.isFinite(due)) return overdue ? "Overdue" : "";
  const days = dayDiff(due, now.getTime());
  if (overdue) return days === 0 ? "Overdue since today" : `Overdue by ${days} ${days === 1 ? "day" : "days"}`;
  return days === 0 ? "Due today" : `Due in ${days} ${days === 1 ? "day" : "days"}`;
}

/** "2 Nov 2026" in the reader's locale; the raw value when it does not parse. */
export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "";
  // A bare YYYY-MM-DD is a calendar date: read it as UTC so it never shifts a day.
  const ms = /^\d{4}-\d{2}-\d{2}$/.test(iso) ? Date.parse(`${iso}T00:00:00Z`) : Date.parse(iso);
  if (!Number.isFinite(ms)) return String(iso);
  return new Date(ms).toLocaleDateString(undefined, {
    day: "numeric",
    month: "short",
    year: "numeric",
    ...(/^\d{4}-\d{2}-\d{2}$/.test(iso) ? { timeZone: "UTC" } : {}),
  });
}

/** 2.4 TB, 812 MB, 15 KB; null is "Unknown". Decimal units, like storage consoles. */
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null || !Number.isFinite(bytes) || bytes < 0) return "Unknown";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let value = bytes;
  let i = 0;
  while (value >= 1000 && i < units.length - 1) {
    value /= 1000;
    i += 1;
  }
  return i === 0 ? `${bytes} B` : `${value >= 100 ? Math.round(value) : trimmed(Math.round(value * 10) / 10)} ${units[i]}`;
}

/** "sha256:4be1…09c2": enough to compare by eye; the full value goes in a title or copy chip. */
export function shortHash(hash: string | null | undefined): string {
  const s = String(hash || "").trim();
  if (!s) return "";
  const colon = s.indexOf(":");
  const prefix = colon > 0 ? s.slice(0, colon + 1) : "";
  const body = colon > 0 ? s.slice(colon + 1) : s;
  return body.length > 12 ? `${prefix}${body.slice(0, 4)}…${body.slice(-4)}` : s;
}

/** "Projection, not measured: about 64 h at the latest size. Basis: …" */
export function projectionText(projection: Trend["projection"] | null | undefined): string {
  if (!projection || projection.kind !== "projection") return "";
  // The API's basis often opens with its own label; say "projection" once.
  const basis = projection.basis.replace(/^\s*projection,?\s*not a measurement:\s*/i, "").trim();
  return `Projection, not measured: about ${formatDuration(projection.seconds)} at the latest size. Basis: ${basis}`;
}

/** "Rows grew 3% since the previous rehearsal" — or nothing when it was not measured. */
export function growthText(growth: number | null | undefined): string {
  if (growth == null || !Number.isFinite(growth)) return "";
  const pct = Math.abs(growth) < 10 ? trimmed(Math.round(growth * 10) / 10) : String(Math.round(growth));
  if (Number(pct) === 0) return "Rows unchanged since the previous rehearsal";
  return growth > 0 ? `Rows grew ${pct}% since the previous rehearsal` : `Rows shrank ${pct.replace("-", "")}% since the previous rehearsal`;
}

/* ── Anchor ─────────────────────────────────────────────────── */

/**
 * The write-once anchor, in words. Only a confirmed S3 Object Lock receipt is
 * green; the local stub is "Not anchored" and a receipt that could not be
 * confirmed is "Anchor failed" with its reason (contract 2: fails closed).
 */
export function anchorPresentation(anchor: Pick<AnchorReceipt, "status" | "reason" | "retention" | "provider"> | null | undefined): { label: string; tone: ExitTone; detail: string } {
  if (!anchor) {
    return { label: "Not anchored", tone: "muted", detail: "No anchor receipt yet: the pack is anchored when the last required role signs." };
  }
  if (anchor.status === "anchored") {
    const until = anchor.retention?.retain_until ? ` until ${formatDate(anchor.retention.retain_until)}` : "";
    const mode = anchor.retention?.mode ? `${anchor.retention.mode.toLowerCase()} mode` : "write-once";
    return { label: "Anchored", tone: "ok", detail: `Held in S3 Object Lock (${mode})${until}, read back and confirmed.` };
  }
  if (anchor.status === "failed") {
    return { label: "Anchor failed", tone: "danger", detail: anchor.reason ? `The anchor could not be confirmed: ${anchor.reason}` : "The anchor could not be confirmed." };
  }
  return {
    label: "Not anchored",
    tone: "muted",
    detail: anchor.reason || (anchor.provider === "stub" ? "Local stub: not write-once." : "This evidence is not held in a write-once store."),
  };
}

/* ── Sign-off (Parity Studio's panel) ───────────────────────── */

/** The rehearsal's sign-off in the shape Parity Studio's SignoffPanel reads. */
export function rehearsalSignoff(rehearsal: Pick<ExitRehearsal, "signoff">): ParitySignoff {
  const s = rehearsal.signoff;
  return {
    required_roles: s.required_roles ?? [],
    approvals: s.approvals ?? [],
    invalidated_approvals: s.invalidated_approvals ?? [],
    status: s.status,
    blocked_reason: s.blocked_reason,
    signature: s.signature,
    signed_at: s.signed_at,
  };
}

/** Why sign-off is not open, or "" when it is. Sign-off opens only on a proven rehearsal (decision 9). */
export function rehearsalSignoffBlockedReason(rehearsal: Pick<ExitRehearsal, "signoff" | "verdict">): string {
  if (rehearsal.signoff.status === "signed") return "";
  const reason = (rehearsal.signoff.blocked_reason || "").trim();
  if (rehearsal.signoff.status === "blocked" || rehearsal.verdict !== "proven") {
    if (reason) return reason;
    if (rehearsal.verdict === "running") return "Sign-off opens when the rehearsal has finished and every table is proven.";
    return "Sign-off opens only for a proven rehearsal: every declared table must be proven complete.";
  }
  return "";
}

/** Version-aware merge: a slow response never rolls the page back to an older rehearsal. */
export function newerRehearsal(current: ExitRehearsal | null, incoming: ExitRehearsal): ExitRehearsal {
  if (!current || current.id !== incoming.id) return incoming;
  return incoming.version >= current.version ? incoming : current;
}

/** Polling: a running rehearsal is re-read; one that finished is left alone. */
export const REHEARSAL_POLL_MS = 5_000;
export const REHEARSAL_POLL_LIMIT = 240;

export function shouldPollRehearsal(rehearsal: Pick<ExitRehearsal, "verdict"> | null, reads: number): boolean {
  return Boolean(rehearsal && rehearsal.verdict === "running" && reads < REHEARSAL_POLL_LIMIT);
}

/* ── Board ──────────────────────────────────────────────────── */

export type BoardFilter = "all" | "proven" | "attention" | "overdue" | "never";

export function boardFilterMatches(row: Pick<ExitBoardRow, "state" | "overdue">, filter: BoardFilter): boolean {
  if (filter === "all") return true;
  if (filter === "proven") return row.state === "proven" && !row.overdue;
  if (filter === "overdue") return row.overdue;
  if (filter === "never") return row.state === "never_rehearsed";
  return row.state === "not_proven" || row.state === "incomplete" || row.state === "failed";
}

export function boardFilterCounts(rows: Array<Pick<ExitBoardRow, "state" | "overdue">>): Record<BoardFilter, number> {
  const out: Record<BoardFilter, number> = { all: 0, proven: 0, attention: 0, overdue: 0, never: 0 };
  for (const row of rows) {
    for (const f of Object.keys(out) as BoardFilter[]) if (boardFilterMatches(row, f)) out[f] += 1;
  }
  return out;
}

const CRIT_RANK: Record<string, number> = { critical: 0, important: 1, standard: 2 };

/** Overdue first, then by criticality, then by name: the order an exit owner works through. */
export function sortBoardRows<T extends Pick<ExitBoardRow, "overdue" | "criticality" | "name">>(rows: T[]): T[] {
  return [...rows].sort(
    (a, b) =>
      Number(b.overdue) - Number(a.overdue)
      || (CRIT_RANK[a.criticality] ?? 3) - (CRIT_RANK[b.criticality] ?? 3)
      || a.name.localeCompare(b.name),
  );
}

/** A system's engine for display: "Oracle", "SQL Server", or "Unknown engine". */
export function engineLabel(engine: string | null | undefined): string {
  const e = String(engine || "").toLowerCase();
  const known: Record<string, string> = {
    postgresql: "PostgreSQL", postgres: "PostgreSQL", mysql: "MySQL", mariadb: "MariaDB", sqlserver: "SQL Server",
    mssql: "SQL Server", oracle: "Oracle", sqlite: "SQLite", snowflake: "Snowflake", bigquery: "BigQuery",
    mongodb: "MongoDB", redshift: "Redshift", databricks: "Databricks", db2: "Db2",
  };
  return known[e] ?? (e ? humanize(e) : "Unknown engine");
}

/* ── Deep links ─────────────────────────────────────────────── */

// The parsers live with the app's hash router (the shell reads them before this page loads).
export { exitFocusFromHash, exitHash, type ExitFocus } from "./appNavigation";

/* ── Declare form ───────────────────────────────────────────── */

export interface SystemDraft {
  name: string;
  description: string;
  criticality: Criticality;
  connectorId: string;
  tables: Array<{ name: string; schema: string; keys: string }>;
  windowValue: string;
  windowUnit: "hours" | "days";
  windowBasis: string;
  regulatoryDates: Array<{ date: string; label: string }>;
  everyDays: string;
  scheduleCron: string;
  scheduleTimezone: string;
  extractFormat: "jsonl" | "csv";
  requiredRoles: string[];
}

export function emptyDraft(): SystemDraft {
  return {
    name: "",
    description: "",
    criticality: "important",
    connectorId: "",
    tables: [{ name: "", schema: "", keys: "" }],
    windowValue: "30",
    windowUnit: "days",
    windowBasis: "",
    regulatoryDates: [],
    everyDays: "90",
    scheduleCron: "",
    scheduleTimezone: "UTC",
    extractFormat: "jsonl",
    requiredRoles: [...DEFAULT_REQUIRED_ROLES],
  };
}

export function draftFromSystem(system: ExitSystem): SystemDraft {
  const hours = system.exit_window_hours;
  const byDays = hours % 24 === 0;
  return {
    name: system.name,
    description: system.description || "",
    criticality: system.criticality,
    connectorId: system.source.connector_id,
    tables: system.tables.length
      ? system.tables.map((t) => ({ name: t.name, schema: t.schema || "", keys: t.key_columns.join(", ") }))
      : [{ name: "", schema: "", keys: "" }],
    windowValue: String(byDays ? hours / 24 : hours),
    windowUnit: byDays ? "days" : "hours",
    windowBasis: system.window_basis || "",
    regulatoryDates: system.regulatory_dates.map((d) => ({ ...d })),
    everyDays: String(system.rehearsal_every_days),
    scheduleCron: system.rehearsal_schedule?.cron || "",
    scheduleTimezone: system.rehearsal_schedule?.timezone || "UTC",
    extractFormat: system.extract_format,
    requiredRoles: [...system.required_roles],
  };
}

/**
 * Check a draft the way the API will (contract 3: 422 on bad input) and build
 * the body. Errors are keyed by field so each shows beside its control.
 */
export function validateDraft(draft: SystemDraft): { errors: Record<string, string>; input: ExitSystemInput | null } {
  const errors: Record<string, string> = {};
  const name = draft.name.trim();
  if (!name) errors.name = "Name the system.";
  if (!draft.connectorId) errors.connector = "Choose the saved connection to take the data from.";
  const tables = draft.tables
    .map((t) => ({
      name: t.name.trim(),
      schema: t.schema.trim() || null,
      key_columns: t.keys.split(",").map((k) => k.trim()).filter(Boolean),
    }))
    .filter((t) => t.name);
  if (!tables.length) errors.tables = "Declare at least one table to take.";
  const seen = new Set<string>();
  for (const t of tables) {
    const key = `${t.schema ?? ""}.${t.name}`.toLowerCase();
    if (seen.has(key)) {
      errors.tables = `${t.schema ? `${t.schema}.` : ""}${t.name} is declared twice.`;
      break;
    }
    seen.add(key);
  }
  const value = Number(draft.windowValue);
  const hours = draft.windowUnit === "days" ? value * 24 : value;
  if (!Number.isFinite(value) || !Number.isInteger(hours) || hours < 1 || hours > 87600) {
    errors.window = "The exit window is 1 hour to 10 years, in whole hours.";
  }
  const every = Number(draft.everyDays);
  if (!Number.isInteger(every) || every < 1 || every > 3650) errors.everyDays = "Rehearse every 1 to 3650 days.";
  const dates = draft.regulatoryDates.filter((d) => d.date.trim() || d.label.trim());
  for (const d of dates) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(d.date.trim()) || !Number.isFinite(Date.parse(`${d.date.trim()}T00:00:00Z`))) {
      errors.regulatoryDates = "Each regulatory date needs a valid date.";
      break;
    }
    if (!d.label.trim()) {
      errors.regulatoryDates = "Each regulatory date needs a label (what happens on that date).";
      break;
    }
  }
  const cron = draft.scheduleCron.trim();
  if (cron && cron.split(/\s+/).length !== 5) errors.schedule = "A schedule is a five-field cron expression, e.g. 0 2 1 * *.";
  if (!draft.requiredRoles.length) errors.roles = "Choose at least one sign-off role.";
  if (Object.keys(errors).length) return { errors, input: null };
  return {
    errors,
    input: {
      name,
      description: draft.description.trim(),
      criticality: draft.criticality,
      source: { connector_id: draft.connectorId },
      tables,
      exit_window_hours: hours,
      window_basis: draft.windowBasis.trim(),
      regulatory_dates: dates.map((d) => ({ date: d.date.trim(), label: d.label.trim() })),
      rehearsal_every_days: every,
      rehearsal_schedule: cron ? { cron, timezone: draft.scheduleTimezone.trim() || "UTC" } : null,
      extract_format: draft.extractFormat,
      required_roles: [...draft.requiredRoles],
    },
  };
}

/**
 * What went wrong and what to do, for any failed exit request. The API's own
 * reason is kept; the status decides the advice around it.
 */
export function exitErrorMessage(err: unknown, fallback: string): string {
  const status = typeof (err as { status?: unknown })?.status === "number" ? (err as { status: number }).status : 0;
  const raw = err instanceof Error ? err.message.trim() : "";
  const reason = /[.!?]$/.test(raw || fallback) ? raw || fallback : `${raw || fallback}.`;
  if (status === 404) return "Not found in this workspace. It may have been deleted, or it belongs to another workspace.";
  if (status === 409) return `${reason} The page has been refreshed so you can see the current state.`;
  if (status === 422) return `The request was refused: ${reason}`;
  if (status >= 500) return `${fallback} The server reported an error (HTTP ${status}). Try again in a moment.`;
  if (/timed out|failed to fetch|network/i.test(reason)) {
    return `${fallback} The request did not reach the server. Check your connection and try again.`;
  }
  return reason;
}
