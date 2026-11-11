/**
 * Jobs grid model — the pure parts of the Jobs page operations grid.
 *
 * Display helpers the Jobs page and its detail share (name, route, duration,
 * live statuses) live here so the grid components and the page read one copy.
 * Everything else is what the grid shows per row (verification, attention,
 * sort keys), what it exports (CSV), and what it remembers per operator
 * (visible columns, density).
 */
import { classifyGate8Status, presentMultiStreamGate8 } from "../components/transfer/gate8Status";
import { formatJobRowMetric, destMetricCompact } from "./conservationLedger";
import { connectorDisplayName } from "./connectorTypes";
import { formatJobRoute, isRestoredEndpointTitle } from "./jobEvidence";
import { parseServerTimestampMs } from "./jobTheaterProgress";
import { readSession } from "./session";
import { formatSyncModeLabel } from "./transferConstants";
import type { TransferJob } from "./types";
import { isJobSuccess, jobStatusLabel, presentedJobStatus } from "./uiUtils";

/** Statuses that still emit progress — the grid shows a live bar for these. */
export const LIVE_JOB_STATUSES = new Set(["running", "pending", "queued", "retrying"]);

export function shortJobId(id: string): string {
  return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}

type RouteFields = Pick<
  TransferJob,
  "source_name" | "source_type" | "destination_database" | "destination_collection" | "destination_type" | "stream_names"
>;

export function jobRouteLabel(job: RouteFields) {
  return formatJobRoute(job);
}

/** The operator's name for a job; a restored endpoint title falls back to the route. */
export function jobDisplayName(job: RouteFields & Pick<TransferJob, "name">, override?: string | null) {
  const named = (override ?? job.name)?.trim();
  if (named && !isRestoredEndpointTitle(named, job)) return named;
  return formatJobRoute(job);
}

export function formatJobDuration(startedAt?: string, completedAt?: string): string | null {
  if (!startedAt) return null;
  // Job-store timestamps are UTC without an offset; Date.parse reads them as local.
  const start = parseServerTimestampMs(startedAt);
  if (start == null) return null;
  const end = completedAt ? parseServerTimestampMs(completedAt) : Date.now();
  if (end == null || end < start) return null;
  const s = Math.floor((end - start) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function durationMs(job: TransferJob): number | null {
  const start = job.started_at ? parseServerTimestampMs(job.started_at) : null;
  if (start == null) return null;
  const end = job.completed_at ? parseServerTimestampMs(job.completed_at) : Date.now();
  return end == null || end < start ? null : end - start;
}

/** When the run started; a queued run that never started shows when it was created. */
export function jobStartedAt(job: Pick<TransferJob, "started_at" | "created_at">): string {
  return job.started_at || job.created_at;
}

function startedMs(job: TransferJob): number {
  return parseServerTimestampMs(jobStartedAt(job)) ?? 0;
}

// ── Verification ──────────────────────────────────────────────────────────

export type JobVerificationTone = "ok" | "warn" | "danger" | "muted";

export interface JobVerificationView {
  label: string;
  tone: JobVerificationTone;
  /** Tooltip: what the verdict means, from the stored report when present. */
  title: string;
  /** Rows held out of the destination (quarantine) — a separate fact from Gate-8. */
  quarantined: number;
}

const VERIFICATION_BADGE: Record<JobVerificationTone, string> = {
  ok: "df2-badge df2-badge-live",
  warn: "df2-badge df2-badge-warn",
  danger: "df2-badge df2-badge-error",
  muted: "df2-badge df2-badge-muted",
};

export function verificationBadgeClass(tone: JobVerificationTone): string {
  return VERIFICATION_BADGE[tone];
}

/**
 * The Verification column for one list row. Uses the detail card's rules
 * (`classifyGate8Status`) on the bounded Gate-8 summary the list carries, so
 * the grid and the detail never disagree; with no stored verdict it says so.
 */
export function jobVerification(job: TransferJob): JobVerificationView {
  const status = presentedJobStatus(job);
  const quarantined = Math.max(0, Number(job.rejected_rows ?? 0) || 0);
  if (LIVE_JOB_STATUSES.has(status)) {
    return { label: "Pending", tone: "muted", title: "Gate-8 verification runs after the load finishes.", quarantined };
  }
  const summary = job.verification_summary;
  if (summary && typeof summary === "object" && Object.keys(summary).length) {
    const report = presentMultiStreamGate8(summary, {
      multi_stream: job.multi_stream,
      streams: job.stream_names,
    }) ?? summary;
    const view = classifyGate8Status(report);
    const message = String(report.message || "").trim();
    return {
      label: view.label,
      tone: view.tone,
      title: message ? `Gate-8: ${view.label}. ${message}` : `Gate-8: ${view.label}.`,
      quarantined,
    };
  }
  if (status === "failed" || status === "cancelled") {
    return {
      label: "Not run",
      tone: "muted",
      title: "The run stopped before Gate-8 verification recorded a verdict.",
      quarantined,
    };
  }
  return {
    label: "Not captured",
    tone: "muted",
    title: "This run recorded no Gate-8 verdict. Open the job for its stored evidence.",
    quarantined,
  };
}

/** "Needs attention": a failed run, or a run whose Gate-8 verification failed. */
export function jobNeedsAttention(job: TransferJob): boolean {
  return presentedJobStatus(job) === "failed" || job.verification_summary?.passed === false;
}

// ── Columns, sort, density ────────────────────────────────────────────────

export type JobsGridColumnId =
  | "status"
  | "job"
  | "route"
  | "mode"
  | "rows"
  | "verification"
  | "duration"
  | "started";

export interface JobsGridColumn {
  id: JobsGridColumnId;
  label: string;
  /** Status and Job identify the row and cannot be hidden. */
  required?: boolean;
  numeric?: boolean;
}

export const JOBS_GRID_COLUMNS: readonly JobsGridColumn[] = [
  { id: "status", label: "Status", required: true },
  { id: "job", label: "Job", required: true },
  { id: "route", label: "Route" },
  { id: "mode", label: "Mode" },
  { id: "rows", label: "Rows", numeric: true },
  { id: "verification", label: "Verification" },
  { id: "duration", label: "Duration", numeric: true },
  { id: "started", label: "Started" },
];

export type JobsGridSortDir = "asc" | "desc";

export interface JobsGridSort {
  column: JobsGridColumnId;
  dir: JobsGridSortDir;
}

/** The server's own order: newest first. */
export const DEFAULT_JOBS_GRID_SORT: JobsGridSort = { column: "started", dir: "desc" };

export function isDefaultJobsGridSort(sort: JobsGridSort): boolean {
  return sort.column === DEFAULT_JOBS_GRID_SORT.column && sort.dir === DEFAULT_JOBS_GRID_SORT.dir;
}

/** Click a header: a new column sorts its natural way first, the same column flips. */
export function nextJobsGridSort(current: JobsGridSort, column: JobsGridColumnId): JobsGridSort {
  if (current.column === column) return { column, dir: current.dir === "asc" ? "desc" : "asc" };
  const numericFirstDesc = column === "started" || column === "rows" || column === "duration";
  return { column, dir: numericFirstDesc ? "desc" : "asc" };
}

const STATUS_RANK: Record<string, number> = {
  failed: 0,
  running: 1,
  pending: 2,
  queued: 2,
  retrying: 2,
  needs_approval: 3,
  cancelled: 4,
  completed_with_quarantine: 5,
  completed_with_warnings: 5,
};

const VERIFICATION_RANK: Record<JobVerificationTone, number> = { danger: 0, warn: 1, muted: 2, ok: 3 };

export interface JobsGridRowText {
  name: (job: TransferJob) => string;
}

function sortKey(job: TransferJob, column: JobsGridColumnId, text: JobsGridRowText): string | number | null {
  switch (column) {
    case "status": {
      const status = presentedJobStatus(job);
      return STATUS_RANK[status] ?? (isJobSuccess(status) ? 6 : 7);
    }
    case "job":
      return text.name(job).toLowerCase();
    case "route":
      return jobRouteLabel(job).toLowerCase();
    case "mode":
      return job.sync_mode ? formatSyncModeLabel(job.sync_mode).toLowerCase() : null;
    case "rows":
      return job.records_processed ?? null;
    case "verification":
      return VERIFICATION_RANK[jobVerification(job).tone];
    case "duration":
      return durationMs(job);
    case "started":
      return startedMs(job);
    default:
      return null;
  }
}

/**
 * Sort the loaded page. The default sort keeps the server's order (newest
 * first); every other sort is stable and puts rows with no value last in
 * either direction, so "—" never floats to the top.
 */
export function sortJobsForGrid(jobs: TransferJob[], sort: JobsGridSort, text: JobsGridRowText): TransferJob[] {
  if (isDefaultJobsGridSort(sort)) return jobs;
  const factor = sort.dir === "asc" ? 1 : -1;
  return jobs
    .map((job, index) => ({ job, index, key: sortKey(job, sort.column, text) }))
    .sort((a, b) => {
      if (a.key == null && b.key == null) return a.index - b.index;
      if (a.key == null) return 1;
      if (b.key == null) return -1;
      const cmp = typeof a.key === "number" && typeof b.key === "number"
        ? a.key - b.key
        : String(a.key).localeCompare(String(b.key));
      return cmp === 0 ? a.index - b.index : cmp * factor;
    })
    .map((entry) => entry.job);
}

export type JobsGridDensity = "compact" | "comfortable";

export interface JobsGridPrefs {
  density: JobsGridDensity;
  hidden: JobsGridColumnId[];
}

export const DEFAULT_JOBS_GRID_PREFS: JobsGridPrefs = { density: "compact", hidden: [] };

const PREFS_PREFIX = "df2.jobsGrid.v1";

/** One key per signed-in operator, so a shared browser keeps each person's layout. */
export function jobsGridPrefsKey(): string {
  let who = "anonymous";
  try {
    who = (readSession()?.email || "").trim().toLowerCase() || "anonymous";
  } catch {
    who = "anonymous";
  }
  return `${PREFS_PREFIX}:${who}`;
}

const HIDEABLE = new Set(JOBS_GRID_COLUMNS.filter((c) => !c.required).map((c) => c.id));

export function readJobsGridPrefs(key = jobsGridPrefsKey()): JobsGridPrefs {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return DEFAULT_JOBS_GRID_PREFS;
    const parsed = JSON.parse(raw) as Partial<JobsGridPrefs>;
    const density: JobsGridDensity = parsed.density === "comfortable" ? "comfortable" : "compact";
    const hidden = Array.isArray(parsed.hidden)
      ? parsed.hidden.filter((id): id is JobsGridColumnId => HIDEABLE.has(id as JobsGridColumnId))
      : [];
    return { density, hidden: Array.from(new Set(hidden)) };
  } catch {
    return DEFAULT_JOBS_GRID_PREFS;
  }
}

export function writeJobsGridPrefs(prefs: JobsGridPrefs, key = jobsGridPrefsKey()): void {
  try {
    window.localStorage.setItem(key, JSON.stringify(prefs));
  } catch {
    // Private mode / quota: the layout still applies for this visit.
  }
}

// ── Display text shared by grid and CSV ───────────────────────────────────

export function jobEngineLabel(type: string | undefined): string {
  const id = String(type || "").trim();
  return id ? connectorDisplayName(id) : "—";
}

export function jobModeLabel(job: Pick<TransferJob, "sync_mode">): string {
  return formatSyncModeLabel(job.sync_mode);
}

/** Drawer subtitle: the engines and mode, which the detail header below does not repeat. */
export function jobDetailSubtitle(job: Pick<TransferJob, "source_type" | "destination_type" | "sync_mode">): string {
  const route = `${jobEngineLabel(job.source_type)} → ${jobEngineLabel(job.destination_type)}`;
  return job.sync_mode ? `${route} · ${jobModeLabel(job)}` : route;
}

export function formatJobStarted(iso: string | undefined, now = new Date()): string {
  if (!iso) return "—";
  const ms = parseServerTimestampMs(iso);
  if (ms == null) return "—";
  const date = new Date(ms);
  const time = date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  if (date.toDateString() === now.toDateString()) return `Today ${time}`;
  return date.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

// ── CSV export of the current page ────────────────────────────────────────

/** RFC 4180 quoting, and a formula-looking cell is defused for spreadsheet apps. */
export function csvCell(value: unknown): string {
  let text = value == null ? "" : String(value);
  if (/^[=+\-@\t\r]/.test(text)) text = `'${text}`;
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export const JOBS_CSV_HEADER = [
  "Job ID",
  "Job",
  "Status",
  "Source engine",
  "Source",
  "Destination engine",
  "Route",
  "Mode",
  "Rows processed",
  "Rows (dest proof)",
  "Verification",
  "Quarantined rows",
  "Duration",
  "Started (UTC)",
  "Completed (UTC)",
  "Error",
] as const;

function isoUtc(value: string | undefined): string {
  if (!value) return "";
  const ms = parseServerTimestampMs(value);
  return ms == null ? value : new Date(ms).toISOString();
}

export function jobsCsv(jobs: TransferJob[], text: JobsGridRowText): string {
  const rows = jobs.map((job) => {
    const status = presentedJobStatus(job);
    const verification = jobVerification(job);
    return [
      job._id,
      text.name(job),
      jobStatusLabel(status),
      job.source_type || "",
      job.source_name || "",
      job.destination_type || "",
      jobRouteLabel(job),
      job.sync_mode ? jobModeLabel(job) : "",
      (job as { records_processed_unknown?: boolean }).records_processed_unknown ? "" : job.records_processed ?? "",
      destMetricCompact(formatJobRowMetric(job)),
      verification.label,
      verification.quarantined || "",
      formatJobDuration(job.started_at, job.completed_at) || "",
      isoUtc(jobStartedAt(job)),
      isoUtc(job.completed_at),
      job.error || "",
    ];
  });
  return [JOBS_CSV_HEADER as readonly unknown[], ...rows].map((row) => row.map(csvCell).join(",")).join("\r\n") + "\r\n";
}

export function jobsCsvFilename(now = new Date()): string {
  const stamp = now.toISOString().slice(0, 16).replace(/[:T]/g, "-");
  return `datawrap-jobs-${stamp}.csv`;
}
