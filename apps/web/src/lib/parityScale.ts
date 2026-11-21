/**
 * Parity at Scale in the Trust Report: the verification that runs after the
 * write, and the difference explorer (Columns / Keys / Values, re-verify one
 * table, repair SQL for the reviewer to run themselves).
 *
 * The API decides every status; these helpers only choose words. Repair SQL is
 * text to read and copy: Datawrap never runs it.
 */

import { API_BASE } from "./types";
import { apiErrorFrom, apiFetch } from "./api";
import type { ParityReport, ParityTone, ParityVerification, ParityVerificationTable } from "./parity";

export interface ExplorerCause {
  code: string;
  label: string;
  rows: number;
  explained: boolean;
}

export interface ExplorerColumn {
  column: string;
  source_column?: string | null;
  /** in_engine: compared inside both databases; streamed_fingerprint: read and compared by Datawrap; not_compared. */
  compared_by: string;
  rows_compared: number;
  rows_differing: number;
  differing_pct: number;
  status: "match" | "explained" | "unexplained" | "excluded" | string;
  causes: ExplorerCause[];
  protected: boolean;
}

export interface ExplorerColumns {
  table: string;
  rows_compared: number;
  columns: ExplorerColumn[];
  complete: boolean;
}

export interface ExplorerKeyCategory {
  count: number;
  keys: string[];
  keys_shown: number;
  at_least: boolean;
}

export interface ExplorerKeys {
  table: string;
  keyless: boolean;
  key_columns: string[];
  key_protected: boolean;
  note: string;
  missing_in_destination: ExplorerKeyCategory;
  extra_in_destination: ExplorerKeyCategory;
  duplicate_keys: ExplorerKeyCategory;
  changed: ExplorerKeyCategory;
}

export interface ExplorerCell {
  column: string;
  source: string | null;
  destination: string | null;
  differs: boolean;
  protected: boolean;
}

export interface ExplorerRow {
  key: Array<string | null>;
  in_source: boolean;
  in_destination: boolean;
  cells: ExplorerCell[];
}

export interface ExplorerValues {
  table: string;
  keyless: boolean;
  offset?: number;
  limit?: number;
  total: number;
  stored?: number;
  rows: ExplorerRow[];
  note?: string;
}

export interface RepairSql {
  table: string;
  available: boolean;
  engine?: string;
  statements?: { insert: number; delete: number; update: number };
  rows_differing?: number;
  sql: string;
  reason?: string;
  executed_by_datawrap?: false;
}

/* ── API ──────────────────────────────────────────────────────── */

function tableUrl(jobId: string, table: string, suffix = ""): string {
  return `${API_BASE}/jobs/${encodeURIComponent(jobId)}/verification/tables/${encodeURIComponent(table)}${suffix}`;
}

async function getJson<T>(url: string, fallback: string, signal?: AbortSignal): Promise<T> {
  const res = await apiFetch(url, { signal });
  if (!res.ok) throw await apiErrorFrom(res, fallback);
  return (await res.json()) as T;
}

export function fetchExplorerColumns(jobId: string, table: string, signal?: AbortSignal): Promise<ExplorerColumns> {
  return getJson(tableUrl(jobId, table, "/columns"), "Could not load the columns of this table.", signal);
}

export function fetchExplorerKeys(jobId: string, table: string, signal?: AbortSignal): Promise<ExplorerKeys> {
  return getJson(tableUrl(jobId, table, "/keys"), "Could not load the keys of this table.", signal);
}

export function fetchExplorerValues(
  jobId: string,
  table: string,
  page: { offset?: number; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ExplorerValues> {
  const q = new URLSearchParams({ offset: String(page.offset ?? 0), limit: String(page.limit ?? 50) });
  return getJson(tableUrl(jobId, table, `/values?${q}`), "Could not read the differing values.", signal);
}

export function fetchRepairSql(jobId: string, table: string, signal?: AbortSignal): Promise<RepairSql> {
  return getJson(tableUrl(jobId, table, "/repair-sql"), "Could not prepare repair statements.", signal);
}

/** Start comparing every row of one table again; the report shows it as running. */
export async function reverifyTable(jobId: string, table: string): Promise<ParityVerification> {
  const res = await apiFetch(tableUrl(jobId, table), { method: "POST", body: "{}", timeoutMs: 60_000 });
  if (!res.ok) throw await apiErrorFrom(res, "Could not start verification of this table.");
  return (await res.json()) as ParityVerification;
}

/* ── Words ────────────────────────────────────────────────────── */

export function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/** True while the comparison of every row is still running for any table. */
export function verificationRunning(report: Pick<ParityReport, "verdict" | "verification">): boolean {
  return report.verdict === "verifying" || report.verification?.state === "running";
}

/** The tables the explorer can open: verified tables whose scan finished. */
export function explorableTables(verification: ParityVerification | null | undefined): ParityVerificationTable[] {
  return (verification?.tables ?? []).filter((t) => t.status !== "running" && t.status !== "error" && t.outcome);
}

/** Steps done of all steps, for a progress bar; null when the scan has not counted its work yet. */
export function verificationProgress(table: ParityVerificationTable): { done: number; total: number; pct: number } | null {
  const done = Math.max(0, Number(table.progress?.items_done ?? 0));
  const queued = Math.max(0, Number(table.progress?.items_queued ?? 0));
  const total = done + queued;
  if (!total) return null;
  return { done, total, pct: Math.min(100, Math.floor((100 * done) / total)) };
}

const TABLE_STATE: Record<string, { label: string; tone: ParityTone }> = {
  running: { label: "Comparing", tone: "muted" },
  passed: { label: "Every row matches", tone: "ok" },
  failed: { label: "Differences found", tone: "warn" },
  incomplete: { label: "Compared in part", tone: "muted" },
  error: { label: "Could not finish", tone: "danger" },
};

export function verificationTableState(status: string): { label: string; tone: ParityTone } {
  return TABLE_STATE[status] ?? { label: "Not compared", tone: "muted" };
}

/** One sentence for the verification card's heading line. */
export function verificationHeadline(verification: ParityVerification): string {
  const n = verification.tables.length;
  switch (verification.state) {
    case "running":
      return `Comparing every row of ${plural(n, "table")} in both databases.`;
    case "passed":
      return `Every row of ${plural(n, "table")} was compared in both databases. None differs.`;
    case "failed":
      return "Every row was compared in both databases. Some rows differ; the explorer below names them.";
    case "incomplete":
      return "Part of a table could not be compared. Re-verify it to compare every row.";
    case "error":
      return "Verification could not finish. Re-verify the table to compare every row.";
    default:
      return verification.reason || "No table needed a separate verification.";
  }
}

const COMPARED_BY: Record<string, string> = {
  in_engine: "In both databases",
  streamed_fingerprint: "Read and compared",
  not_compared: "Not compared",
};

export function comparedByLabel(code: string): string {
  return COMPARED_BY[code] ?? code.replace(/_/g, " ");
}

export function columnStatusPresentation(column: Pick<ExplorerColumn, "status" | "rows_differing">): {
  label: string;
  tone: ParityTone;
} {
  switch (column.status) {
    case "match":
      return { label: "Match", tone: "ok" };
    case "explained":
      return { label: "Explained", tone: "ok" };
    case "unexplained":
      return { label: "Unexplained", tone: "warn" };
    case "excluded":
      return { label: "Not compared", tone: "muted" };
    default:
      return { label: column.status.replace(/_/g, " "), tone: "muted" };
  }
}

/** "0.0042%": small shares never round to zero while rows differ. */
export function differingPct(pct: number, rows: number): string {
  if (!rows) return "0%";
  if (pct < 0.01) return "< 0.01%";
  return `${Number(pct.toFixed(pct < 1 ? 2 : 1))}%`;
}

const PROTECTED = "protected";

/** What a screen reader hears for one value pair: "amount, source 10.50, destination 10.5, differs". */
export function cellAccessibleName(cell: ExplorerCell): string {
  if (cell.protected) return `${cell.column}, protected, value not shown`;
  const side = (v: string | null) => (v == null ? "empty" : v === "" ? "blank" : v);
  return `${cell.column}, source ${side(cell.source)}, destination ${side(cell.destination)}, ${cell.differs ? "differs" : "same"}`;
}

/** How a value reads in a cell: NULL and blank are told apart, protected never shows a value. */
export function cellText(value: string | null, isProtected: boolean): string {
  if (isProtected || value === PROTECTED) return "Protected";
  if (value == null) return "NULL";
  return value === "" ? "(blank)" : value;
}

/** The repair preview's one-line summary: "1 insert, 1 delete, 1 update". */
export function repairSummary(repair: RepairSql): string {
  const s = repair.statements ?? { insert: 0, delete: 0, update: 0 };
  const parts = [
    s.insert ? plural(s.insert, "insert") : "",
    s.delete ? plural(s.delete, "delete") : "",
    s.update ? plural(s.update, "update") : "",
  ].filter(Boolean);
  return parts.length ? parts.join(", ") : "No statements";
}
