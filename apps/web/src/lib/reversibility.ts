/**
 * Reversibility gate (Page C): can a plan's or a job's destination give the
 * source data back?
 *
 * The verdict, every column's invertibility and the gate status are the API's
 * deterministic answer (contract section 1, services/reversibility.py). This
 * module only types that answer and words it: it never upgrades a verdict, and
 * a conditional column checked on a sample is never called proven.
 */

import { humanize, type ParityTone } from "./parity";

export type ReversibilityVerdict = "reversible" | "partly_reversible" | "not_reversible";
export type Invertibility = "invertible" | "lossy" | "conditional";
export type ReversibilityGateStatus = "pass" | "block" | "skip";
export type ColumnCheckStatus = "fits" | "does_not_fit" | "not_checked";

export interface ColumnCheck {
  status: ColumnCheckStatus;
  rows_checked: number;
  rows_not_fitting: number;
}

export interface ColumnInvertibility {
  /** Null for a destination-only column (default:, concat:) and for a row-changing recipe step. */
  source: string | null;
  /** Null for a dropped source column and for a row-changing recipe step. */
  target: string | null;
  source_type: string | null;
  target_type: string | null;
  /** Canonical engine ids: the Map transform first, then recipe steps. */
  transforms: string[];
  invertibility: Invertibility;
  reason_code: string;
  /** A plain sentence naming the column. */
  reason: string;
  check: ColumnCheck | null;
}

export interface ReversibilityTable {
  name: string;
  verdict: ReversibilityVerdict;
  reason: string;
  rollback: "restore_from_destination" | "keep_source_as_fallback";
  rollback_note: string;
  columns: ColumnInvertibility[];
}

export interface LossyColumn {
  table: string;
  column: string;
  reason_code: string;
  reason: string;
}

export interface ReversibilityGate {
  id: "g24_reversibility" | string;
  status: ReversibilityGateStatus;
  message: string;
}

export interface ReversibilityReport {
  scope: "plan" | "job";
  subject_id: string;
  generated_at: string;
  verdict: ReversibilityVerdict;
  verdict_reason: string;
  require_reversibility: boolean;
  gate: ReversibilityGate;
  /** The observed rows conditional columns were checked against: a sample, never the population. */
  data_checked: { rows: number; source: "plan_sample" | "preflight_sample" | "none" | string };
  counts: { invertible: number; conditional: number; lossy: number };
  /** Every lossy column, always. */
  lossy_columns: LossyColumn[];
  tables: ReversibilityTable[];
  rollback_note: string;
}

export const REVERSIBILITY_GATE_ID = "g24_reversibility";

const VERDICTS: Record<ReversibilityVerdict, { label: string; short: string; tone: ParityTone }> = {
  reversible: { label: "Reversible", short: "Reversible", tone: "ok" },
  partly_reversible: { label: "Partly reversible", short: "Partly", tone: "warn" },
  not_reversible: { label: "Not reversible", short: "Not reversible", tone: "danger" },
};

/** Verdict pill. An unknown verdict reads as its own words and never as green. */
export function reversibilityVerdictPresentation(verdict: string): { label: string; short: string; tone: ParityTone } {
  return VERDICTS[verdict as ReversibilityVerdict] ?? { label: humanize(verdict) || "Unknown", short: humanize(verdict) || "Unknown", tone: "muted" };
}

const INVERTIBILITY: Record<Invertibility, { label: string; tone: ParityTone }> = {
  invertible: { label: "Invertible", tone: "ok" },
  conditional: { label: "Conditional", tone: "warn" },
  lossy: { label: "Lossy", tone: "danger" },
};

export function invertibilityPresentation(inv: string): { label: string; tone: ParityTone } {
  return INVERTIBILITY[inv as Invertibility] ?? { label: humanize(inv) || "Unknown", tone: "muted" };
}

/** A conditional column's pill follows its data check: a fit is invertible for the data seen, not proven. */
export function columnPillPresentation(col: Pick<ColumnInvertibility, "invertibility" | "check">): { label: string; tone: ParityTone } {
  if (col.invertibility !== "conditional") return invertibilityPresentation(col.invertibility);
  const status = col.check?.status ?? "not_checked";
  if (status === "fits") return { label: "Fits sample", tone: "muted" };
  if (status === "does_not_fit") return { label: "Does not fit", tone: "danger" };
  return { label: "Not checked", tone: "warn" };
}

const REASON_CODES: Record<string, string> = {
  identity: "Unchanged",
  rename: "Renamed",
  widen: "Widened",
  lossless_cast: "Lossless cast",
  type_preserving_bind: "Type-preserving bind",
  hash_pii: "Hashed (hash_pii)",
  mask_pii: "Masked (mask_pii)",
  redact: "Redacted",
  omit: "Omitted",
  dropped: "Dropped",
  narrowing: "Narrowed",
  truncation: "Truncated",
  rounding: "Rounded",
  concat: "Combined (concat)",
  vector: "Embedded (vector)",
  cast: "Cast",
  normalize: "Normalised",
  replace: "Replaced values",
  default: "Default value",
  assume_timezone: "Time zone assumed",
  type_unknown: "Type not declared",
  row_change: "Changes rows",
  recipe_lossy: "Recipe step changes values",
  recipe_invertible: "Recipe step (invertible)",
  recipe_conditional: "Recipe step (not checked)",
};

/** "hash_pii" -> "Hashed (hash_pii)"; an unknown code still reads as words. */
export function reasonCodeLabel(code: string): string {
  return REASON_CODES[code] ?? humanize(code);
}

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? one : many}`;
}

/**
 * What the data check found for one column, in words. A fit is stated with its
 * sample size and says rows not sampled are not proven (contract decision 3).
 */
export function checkText(check: ColumnCheck | null | undefined): string {
  if (!check) return "";
  if (check.status === "fits") {
    return `Fits the ${plural(check.rows_checked, "observed row")}; not proven for rows not sampled.`;
  }
  if (check.status === "does_not_fit") {
    return `${check.rows_not_fitting.toLocaleString("en-US")} of ${plural(check.rows_checked, "observed row")} would not round-trip.`;
  }
  return "Not checked: no observed values to test.";
}

/** "the 25 rows of the plan sample" — or that no rows were checked. Never population wording. */
export function dataCheckedText(data: ReversibilityReport["data_checked"] | null | undefined): string {
  const rows = data?.rows ?? 0;
  if (!data || data.source === "none" || rows <= 0) {
    return "No observed rows were available, so conditional columns are not checked.";
  }
  const where = data.source === "preflight_sample" ? "the Validate sample" : data.source === "plan_sample" ? "the plan sample" : "a sample";
  return `Conditional columns were checked against ${plural(rows, "observed row")} from ${where}. A sample never proves the full table.`;
}

/** The gate line beside the switch: what Execute will do. */
export function gatePresentation(report: Pick<ReversibilityReport, "gate" | "require_reversibility">): { label: string; tone: ParityTone; detail: string } {
  const status = report.gate?.status;
  if (!report.require_reversibility || status === "skip") {
    return { label: "Not required", tone: "muted", detail: "Turn on Require reversibility to block Execute until every table is reversible." };
  }
  if (status === "block") {
    return { label: "Blocks Execute", tone: "danger", detail: report.gate.message || "The gate blocks this transfer until every table is reversible." };
  }
  return { label: "Gate passes", tone: "ok", detail: report.gate?.message || "Every table is reversible, so the gate passes." };
}

const ROLLBACK_LABELS: Record<string, string> = {
  restore_from_destination: "Restore from destination",
  keep_source_as_fallback: "Keep source as fallback",
};

export function rollbackLabel(rollback: string): string {
  return ROLLBACK_LABELS[rollback] ?? humanize(rollback);
}

const ORDER: Record<string, number> = { lossy: 0, conditional: 1, invertible: 2 };

/**
 * Columns in the order a reviewer needs them: lossy first, then conditional
 * (does-not-fit before not-checked before fits), then invertible.
 */
export function columnsForReview(columns: ColumnInvertibility[]): ColumnInvertibility[] {
  const checkRank = (c: ColumnInvertibility) =>
    c.invertibility !== "conditional" ? 0 : c.check?.status === "does_not_fit" ? 0 : c.check?.status === "not_checked" || !c.check ? 1 : 2;
  return columns
    .map((c, i) => ({ c, i }))
    .sort((a, b) => (ORDER[a.c.invertibility] ?? 3) - (ORDER[b.c.invertibility] ?? 3) || checkRank(a.c) - checkRank(b.c) || a.i - b.i)
    .map(({ c }) => c);
}

/** "email → email_hash", "email (dropped)", "→ region (destination only)", "filter_rows step". */
export function columnLabel(col: Pick<ColumnInvertibility, "source" | "target" | "transforms">): string {
  if (col.source && col.target) return col.source === col.target ? col.source : `${col.source} → ${col.target}`;
  if (col.source) return `${col.source} (not carried)`;
  if (col.target) return `${col.target} (destination only)`;
  return col.transforms.length ? `${col.transforms[0]} step` : "Recipe step";
}

/** Per-table counts by invertibility, for the table row's summary. */
export function tableColumnCounts(table: Pick<ReversibilityTable, "columns">): { invertible: number; conditional: number; lossy: number } {
  const counts = { invertible: 0, conditional: 0, lossy: 0 };
  for (const c of table.columns) {
    if (c.invertibility in counts) counts[c.invertibility as Invertibility] += 1;
  }
  return counts;
}

/** "3 lossy · 1 conditional · 12 invertible" with zero parts left out. */
export function countsText(counts: { invertible: number; conditional: number; lossy: number }): string {
  const parts = [
    counts.lossy ? `${counts.lossy.toLocaleString("en-US")} lossy` : "",
    counts.conditional ? `${counts.conditional.toLocaleString("en-US")} conditional` : "",
    counts.invertible ? `${counts.invertible.toLocaleString("en-US")} invertible` : "",
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "No columns analysed";
}

/** A preflight answer carries `reversibility` on the plan path; anything else has none. */
export function reversibilityFromPreflight(preflight: unknown): ReversibilityReport | null {
  const rev = (preflight as { reversibility?: unknown } | null | undefined)?.reversibility;
  if (!rev || typeof rev !== "object") return null;
  const r = rev as Partial<ReversibilityReport>;
  return r.verdict && Array.isArray(r.tables) && r.counts && r.gate ? (rev as ReversibilityReport) : null;
}
