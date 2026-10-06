/**
 * Operator-visible fields the engine stores on the job or under
 * destination_summary. One reader — Jobs, Theater, the result dashboard,
 * and the SSE normalizer all use this. Do not grow a second health model.
 *
 * Stream order matches services.row_conservation.account_job: a non-empty
 * destination_summary.streams list is the run. job.streams is the fallback
 * (CDC checkpoints promote it before the summary list exists).
 */

import type { CdcStreamHealth, RejectedDetail } from "./types";

type SummaryCarrier = {
  streams?: unknown;
  rejected_rows?: unknown;
  coerced_null_rows?: unknown;
  rejected_details?: unknown;
  rejected_details_total?: unknown;
  warnings?: unknown;
  warnings_suppressed?: unknown;
  foreign_keys?: unknown;
};

export type JobEvidenceCarrier = {
  streams?: unknown;
  rejected_rows?: unknown;
  coerced_null_rows?: unknown;
  rejected_details?: unknown;
  rejected_details_total?: unknown;
  destination_summary?: SummaryCarrier | null;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function summaryOf(job: JobEvidenceCarrier | null | undefined): SummaryCarrier | null {
  const ds = job?.destination_summary;
  return isRecord(ds) ? ds : null;
}

function finiteNumber(value: unknown): number | null {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function streamList(value: unknown): CdcStreamHealth[] {
  if (!Array.isArray(value)) return [];
  const out: CdcStreamHealth[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    const name = String(item.name ?? item.stream ?? "").trim();
    if (!name) continue;
    const processed = finiteNumber(item.records_processed);
    const lag = finiteNumber(item.cdc_lag_seconds);
    const bytes = finiteNumber(item.replication_lag_bytes);
    const watermark = item.watermark == null ? null : String(item.watermark);
    const error = item.error == null || String(item.error).trim() === "" ? null : String(item.error);
    out.push({
      name,
      status: item.status == null ? undefined : String(item.status),
      records_processed: processed ?? undefined,
      cdc_lag_seconds: lag,
      replication_lag_bytes: bytes,
      watermark: watermark || null,
      error,
      row_accounting: isRecord(item.row_accounting)
        ? (item.row_accounting as CdcStreamHealth["row_accounting"])
        : undefined,
    });
  }
  return out;
}

/** Per-stream health for the run. Empty when neither place recorded a named stream. */
export function readJobStreams(job: JobEvidenceCarrier | null | undefined): CdcStreamHealth[] {
  const fromSummary = streamList(summaryOf(job)?.streams);
  if (fromSummary.length) return fromSummary;
  return streamList(job?.streams);
}

function countFrom(top: unknown, nested: unknown): number {
  const topN = finiteNumber(top);
  if (topN != null) return topN;
  return finiteNumber(nested) ?? 0;
}

/** Quarantine count. Top-level wins when stamped; otherwise the summary (running checkpoints). */
export function readRejectedRows(job: JobEvidenceCarrier | null | undefined): number {
  return countFrom(job?.rejected_rows, summaryOf(job)?.rejected_rows);
}

export function readCoercedNullRows(job: JobEvidenceCarrier | null | undefined): number {
  return countFrom(job?.coerced_null_rows, summaryOf(job)?.coerced_null_rows);
}

/** Finding sample. Prefer a non-empty top-level list, then the summary sample. */
export function readRejectedDetails(
  job: JobEvidenceCarrier | null | undefined,
): RejectedDetail[] | undefined {
  if (Array.isArray(job?.rejected_details) && job.rejected_details.length) {
    return job.rejected_details as RejectedDetail[];
  }
  const nested = summaryOf(job)?.rejected_details;
  if (Array.isArray(nested) && nested.length) return nested as RejectedDetail[];
  if (Array.isArray(job?.rejected_details)) return job.rejected_details as RejectedDetail[];
  return undefined;
}

export function readRejectedDetailsTotal(job: JobEvidenceCarrier | null | undefined): number {
  const top = finiteNumber(job?.rejected_details_total);
  if (top != null) return top;
  const nested = finiteNumber(summaryOf(job)?.rejected_details_total);
  if (nested != null) return nested;
  return readRejectedDetails(job)?.length ?? 0;
}

export type WriterWarnings = {
  messages: string[];
  /** Engine dropped this many past the sample. They are not in `messages`. */
  suppressed: number;
};

/** Destination writer messages. They live on destination_summary, not the job root. */
export function readWriterWarnings(job: JobEvidenceCarrier | null | undefined): WriterWarnings {
  const summary = summaryOf(job);
  const raw = summary?.warnings;
  const messages = Array.isArray(raw)
    ? raw.map((item) => String(item).trim()).filter(Boolean)
    : [];
  const suppressed = finiteNumber(summary?.warnings_suppressed);
  return { messages, suppressed: suppressed != null && suppressed > 0 ? suppressed : 0 };
}

export type ForeignKeyDecisionView = {
  name: string;
  status: string;
  reason: string;
  destTable: string;
  referencedTable: string;
  integrityViolation: boolean;
};

export type ForeignKeyCarryView = {
  verdict: string;
  carried: number;
  integrityViolations: number;
  cycle: string[];
  /** Null when the job predates cycle_resolved — treat a cycle as unresolved. */
  cycleResolved: boolean | null;
  cycleNote: string;
  error: string;
  decisions: ForeignKeyDecisionView[];
};

/**
 * Post-load foreign-key carry (`services.foreign_key_orchestration.summarize`).
 * Returns null when the run did not record a carry.
 */
export function readForeignKeyCarry(job: JobEvidenceCarrier | null | undefined): ForeignKeyCarryView | null {
  const raw = summaryOf(job)?.foreign_keys;
  if (!isRecord(raw)) return null;
  const decisions: ForeignKeyDecisionView[] = [];
  if (Array.isArray(raw.decisions)) {
    for (const item of raw.decisions) {
      if (!isRecord(item)) continue;
      decisions.push({
        name: String(item.name || ""),
        status: String(item.status || "unknown"),
        reason: String(item.reason || item.source_detail || ""),
        destTable: String(item.dest_table || item.table || ""),
        referencedTable: String(item.referenced_table || ""),
        integrityViolation: item.integrity_violation === true,
      });
    }
  }
  const cycle = Array.isArray(raw.cycle)
    ? raw.cycle.map((name) => String(name).trim()).filter(Boolean)
    : [];
  const carried = finiteNumber(raw.carried) ?? decisions.filter((d) => d.status === "carried").length;
  const integrityViolations = finiteNumber(raw.integrity_violations)
    ?? decisions.filter((d) => d.integrityViolation).length;
  const verdict = String(raw.verdict || "");
  const error = String(raw.error || "");
  if (!verdict && !error && decisions.length === 0 && cycle.length === 0) return null;
  return {
    verdict,
    carried,
    integrityViolations,
    cycle,
    cycleResolved: raw.cycle_resolved == null ? null : Boolean(raw.cycle_resolved),
    cycleNote: String(raw.cycle_note || ""),
    error,
    decisions,
  };
}

/** Decisions that are not a quiet successful recreate. */
export function foreignKeyProblems(carry: ForeignKeyCarryView): ForeignKeyDecisionView[] {
  return carry.decisions.filter((d) => d.integrityViolation || d.status !== "carried");
}
