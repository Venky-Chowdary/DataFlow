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
