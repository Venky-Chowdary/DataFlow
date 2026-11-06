/**
 * Rows a job reports, honestly: a cancel the API could not attribute to the
 * destination records `records_processed: null` (C16) — unknown, never 0.
 */
export function rowsProcessedUnknown(raw: Record<string, unknown>): boolean {
  return raw.records_processed === null && raw.rows_processed == null;
}

export function formatRowsProcessed(job: {
  records_processed?: number | null;
  records_processed_unknown?: boolean;
}): string {
  if (job.records_processed_unknown || job.records_processed == null) return "an unknown number of";
  return Number(job.records_processed).toLocaleString();
}
