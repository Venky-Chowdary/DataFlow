/**
 * Quarantine finding shape and the one question replay depends on.
 *
 * Kept out of the panel component so it can be read without rendering React.
 */

export type QuarantineRow = {
  /** Source row number — or, on a transform divert, the record as it reached the step. */
  row?: number | Record<string, unknown>;
  source_row_index?: number;
  column?: string;
  target?: string;
  value?: string | null;
  reason?: string;
  policy?: string;
  values?: Record<string, string | null>;
  source_values?: Record<string, string | null>;
  chars?: string[];
  suggested_transform?: string;
  suggested_fix?: string;
  suggested_target_type?: string;
  retry_status?: string;
  /** Destination DLQ row id — required to stamp `_df_promoted_at` after Promote. */
  _df_qid?: string;
};

/**
 * Whether replay could rebuild a row from this finding.
 *
 * Replay rewrites the stored quarantine payload; a finding that names no column
 * and carries no values dictionary has no payload, so the writer is handed an
 * empty record and the request is refused. Offering the control on an open count
 * alone produced a button that answered every click with the same 400.
 */
/**
 * Preflight blocks are stored on the job before any write. Those findings are
 * not a replay payload — the next action is Validate, not Promote.
 */
export function quarantineEvidenceSource(
  details: QuarantineRow[] | undefined,
  apiSource?: string | null,
): string {
  if (apiSource === "preflight" || apiSource === "write" || apiSource === "dlq") {
    return apiSource;
  }
  if (!details?.length) return apiSource || "none";
  if (details.every((row) => (row.policy || "") === "preflight_quarantine")) {
    return "preflight";
  }
  return "job";
}

export function isReplayable(row: QuarantineRow): boolean {
  const named = String(row.column || row.target || "").trim();
  if (named) return true;
  return (
    Object.keys(row.values ?? {}).length > 0
    || Object.keys(row.source_values ?? {}).length > 0
  );
}

/**
 * 1-based source row of a finding. A transform divert stores the diverted
 * record under ``row`` and the row number under ``source_row_index``; rendering
 * that record as a table cell crashed Job Theater and Transfer Studio.
 */
export function quarantineSourceRow(finding: Pick<QuarantineRow, "row" | "source_row_index">): number | null {
  if (typeof finding.row === "number" && Number.isFinite(finding.row)) return finding.row;
  const idx = finding.source_row_index;
  return typeof idx === "number" && Number.isFinite(idx) ? idx : null;
}
