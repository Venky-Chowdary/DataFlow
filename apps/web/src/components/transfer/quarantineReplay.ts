/**
 * Quarantine finding shape and the one question replay depends on.
 *
 * Kept out of the panel component so it can be read without rendering React.
 */

export type QuarantineRow = {
  row?: number;
  column?: string;
  target?: string;
  value?: string;
  reason?: string;
  policy?: string;
  values?: Record<string, string>;
  source_values?: Record<string, string>;
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
export function isReplayable(row: QuarantineRow): boolean {
  const named = String(row.column || row.target || "").trim();
  if (named) return true;
  return (
    Object.keys(row.values ?? {}).length > 0
    || Object.keys(row.source_values ?? {}).length > 0
  );
}
