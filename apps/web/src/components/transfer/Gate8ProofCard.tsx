/** Gate-8 reconciliation proof — source vs destination rows + checksums. */

export interface Gate8Reconciliation {
  passed?: boolean;
  message?: string;
  source_rows?: number;
  target_rows?: number;
  source_checksum?: string;
  target_checksum?: string;
  rejected_rows?: number;
  coerced_null_rows?: number;
}

interface Gate8ProofCardProps {
  report: Gate8Reconciliation;
  /** Optional plain-language pipeline explanation from the engine. */
  explanation?: string;
  className?: string;
  compact?: boolean;
}

function shortChecksum(value?: string): string {
  const v = (value || "").trim();
  if (!v) return "—";
  return v.length > 16 ? `${v.slice(0, 12)}…` : v;
}

/**
 * Operator-facing Gate-8 card: what reconciliation is, whether it passed,
 * and the exact evidence (row counts + content fingerprints).
 */
export function Gate8ProofCard({
  report,
  explanation,
  className = "",
  compact = false,
}: Gate8ProofCardProps) {
  const passed = Boolean(report.passed);
  const sourceRows = Number(report.source_rows ?? 0);
  const targetRows = Number(report.target_rows ?? 0);
  const delta = targetRows - sourceRows;

  return (
    <section
      className={`df2-gate8-proof ${passed ? "is-pass" : "is-fail"}${compact ? " is-compact" : ""} ${className}`.trim()}
      aria-label="Gate-8 reconciliation"
    >
      <header className="df2-gate8-proof-head">
        <div>
          <span className="df2-gate8-proof-kicker">Gate-8 · Reconciliation</span>
          <h3>{passed ? "Source and destination match" : "Reconciliation did not verify"}</h3>
        </div>
        <span className={`df2-gate8-proof-badge ${passed ? "is-ok" : "is-bad"}`}>
          {passed ? "Verified" : "Failed"}
        </span>
      </header>

      <p className="df2-gate8-proof-lede">
        After the write finishes, DataFlow compares <strong>row counts</strong> and
        {" "}
        <strong>content checksums</strong> so silent truncation or corruption cannot
        look like success. This is not the writer checksum alone — it is an independent
        source↔destination proof.
      </p>

      {report.message && (
        <p className="df2-gate8-proof-message">{report.message}</p>
      )}

      <dl className="df2-gate8-proof-grid">
        <div>
          <dt>Source rows</dt>
          <dd>{sourceRows.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Destination rows</dt>
          <dd>{targetRows.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Delta</dt>
          <dd className={delta === 0 ? "is-ok" : "is-warn"}>
            {delta === 0 ? "0" : `${delta > 0 ? "+" : ""}${delta.toLocaleString()}`}
          </dd>
        </div>
        <div>
          <dt>Source checksum</dt>
          <dd title={report.source_checksum || undefined}>{shortChecksum(report.source_checksum)}</dd>
        </div>
        <div>
          <dt>Destination checksum</dt>
          <dd title={report.target_checksum || undefined}>{shortChecksum(report.target_checksum)}</dd>
        </div>
        <div>
          <dt>Checksums</dt>
          <dd className={
            report.source_checksum
            && report.target_checksum
            && report.source_checksum === report.target_checksum
              ? "is-ok"
              : "is-warn"
          }>
            {report.source_checksum && report.target_checksum
              ? (report.source_checksum === report.target_checksum ? "Match" : "Mismatch")
              : "—"}
          </dd>
        </div>
      </dl>

      {explanation && (
        <details className="df2-gate8-proof-explain">
          <summary>How this transfer was planned</summary>
          <pre>{explanation}</pre>
        </details>
      )}
    </section>
  );
}
