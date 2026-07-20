/** Gate-8 reconciliation proof — source vs destination rows + checksums. */

export interface Gate8SampleMismatch {
  row?: string | number;
  source?: string;
  target?: string;
  source_value?: string;
  target_value?: string;
  column?: string;
}

export interface Gate8Reconciliation {
  passed?: boolean;
  message?: string;
  source_rows?: number;
  target_rows?: number;
  source_checksum?: string;
  target_checksum?: string;
  rejected_rows?: number;
  coerced_null_rows?: number;
  missing_key_count?: number;
  extra_key_count?: number;
  matched_key_count?: number;
  row_fidelity_score?: number;
  sample_compare?: {
    passed?: boolean;
    compared?: number;
    skipped?: boolean;
    mismatches?: Gate8SampleMismatch[];
  };
}

interface Gate8ProofCardProps {
  report: Gate8Reconciliation;
  /** Optional plain-language pipeline explanation from the engine. */
  explanation?: string;
  className?: string;
  compact?: boolean;
  /** Closed-loop: open Map / Validate when reconcile fails. */
  onOpenValidate?: () => void;
  /** Closed-loop: jump to quarantine findings. */
  onOpenQuarantine?: () => void;
  /** Closed-loop: re-run the transfer. */
  onRerun?: () => void;
}

function shortChecksum(value?: string): string {
  const v = (value || "").trim();
  if (!v) return "—";
  return v.length > 16 ? `${v.slice(0, 12)}…` : v;
}

function mismatchLabel(m: Gate8SampleMismatch): string {
  const col = m.column || m.source || m.target || "value";
  const row = m.row != null ? `row ${m.row}` : "row ?";
  return `${row} · ${col}: ${String(m.source_value ?? "—")} → ${String(m.target_value ?? "—")}`;
}

function exportGate8Proof(report: Gate8Reconciliation) {
  const blob = new Blob([JSON.stringify(report, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `dataflow-gate8-proof-${Date.now()}.json`;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1500);
}

/**
 * Operator-facing Gate-8 card: what reconciliation is, whether it passed,
 * evidence (row counts + content fingerprints), and next remediation actions.
 */
export function Gate8ProofCard({
  report,
  explanation,
  className = "",
  compact = false,
  onOpenValidate,
  onOpenQuarantine,
  onRerun,
}: Gate8ProofCardProps) {
  const passed = Boolean(report.passed);
  const sourceRows = Number(report.source_rows ?? 0);
  const targetRows = Number(report.target_rows ?? 0);
  const delta = targetRows - sourceRows;
  const mismatches = report.sample_compare?.mismatches ?? [];
  const missingKeys = Number(report.missing_key_count ?? 0);
  const extraKeys = Number(report.extra_key_count ?? 0);
  const hasFindings = !passed || mismatches.length > 0 || missingKeys > 0 || extraKeys > 0;

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
        {(missingKeys > 0 || extraKeys > 0 || report.matched_key_count != null) && (
          <div>
            <dt>Keys</dt>
            <dd className={missingKeys || extraKeys ? "is-warn" : "is-ok"}>
              {report.matched_key_count != null ? `${report.matched_key_count.toLocaleString()} matched` : "—"}
              {missingKeys > 0 ? ` · ${missingKeys.toLocaleString()} missing` : ""}
              {extraKeys > 0 ? ` · ${extraKeys.toLocaleString()} extra` : ""}
            </dd>
          </div>
        )}
      </dl>

      {mismatches.length > 0 && (
        <div className="df2-gate8-proof-mismatches" aria-label="Sample value mismatches">
          <strong>Sample mismatches ({mismatches.length})</strong>
          <ul>
            {mismatches.slice(0, 8).map((m, i) => (
              <li key={`${m.row}-${m.source}-${i}`}>{mismatchLabel(m)}</li>
            ))}
          </ul>
          {mismatches.length > 8 && (
            <p className="df2-gate8-proof-more">+{mismatches.length - 8} more in exported proof</p>
          )}
        </div>
      )}

      {hasFindings && (
        <div className="df2-gate8-proof-next" role="region" aria-label="Next reconciliation step">
          <div className="df2-gate8-proof-next-copy">
            <strong>Next step</strong>
            <p>
              {!passed
                ? "Fix mapping or transforms, inspect quarantine if rows were isolated, then re-run. Export the proof for audit."
                : "Proof passed with findings to review — export if you need an artifact."}
            </p>
          </div>
          <div className="df2-gate8-proof-next-actions">
            {!passed && onOpenValidate && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onOpenValidate}>
                Open Validate / Map
              </button>
            )}
            {!passed && onOpenQuarantine && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenQuarantine}>
                Inspect quarantine
              </button>
            )}
            {!passed && onRerun && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onRerun}>
                Re-run transfer
              </button>
            )}
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-secondary"
              onClick={() => exportGate8Proof(report)}
            >
              Export proof JSON
            </button>
          </div>
        </div>
      )}

      {explanation && (
        <details className="df2-gate8-proof-explain">
          <summary>How this transfer was planned</summary>
          <pre>{explanation}</pre>
        </details>
      )}
    </section>
  );
}
