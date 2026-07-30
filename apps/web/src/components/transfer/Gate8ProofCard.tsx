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
  preview?: boolean;
  phase?: string;
  post_write_pending?: boolean;
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

/** True when evidence is writer-ack only — not independent source/target Verified. */
export function isGate8WriterAckOnly(report: Gate8Reconciliation): boolean {
  const phase = String(report.phase || "").toLowerCase();
  if (phase.includes("writer_ack")) return true;
  const msg = String(report.message || "").toLowerCase();
  if (/verified by writer|read-back verifier not available/i.test(msg)) return true;
  if (report.passed && report.source_checksum && !report.target_checksum) return true;
  return false;
}

/** True when evidence is pre-write only — never show Verified / match claims. */
export function isGate8PreWriteSimulation(report: Gate8Reconciliation): boolean {
  if (report.preview === true || report.post_write_pending === true) return true;
  if (String(report.phase || "").toLowerCase().includes("pre_write")) return true;
  if (String(report.phase || "").toLowerCase().includes("post_write_pending")) return true;
  const hasChecksums = Boolean(report.source_checksum && report.target_checksum);
  const destRows = Number(report.target_rows ?? 0);
  const msg = String(report.message || "").toLowerCase();
  // Writer digest alone (or duplicated into both sides) is not independent proof.
  if (/writer checksum|still pending|may still be loading|compare still pending/i.test(msg)) {
    return true;
  }
  if (
    hasChecksums
    && report.source_checksum === report.target_checksum
    && !report.sample_compare
    && /writer|pending|loading/i.test(msg)
  ) {
    return true;
  }
  if (!hasChecksums && (destRows === 0 || /after (transfer )?execution|pre-write|pending/i.test(msg))) {
    return true;
  }
  return false;
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
  const preWrite = isGate8PreWriteSimulation(report);
  const writerAck = !preWrite && isGate8WriterAckOnly(report);
  const passed = Boolean(report.passed) && !preWrite && !writerAck;
  const simulationOk = Boolean(report.passed) && preWrite;
  const writerAckOk = Boolean(report.passed) && writerAck;
  const sourceRows = Number(report.source_rows ?? 0);
  const targetRows = Number(report.target_rows ?? 0);
  const rejectedRows = Number(report.rejected_rows ?? 0);
  const coercedNullRows = Number(report.coerced_null_rows ?? 0);
  // Quarantine hold-outs are counted in source_rows but not written — delta vs
  // expected (source − held_out), not raw source−dest (that falsely warns on pass).
  const heldOut = Math.max(rejectedRows - coercedNullRows, 0);
  const expectedRows = Math.max(sourceRows - heldOut, 0);
  const delta = targetRows - expectedRows;
  const mismatches = report.sample_compare?.mismatches ?? [];
  const missingKeys = Number(report.missing_key_count ?? 0);
  const extraKeys = Number(report.extra_key_count ?? 0);
  const hasFindings = !passed && !simulationOk && !writerAckOk
    || mismatches.length > 0
    || missingKeys > 0
    || extraKeys > 0
    || heldOut > 0;

  const toneClass = preWrite
    ? (simulationOk ? "is-pending" : "is-fail")
    : writerAck
      ? (writerAckOk ? "is-pending" : "is-fail")
      : (passed ? "is-pass" : "is-fail");
  const title = preWrite
    ? (simulationOk
      ? "Pre-write simulation passed — post-write proof pending"
      : "Pre-write reconciliation did not verify")
    : writerAck
      ? (writerAckOk
        ? "Writer acknowledged — independent read-back not available"
        : "Writer acknowledgment did not verify")
      : (passed ? "Source and destination match" : "Reconciliation did not verify");
  const badge = preWrite
    ? (simulationOk ? "Pending" : "Failed")
    : writerAck
      ? (writerAckOk ? "Writer ack" : "Failed")
      : (passed ? "Verified" : "Failed");
  const badgeClass = preWrite
    ? (simulationOk ? "is-pending" : "is-bad")
    : writerAck
      ? (writerAckOk ? "is-pending" : "is-bad")
      : (passed ? "is-ok" : "is-bad");

  return (
    <section
      className={`df2-gate8-proof ${toneClass}${compact ? " is-compact" : ""} ${className}`.trim()}
      aria-label="Gate-8 reconciliation"
    >
      <header className="df2-gate8-proof-head">
        <div>
          <span className="df2-gate8-proof-kicker">Gate-8 · Reconciliation</span>
          <h3>{title}</h3>
        </div>
        <span className={`df2-gate8-proof-badge ${badgeClass}`}>
          {badge}
        </span>
      </header>

      <p className="df2-gate8-proof-lede">
        {preWrite ? (
          <>
            This is a <strong>pre-write simulation</strong> on sample rows.
            {" "}Post-write <strong>row-count</strong> and <strong>checksum</strong> proof
            is produced only after Execute finishes — never claim “source and destination match”
            before the write.
          </>
        ) : (
          <>
            After the write finishes, DataFlow compares <strong>row counts</strong> and
            {" "}
            <strong>content checksums</strong> so silent truncation or corruption cannot
            look like success. Quarantined rows are <strong>held out</strong> of the primary
            table (not NULL-invented) and still counted in the proof.
          </>
        )}
      </p>

      {report.message && (
        <p className="df2-gate8-proof-message">{report.message}</p>
      )}

      {preWrite && (
        <dl className="df2-gate8-proof-grid df2-gate8-proof-phases" aria-label="Reconciliation phases">
          <div>
            <dt>Pre-write simulation</dt>
            <dd className={simulationOk ? "is-ok" : "is-warn"}>
              {simulationOk ? `Passed — ${sourceRows.toLocaleString()} sample row(s)` : "Needs review"}
            </dd>
          </div>
          <div>
            <dt>Post-write row-count</dt>
            <dd className="is-warn">Pending</dd>
          </div>
          <div>
            <dt>Post-write checksum</dt>
            <dd className="is-warn">Pending</dd>
          </div>
        </dl>
      )}

      <dl className="df2-gate8-proof-grid">
        <div>
          <dt>Source rows</dt>
          <dd>{sourceRows.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Destination rows</dt>
          <dd>{preWrite ? "—" : targetRows.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Expected dest</dt>
          <dd title="source − quarantine hold-outs">
            {preWrite ? "—" : expectedRows.toLocaleString()}
          </dd>
        </div>
        <div>
          <dt>Delta vs expected</dt>
          <dd className={preWrite ? undefined : (delta === 0 ? "is-ok" : "is-warn")}>
            {preWrite ? "—" : (delta === 0 ? "0" : `${delta > 0 ? "+" : ""}${delta.toLocaleString()}`)}
          </dd>
        </div>
        {(heldOut > 0 || coercedNullRows > 0) && (
          <div>
            <dt>Quarantine</dt>
            <dd className={heldOut > 0 || coercedNullRows > 0 ? "is-warn" : "is-ok"}>
              {heldOut > 0 ? `${heldOut.toLocaleString()} held out` : "0 held out"}
              {coercedNullRows > 0 ? ` · ${coercedNullRows.toLocaleString()} coerced NULL` : ""}
            </dd>
          </div>
        )}
        <div>
          <dt>Source checksum</dt>
          <dd title={report.source_checksum || undefined}>
            {preWrite ? "—" : shortChecksum(report.source_checksum)}
          </dd>
        </div>
        <div>
          <dt>Destination checksum</dt>
          <dd title={report.target_checksum || undefined}>
            {preWrite ? "—" : shortChecksum(report.target_checksum)}
          </dd>
        </div>
        <div>
          <dt>Checksums</dt>
          <dd className={
            preWrite
              ? "is-warn"
              : (
                report.source_checksum
                && report.target_checksum
                && report.source_checksum === report.target_checksum
                  ? "is-ok"
                  : "is-warn"
              )
          }>
            {preWrite
              ? "Pending"
              : report.source_checksum && report.target_checksum
                ? (report.source_checksum === report.target_checksum ? "Match" : "Mismatch")
                : "—"}
          </dd>
        </div>
        {(missingKeys > 0 || extraKeys > 0 || report.matched_key_count != null) && !preWrite && (
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

      {(hasFindings || preWrite) && (
        <div className="df2-gate8-proof-next" role="region" aria-label="Next reconciliation step">
          <div className="df2-gate8-proof-next-copy">
            <strong>Next step</strong>
            <p>
              {preWrite
                ? "Run Execute to produce post-write row-count and checksum proof in Job Theater."
                : !passed
                  ? "Fix mapping or transforms, inspect quarantine if rows were isolated, then re-run. Export the proof for audit."
                  : "Proof passed with findings to review — export if you need an artifact."}
            </p>
          </div>
          <div className="df2-gate8-proof-next-actions">
            {!passed && !preWrite && onOpenValidate && (
              <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onOpenValidate}>
                Open Validate / Map
              </button>
            )}
            {!passed && !preWrite && onOpenQuarantine && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenQuarantine}>
                Inspect quarantine
              </button>
            )}
            {!passed && !preWrite && onRerun && (
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
