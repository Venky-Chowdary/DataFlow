/** Gate-8 reconciliation proof — source vs destination rows + checksums. */

import type { Gate8ReconciliationPayload } from "../../lib/types";

/**
 * The card renders the engine payload verbatim — one shape, defined in
 * `lib/types.ts`. A second local copy is how honesty fields (identity,
 * alignment, read-back error) silently fell off the card before.
 */
export type Gate8Reconciliation = Gate8ReconciliationPayload;
export type Gate8Identity = NonNullable<Gate8ReconciliationPayload["identity"]>;
export type Gate8SampleMismatch = NonNullable<
  NonNullable<Gate8ReconciliationPayload["sample_compare"]>["mismatches"]
>[number];

interface Gate8ProofCardProps {
  report: Gate8Reconciliation;
  /** Optional plain-language pipeline explanation from the engine. */
  explanation?: string;
  className?: string;
  compact?: boolean;
  /** Closed-loop: open Map / Validate when reconcile fails. */
  onOpenValidate?: () => void;
  /** Override default “Open Validate / Map” label when already on Validate. */
  onOpenValidateLabel?: string;
  /** Closed-loop: jump to quarantine findings (inspect only — never mutate). */
  onOpenQuarantine?: () => void;
  /** Closed-loop: re-run Validate (or transfer when wired from Jobs). */
  onRerun?: () => void;
  /** Override default “Re-run transfer” label (Validate uses Re-run Validate). */
  onRerunLabel?: string;
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
  if (phase.includes("sample_verified")) return false;
  if (phase.includes("writer_ack")) return true;
  const msg = String(report.message || "").toLowerCase();
  if (/sample-verified|sample verified|key-aligned field/i.test(msg)) return false;
  if (/verified by writer|read-back verifier not available/i.test(msg)) return true;
  if (report.passed && report.source_checksum && !report.target_checksum) {
    // Keyed sample proof upgrades writer-ack when present.
    const compared = Number(report.sample_compare?.compared ?? 0);
    if (compared > 0 && report.sample_compare?.passed !== false) return false;
    return true;
  }
  return false;
}

/** True when evidence is keyed sample read-back (SaaS / Kafka reverse-ETL class). */
export function isGate8SampleVerified(report: Gate8Reconciliation): boolean {
  const phase = String(report.phase || "").toLowerCase();
  if (phase.includes("sample_verified")) return true;
  const msg = String(report.message || "").toLowerCase();
  if (/sample-verified|sample verified/i.test(msg)) return true;
  const compared = Number(report.sample_compare?.compared ?? 0);
  return Boolean(
    report.passed
    && compared > 0
    && report.sample_compare?.passed !== false
    && !String(report.target_checksum || "").trim()
  );
}

/** True when evidence is pre-write only — never show Verified / match claims. */
export function isGate8PreWriteSimulation(report: Gate8Reconciliation): boolean {
  // Writer-ack is post-write limited proof — not a pre-write simulation.
  if (isGate8WriterAckOnly(report)) return false;
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

export type Gate8StatusView = {
  label: string;
  tone: "ok" | "warn" | "danger" | "muted";
  /** Independent post-write source↔dest proof — not writer-ack or pre-write. */
  fullPass: boolean;
};

/** Shared operator label for Gate-8 — never call writer-ack / pre-write “Passed”. */
/**
 * True when the engine compared values but could not prove row identity
 * (no primary key, or null/duplicate keys). Positional matches are not
 * keyed fidelity proof and must never render as a clean pass.
 */
export function isGate8IdentityUnproven(report: Gate8Reconciliation): boolean {
  const mode = String(report.verification_mode || "").toLowerCase();
  if (mode === "unproven_identity" || mode === "positional_only") return true;
  if (report.identity && report.identity.proven === false) return true;
  const alignment = String(report.sample_compare?.alignment || "").toLowerCase();
  if (alignment === "unproven_identity" || alignment === "positional_only") return true;
  return Boolean(report.sample_compare?.identity_warning);
}

export function classifyGate8Status(
  report: Gate8Reconciliation | null | undefined,
): Gate8StatusView {
  if (!report) {
    return { label: "Pending", tone: "muted", fullPass: false };
  }
  if (report.passed === false) {
    return { label: "Failed", tone: "danger", fullPass: false };
  }
  if (isGate8WriterAckOnly(report)) {
    return { label: "Writer ack", tone: "warn", fullPass: false };
  }
  // A positional / unproven-identity compare is not keyed fidelity proof —
  // labelling it "Passed" is the false-proof the engine explicitly refuses.
  if (isGate8IdentityUnproven(report)) {
    return { label: "Unproven identity", tone: "warn", fullPass: false };
  }
  if (isGate8SampleVerified(report)) {
    return { label: "Sample verified", tone: "ok", fullPass: true };
  }
  if (isGate8PreWriteSimulation(report)) {
    return { label: "Pre-write only", tone: "warn", fullPass: false };
  }
  if (report.passed === true) {
    return { label: "Passed", tone: "ok", fullPass: true };
  }
  return { label: "Pending", tone: "muted", fullPass: false };
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
  onOpenValidateLabel = "Open Validate / Map",
  onOpenQuarantine,
  onRerun,
  onRerunLabel = "Re-run transfer",
}: Gate8ProofCardProps) {
  const preWrite = isGate8PreWriteSimulation(report);
  const sampleVerified = !preWrite && isGate8SampleVerified(report);
  const writerAck = !preWrite && !sampleVerified && isGate8WriterAckOnly(report);
  const passed = Boolean(report.passed) && !preWrite && !writerAck;
  const simulationOk = Boolean(report.passed) && preWrite;
  const writerAckOk = Boolean(report.passed) && writerAck;
  const sourceRows = Number(report.source_rows ?? 0);
  const targetRows = Number(report.target_rows ?? 0);
  const rejectedRows = Number(report.rejected_rows ?? 0);
  const coercedNullRows = Number(report.coerced_null_rows ?? 0);
  const rowsSkipped = Number(report.rows_skipped ?? 0);
  // Quarantine hold-outs + intentional LSN-guard skips are counted in
  // source_rows but not written — delta vs expected, not raw source−dest.
  const heldOut = Math.max(rejectedRows - coercedNullRows, 0);
  const expectedRows = Math.max(sourceRows - heldOut - rowsSkipped, 0);
  const delta = targetRows - expectedRows;
  const mismatches = report.sample_compare?.mismatches ?? [];
  const sampleSkipped = Boolean(report.sample_compare?.skipped);
  const sampleError = String(report.sample_compare?.error || "").trim();
  const alignment = String(report.sample_compare?.alignment || "").toLowerCase();
  const identityWarning = String(
    report.sample_compare?.identity_warning
    || (report.identity?.proven === false ? report.identity?.reason : "")
    || "",
  ).trim();
  const identityUnproven = isGate8IdentityUnproven(report);
  const fidelity = report.row_fidelity_score;
  const missingKeys = Number(report.missing_key_count ?? 0);
  const extraKeys = Number(report.extra_key_count ?? 0);
  const hasFindings = !passed && !simulationOk && !writerAckOk
    || mismatches.length > 0
    || missingKeys > 0
    || extraKeys > 0
    || heldOut > 0
    || identityUnproven
    || Boolean(sampleError)
    || sampleSkipped;

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
      : identityUnproven && (passed || sampleVerified)
        ? "Sample compared — identity not proven"
        : sampleVerified
          ? "Keyed sample read-back matched (reverse-ETL class proof)"
          : (passed ? "Source and destination match" : "Reconciliation did not verify");
  const badge = preWrite
    ? (simulationOk ? "Pending" : "Failed")
    : writerAck
      ? (writerAckOk ? "Writer ack" : "Failed")
      : identityUnproven && (passed || sampleVerified)
        ? "Unproven identity"
        : sampleVerified
          ? "Sample verified"
          : (passed ? "Verified" : "Failed");
  const badgeClass = preWrite
    ? (simulationOk ? "is-pending" : "is-bad")
    : writerAck
      ? (writerAckOk ? "is-pending" : "is-bad")
      : identityUnproven && (passed || sampleVerified)
        ? "is-pending"
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

      {identityWarning && (
        <p className="df2-gate8-proof-message is-warn" role="status">
          Identity caveat: {identityWarning}
          {alignment === "positional_only"
            ? " Comparison is positional only — do not treat this as keyed fidelity proof."
            : ""}
        </p>
      )}

      {sampleError && (
        <p className="df2-gate8-proof-message is-warn" role="alert">
          Read-back failed: {sampleError}
        </p>
      )}

      {sampleSkipped && !sampleError && (
        <p className="df2-gate8-proof-message is-warn" role="status">
          Value read-back was not performed — row counts/checksums alone do not prove
          per-cell fidelity.
        </p>
      )}

      {preWrite && (
        <dl className="df2-gate8-proof-grid df2-gate8-proof-phases" aria-label="Reconciliation phases">
          <div>
            <dt>Pre-write simulation</dt>
            <dd className={simulationOk ? "is-ok" : "is-warn"}>
              {simulationOk
                ? `Passed — ${sourceRows.toLocaleString()} sample row(s)`
                : "Needs review"}
            </dd>
          </div>
          <div>
            <dt>Post-write proof</dt>
            <dd className="is-warn">Pending Execute</dd>
          </div>
        </dl>
      )}

      {!preWrite && (
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
          <dt>Expected dest</dt>
          <dd title="source − quarantine hold-outs">
            {expectedRows.toLocaleString()}
          </dd>
        </div>
        <div>
          <dt>Delta vs expected</dt>
          <dd className={delta === 0 ? "is-ok" : "is-warn"}>
            {delta === 0 ? "0" : `${delta > 0 ? "+" : ""}${delta.toLocaleString()}`}
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
        {rowsSkipped > 0 && (
          <div>
            <dt>Skipped (LSN guard)</dt>
            <dd className="is-ok" title="Intentional CDC redelivery skips — not a shortfall">
              {rowsSkipped.toLocaleString()}
            </dd>
          </div>
        )}
        {fidelity != null && Number.isFinite(Number(fidelity)) && (
          <div>
            <dt>Row fidelity</dt>
            <dd className={Number(fidelity) >= 0.95 ? "is-ok" : "is-warn"}>
              {(Number(fidelity) * 100).toFixed(1)}%
            </dd>
          </div>
        )}
        <div>
          <dt>Source checksum</dt>
          <dd title={report.source_checksum || undefined}>
            {shortChecksum(report.source_checksum)}
          </dd>
        </div>
        <div>
          <dt>Destination checksum</dt>
          <dd title={report.target_checksum || undefined}>
            {shortChecksum(report.target_checksum)}
          </dd>
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
      )}

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
                {onOpenValidateLabel}
              </button>
            )}
            {!passed && !preWrite && onOpenQuarantine && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenQuarantine}>
                Inspect sample cells
              </button>
            )}
            {!passed && !preWrite && onRerun && (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onRerun}>
                {onRerunLabel}
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
