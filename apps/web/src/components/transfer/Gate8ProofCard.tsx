/** Gate-8 reconciliation proof — source vs destination rows + checksums. */

import { useEffect, useRef, useState } from "react";
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
  /** When set, Export downloads a signed HMAC proof pack from the API. */
  jobId?: string;
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

function downloadBlob(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.rel = "noopener";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1500);
}

function downloadJson(filename: string, payload: unknown) {
  downloadBlob(filename, new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" }));
}

/** Client-facing run report: rows read/written/quarantined, verdict, signature. */
async function exportMigrationCertificate(jobId: string) {
  try {
    const { fetchMigrationCertificatePdf, fetchMigrationCertificateMarkdown } = await import(
      "../../lib/api"
    );
    try {
      downloadBlob(
        `datawrap-migration-certificate-${jobId}.pdf`,
        await fetchMigrationCertificatePdf(jobId),
      );
      return;
    } catch {
      // A deployment without the PDF renderer still owes the operator the
      // evidence, so fall back to the same content as markdown.
      const markdown = await fetchMigrationCertificateMarkdown(jobId);
      downloadBlob(
        `datawrap-migration-certificate-${jobId}.md`,
        new Blob([markdown], { type: "text/markdown" }),
      );
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : "Certificate export failed";
    window.alert(`Could not export the Migration Certificate.\n${message}`);
  }
}

async function exportGate8Proof(report: Gate8Reconciliation, jobId?: string) {
  if (jobId) {
    try {
      const { fetchSignedProofPack } = await import("../../lib/api");
      const pack = await fetchSignedProofPack(jobId);
      downloadJson(`datawrap-signed-proof-${jobId}.json`, pack);
      return;
    } catch (err) {
      const message = err instanceof Error ? err.message : "Signed proof pack export failed";
      // Do not silently substitute an unsigned snapshot when the CTA promised HMAC.
      window.alert(`Could not export signed proof pack.\n${message}`);
      return;
    }
  }
  downloadJson(`dataflow-gate8-proof-${Date.now()}.json`, {
    honesty: "Unsigned local Gate-8 snapshot — open Jobs with a job id to export HMAC-signed pack.",
    gate8: report,
  });
}

async function verifyProofFile(
  file: File,
): Promise<{ ok: boolean; errors: string[]; content_sha256?: string }> {
  const text = await file.text();
  const pack = JSON.parse(text) as Record<string, unknown>;
  const { verifySignedProofPack } = await import("../../lib/api");
  return verifySignedProofPack(pack);
}

/** Dest was re-read; source digest is the in-process write-pass — not migration_proven. */
export function isGate8WritePassDestReadback(report: Gate8Reconciliation): boolean {
  const assurance = String(report.assurance_level || report.coverage || "").toLowerCase();
  const phase = String(report.phase || "").toLowerCase();
  return assurance === "write_pass_dest_readback" || phase.includes("write_pass");
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
  if (String(report.coverage || "").toLowerCase() === "sample") return true;
  const msg = String(report.message || "").toLowerCase();
  if (/sample-verified|sample verified/i.test(msg)) return true;
  const compared = Number(report.sample_compare?.compared ?? 0);
  const sampleOk = Boolean(
    report.passed
    && compared > 0
    && report.sample_compare?.passed !== false,
  );
  if (!sampleOk) return false;
  // No independent dest digest — sample is the only proof.
  if (!String(report.target_checksum || "").trim()) return true;
  // Whole-table digests diverge — sample authority, not population proof.
  const src = String(report.source_checksum || "").trim();
  const tgt = String(report.target_checksum || "").trim();
  if (src && tgt && src !== tgt) return true;
  return false;
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

/** True when Gate-8 closed on dest-before delta, not comparable whole-table hashes. */
export function isGate8AppendDelta(report: Gate8Reconciliation | null | undefined): boolean {
  if (!report) return false;
  const scope = String(report.checksum_scope || "").toLowerCase();
  if (scope === "whole_table_not_comparable") return true;
  const coverage = String(report.coverage || report.assurance_level || "").toLowerCase();
  const phase = String(report.phase || "").toLowerCase();
  const hashesDiverge = Boolean(
    report.source_checksum
    && report.target_checksum
    && report.source_checksum !== report.target_checksum,
  );
  // coverage/phase row_count alone is not enough — overwrite conservation
  // fixtures reuse that phase. Incomparable hashes + dest-before scope is.
  return (
    (coverage === "row_count" || phase.includes("post_write_row_count"))
    && hashesDiverge
    && report.checksum_match !== true
  );
}

/** True when dest was re-read WHERE pk IN (written keys) — batch proof, not whole-table. */
export function isGate8KeyedBatch(report: Gate8Reconciliation | null | undefined): boolean {
  if (!report) return false;
  return String(report.checksum_scope || "").toLowerCase() === "written_batch_keys";
}

/** Dest-before identity for Full Append / keyed extra dest. Display-only — never recompute conservation. */
export function gate8AppendIdentity(report: Gate8Reconciliation): {
  destBefore: number | null;
  destAfter: number;
  written: number | null;
  expected: number;
  deltaOk: boolean | null;
} {
  const destAfter = Number(report.target_rows ?? 0);
  const destBeforeRaw = report.target_rows_before;
  const destBefore =
    destBeforeRaw != null && Number.isFinite(Number(destBeforeRaw))
      ? Number(destBeforeRaw)
      : null;
  const heldOut = Math.max(
    Number(report.rejected_rows ?? 0) - Number(report.coerced_null_rows ?? 0),
    0,
  );
  const expected = Math.max(
    Number(report.source_rows ?? 0) - heldOut - Number(report.rows_skipped ?? 0),
    0,
  );
  const written = destBefore != null ? destAfter - destBefore : null;
  return {
    destBefore,
    destAfter,
    written,
    expected,
    deltaOk: written != null ? written === expected : null,
  };
}

export type Gate8StatusView = {
  label: string;
  tone: "ok" | "warn" | "danger" | "muted";
  /**
   * Independent full-checksum post-write proof (source↔dest digests match).
   * Sample-verified / writer-ack / pre-write must keep this false — never invent
   * population proof from a keyed sample.
   */
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
  // Declined: the engine refused to pair rows it could not identify. The rows
  // may be perfect and the sample still proved nothing, so this must read as
  // unproven rather than as a clean pass.
  if (alignment === "declined") return true;
  return Boolean(report.sample_compare?.identity_warning);
}

/** Why a read-back sample was not compared, in the operator's terms. */
export function gate8SampleDeclinedReason(report: Gate8Reconciliation): string {
  const alignment = String(report.sample_compare?.alignment || "").toLowerCase();
  if (alignment !== "declined") return "";
  return String(report.sample_compare?.reason || "").trim()
    || "No identity key to align the read-back sample against the source.";
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
  if (isGate8WritePassDestReadback(report)) {
    return { label: "Write-pass + dest read-back", tone: "warn", fullPass: false };
  }
  const provenance = String(report.source_checksum_provenance || "").toLowerCase();
  if (
    report.passed === true
    && provenance === "independent_source_reread"
    && String(report.assurance_level || report.coverage || "").toLowerCase() === "full_checksum"
  ) {
    return { label: "Independent re-read", tone: "ok", fullPass: true };
  }
  // A positional / unproven-identity compare is not keyed fidelity proof —
  // labelling it "Passed" is the false-proof the engine explicitly refuses.
  if (isGate8IdentityUnproven(report)) {
    return { label: "Unproven identity", tone: "warn", fullPass: false };
  }
  if (isGate8SampleVerified(report)) {
    // Sample ≠ population — never green "ok" (Enterprise GA honesty).
    return { label: "Sample verified", tone: "warn", fullPass: false };
  }
  if (isGate8KeyedBatch(report) && report.passed === true) {
    return { label: "Batch verified", tone: "warn", fullPass: false };
  }
  if (isGate8AppendDelta(report)) {
    return { label: "Append delta", tone: "warn", fullPass: false };
  }
  // File/object export: API may set passed=true for operational success while
  // unproven/migration_proven=false — never green "Passed" without read-back.
  // Check before pre-write simulation so post-write file exports are not
  // mislabeled as "Pre-write only".
  if (
    report.unproven === true
    || report.skipped_readback === true
  ) {
    return { label: "Unproven (no read-back)", tone: "warn", fullPass: false };
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
  jobId,
  onOpenValidate,
  onOpenValidateLabel = "Open Validate / Map",
  onOpenQuarantine,
  onRerun,
  onRerunLabel = "Re-run transfer",
}: Gate8ProofCardProps) {
  const verifyInputRef = useRef<HTMLInputElement>(null);
  const [verifyState, setVerifyState] = useState<
    | { status: "idle" }
    | { status: "working" }
    | { status: "ok"; sha?: string }
    | { status: "fail"; errors: string[] }
  >({ status: "idle" });
  const [packSummary, setPackSummary] = useState<{
    migration_proven?: boolean;
    claim_level?: string;
    accepted_risks?: number;
    incomplete?: string[];
    versions_honesty?: string;
    ddl_hash?: string | null;
    mapping_hash?: string | null;
    rollback?: string;
  } | null>(null);
  useEffect(() => {
    setVerifyState({ status: "idle" });
  }, [jobId, report.source_checksum, report.target_checksum, report.phase, report.passed]);
  useEffect(() => {
    setPackSummary(null);
    if (!jobId) return;
    let cancelled = false;
    void (async () => {
      try {
        const { fetchSignedProofPack } = await import("../../lib/api");
        const pack = await fetchSignedProofPack(jobId);
        if (cancelled) return;
        const assurance = (pack.assurance || {}) as Record<string, unknown>;
        const hashes = (pack.hashes || {}) as Record<string, unknown>;
        const rb = (pack.rollback_plan || {}) as Record<string, unknown>;
        const risks = Array.isArray(pack.accepted_risks) ? pack.accepted_risks : [];
        const incomplete = Array.isArray(pack.proof_incomplete_reasons)
          ? (pack.proof_incomplete_reasons as string[])
          : [];
        setPackSummary({
          migration_proven: Boolean(assurance.migration_proven),
          claim_level: String(assurance.claim_level || ""),
          accepted_risks: risks.length,
          incomplete: incomplete.slice(0, 4),
          versions_honesty: String(pack.connector_versions_honesty || ""),
          ddl_hash: hashes.ddl_hash != null ? String(hashes.ddl_hash) : null,
          mapping_hash: hashes.mapping_hash != null ? String(hashes.mapping_hash) : null,
          rollback: rb.strategy
            ? `${rb.strategy}${rb.executable ? " (executable staging discard)" : " (not executable)"}`
            : undefined,
        });
      } catch {
        if (!cancelled) setPackSummary(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [jobId, report.source_checksum, report.target_checksum, report.phase, report.passed]);
  const preWrite = isGate8PreWriteSimulation(report);
  const sampleVerified = !preWrite && isGate8SampleVerified(report);
  const keyedBatch = !preWrite && !sampleVerified && isGate8KeyedBatch(report);
  const appendDelta = !preWrite && !sampleVerified && !keyedBatch && isGate8AppendDelta(report);
  const writerAck = !preWrite && !sampleVerified && !appendDelta && !keyedBatch && isGate8WriterAckOnly(report);
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
  const appendId = gate8AppendIdentity(report);
  const showAppendIdentity = Boolean((appendDelta || keyedBatch) && appendId.destBefore != null);
  const mismatches = report.sample_compare?.mismatches ?? [];
  const sampleSkipped = Boolean(report.sample_compare?.skipped);
  const declinedReason = gate8SampleDeclinedReason(report);
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

  const fullChecksumPass = passed && !sampleVerified && !writerAck && !preWrite && !identityUnproven && !appendDelta && !keyedBatch;
  const toneClass = preWrite
    ? (simulationOk ? "is-pending" : "is-fail")
    : writerAck
      ? (writerAckOk ? "is-pending" : "is-fail")
      : sampleVerified || identityUnproven || appendDelta || keyedBatch
        ? "is-pending"
        : (fullChecksumPass ? "is-pass" : (passed ? "is-pending" : "is-fail"));
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
          ? "Keyed sample matched — not population / migration_proven"
          : keyedBatch && passed
            ? "This run’s rows verified — extra destination rows are outside this proof"
            : appendDelta && passed
              ? "Append delta verified — whole-table checksums not comparable"
              : (fullChecksumPass ? "Source and destination match" : "Reconciliation did not verify");
  const badge = preWrite
    ? (simulationOk ? "Pending" : "Failed")
    : writerAck
      ? (writerAckOk ? "Writer ack" : "Failed")
      : identityUnproven && (passed || sampleVerified)
        ? "Unproven identity"
        : sampleVerified
          ? "Sample only"
          : keyedBatch && passed
            ? "Batch"
            : appendDelta && passed
              ? "Row count"
              : (fullChecksumPass ? "Verified" : "Failed");
  const badgeClass = preWrite
    ? (simulationOk ? "is-pending" : "is-bad")
    : writerAck
      ? (writerAckOk ? "is-pending" : "is-bad")
      : identityUnproven || sampleVerified || ((appendDelta || keyedBatch) && passed)
        ? "is-pending"
        : (fullChecksumPass ? "is-ok" : "is-bad");

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
        ) : appendDelta || keyedBatch ? (
          <>
            Full Append into a table that already held rows cannot compare whole-table
            checksums. The identity is <strong>dest after − dest before</strong>
            {keyedBatch ? " plus a digest of the keys this run wrote" : ""}.
            Per-cell / population fidelity is <strong>not</strong> proven — use overwrite
            or upsert with a primary key for <strong>full_checksum</strong>.
          </>
        ) : (
          <>
            After the write finishes, Datawrap compares <strong>row counts</strong> and
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

      {declinedReason && !sampleError && (
        <p className="df2-gate8-proof-message is-warn" role="status">
          <strong>Per-row sample not compared.</strong> {declinedReason} Row
          counts and checksums still apply; per-cell fidelity is unproven for
          this run.
        </p>
      )}

      {sampleSkipped && !sampleError && !declinedReason && (
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
        {showAppendIdentity ? (
          <>
            <div>
              <dt>Dest before</dt>
              <dd>{appendId.destBefore!.toLocaleString()}</dd>
            </div>
            <div>
              <dt>This run wrote</dt>
              <dd className={appendId.deltaOk ? "is-ok" : "is-warn"}>
                {appendId.written!.toLocaleString()}
                {appendId.expected !== appendId.written
                  ? ` · expected ${appendId.expected.toLocaleString()}`
                  : ""}
              </dd>
            </div>
            <div>
              <dt>Dest after</dt>
              <dd>{appendId.destAfter.toLocaleString()}</dd>
            </div>
            <div>
              <dt>Dest Δ</dt>
              <dd className={appendId.deltaOk ? "is-ok" : "is-warn"}>
                {appendId.written != null
                  ? `${appendId.written > 0 ? "+" : ""}${appendId.written.toLocaleString()}`
                  : "—"}
              </dd>
            </div>
          </>
        ) : (
          <>
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
          </>
        )}
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
            appendDelta
              ? "is-warn"
              : report.source_checksum
                && report.target_checksum
                && report.source_checksum === report.target_checksum
                ? "is-ok"
                : "is-warn"
          }>
            {appendDelta
              ? "Not comparable"
              : keyedBatch && report.source_checksum && report.target_checksum
                && report.source_checksum === report.target_checksum
                ? "Match (written keys)"
                : report.source_checksum && report.target_checksum
                  ? (report.source_checksum === report.target_checksum ? "Match" : "Mismatch")
                  : "—"}
          </dd>
        </div>
        {report.sample_compare?.sample_seed?.method === "stratified" && (
          <p className="df2-muted" style={{ fontSize: 12, marginTop: 6 }}>
            Sample plan: <strong>stratified</strong>
            {report.sample_compare.sample_seed.stratify_by
              ? ` by ${String(report.sample_compare.sample_seed.stratify_by)}`
              : ""}
            {report.sample_compare.sample_seed.auto_selected ? " (auto)" : ""}
            {" — "}rare categories preferred within this sample. Still{" "}
            <strong>sample coverage</strong>, not population proof.
          </p>
        )}
        {report.sample_compare?.sample_seed?.content_sha256 && (
          <div>
            <dt>Sample seed</dt>
            <dd
              title={`method=${report.sample_compare.sample_seed.method || "?"} size=${report.sample_compare.sample_seed.size ?? "?"}`}
            >
              {shortChecksum(report.sample_compare.sample_seed.content_sha256)}
            </dd>
          </div>
        )}
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

      {report.match_summary && (
        <div className="df2-gate8-match" aria-label="What was compared">
          <strong>What was compared</strong>
          <dl>
            <div>
              <dt>Populations</dt>
              <dd>
                source {(report.match_summary.source_rows ?? 0).toLocaleString()} ·
                {" "}destination {(report.match_summary.dest_rows ?? 0).toLocaleString()}
                {report.match_summary.dest_rows_before != null
                  ? ` (held ${report.match_summary.dest_rows_before.toLocaleString()} before this run)`
                  : ""}
              </dd>
            </div>
            <div>
              <dt>Cells agreeing</dt>
              {/* Null percent is "not measured", never 0% — an unmeasured
                  comparison must not read as total disagreement. */}
              <dd className={report.match_summary.sample_match_percent == null
                ? "is-warn"
                : report.match_summary.sample_match_percent === 100 ? "is-ok" : "is-warn"}>
                {report.match_summary.sample_match_percent == null
                  ? "not measured"
                  : `${report.match_summary.sample_match_percent}%`}
              </dd>
            </div>
            <div>
              <dt>Of</dt>
              <dd>{report.match_summary.denominator}</dd>
            </div>
          </dl>
          {(report.remediation?.length ?? 0) > 0 && (
            <ol className="df2-gate8-match-fix">
              {report.remediation!.slice(0, 4).map((r, i) => (
                <li key={`${r.action}-${i}`}>
                  <strong>{r.label}</strong>
                  <span>{r.why}</span>
                </li>
              ))}
            </ol>
          )}
        </div>
      )}

      {packSummary && (
        <dl className="df2-gate8-proof-honesty" aria-label="Signed proof pack honesty">
          <div>
            <dt>migration_proven</dt>
            <dd className={packSummary.migration_proven ? "is-ok" : "is-warn"}>
              {packSummary.migration_proven ? "true" : "false"}
              {packSummary.claim_level ? ` · ${packSummary.claim_level}` : ""}
            </dd>
          </div>
          <div>
            <dt>Accepted risks</dt>
            <dd>{packSummary.accepted_risks ?? 0}</dd>
          </div>
          <div>
            <dt>Hashes</dt>
            <dd>
              ddl {shortChecksum(packSummary.ddl_hash || undefined)}
              {" · "}
              map {shortChecksum(packSummary.mapping_hash || undefined)}
            </dd>
          </div>
          <div>
            <dt>Connector versions</dt>
            <dd>{packSummary.versions_honesty || "absent"}</dd>
          </div>
          {packSummary.rollback && (
            <div>
              <dt>Rollback</dt>
              <dd>{packSummary.rollback}</dd>
            </div>
          )}
          {(packSummary.incomplete?.length ?? 0) > 0 && (
            <div>
              <dt>Incomplete</dt>
              <dd className="is-warn">{packSummary.incomplete!.join("; ")}</dd>
            </div>
          )}
        </dl>
      )}

      <div className="df2-gate8-proof-next" role="region" aria-label="Proof export and next steps">
        <div className="df2-gate8-proof-next-copy">
          <strong>{hasFindings || preWrite ? "Next step" : "Audit export"}</strong>
          <p>
            {preWrite
              ? "Run Execute to produce post-write row-count and checksum proof in Job Theater."
              : !passed
                ? "Fix mapping or transforms, inspect quarantine if rows were isolated, then re-run. Export the proof for audit."
                : sampleVerified
                  ? "Sample matched — export the signed pack; migration_proven stays false until full_checksum."
                  : appendDelta || keyedBatch
                    ? "Rows landed. For full_checksum cell proof, re-run with overwrite/truncate or upsert on a primary key."
                    : "Export the signed proof pack for diligence (accepted risks, policies, hashes)."}
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
            onClick={() => void exportGate8Proof(report, jobId)}
          >
            {jobId ? "Export signed proof pack" : "Export proof JSON"}
          </button>
          {jobId && !preWrite && (
            <button
              type="button"
              className="df2-btn df2-btn-sm"
              onClick={() => void exportMigrationCertificate(jobId)}
              title="Signed audit PDF: rows read/written/quarantined by reason, reconciliation verdict, destination physical state"
            >
              Download Migration Certificate (PDF)
            </button>
          )}
          <input
            ref={verifyInputRef}
            type="file"
            accept="application/json,.json"
            hidden
            onChange={(e) => {
              const file = e.target.files?.[0];
              e.target.value = "";
              if (!file) return;
              setVerifyState({ status: "working" });
              void verifyProofFile(file)
                .then((res) => {
                  if (res.ok) {
                    setVerifyState({ status: "ok", sha: res.content_sha256 });
                  } else {
                    setVerifyState({
                      status: "fail",
                      errors: res.errors?.length ? res.errors : ["Signature verification failed"],
                    });
                  }
                })
                .catch((err) => {
                  setVerifyState({
                    status: "fail",
                    errors: [err instanceof Error ? err.message : "Verify failed"],
                  });
                });
            }}
          />
          <button
            type="button"
            className="df2-btn df2-btn-sm"
            disabled={verifyState.status === "working"}
            onClick={() => verifyInputRef.current?.click()}
            title="Re-check an exported HMAC proof pack (buyer diligence)"
          >
            {verifyState.status === "working" ? "Verifying…" : "Verify proof pack"}
          </button>
        </div>
        {verifyState.status === "ok" && (
          <p className="df2-gate8-proof-verify is-ok" role="status">
            HMAC proof pack verified
            {verifyState.sha ? ` · content ${verifyState.sha.slice(0, 12)}…` : ""}.
          </p>
        )}
        {verifyState.status === "fail" && (
          <p className="df2-gate8-proof-verify is-fail" role="alert">
            Proof verify failed: {verifyState.errors.join("; ")}
          </p>
        )}
      </div>

      {explanation && (
        <details className="df2-gate8-proof-explain">
          <summary>How this transfer was planned</summary>
          <pre>{explanation}</pre>
        </details>
      )}
    </section>
  );
}
