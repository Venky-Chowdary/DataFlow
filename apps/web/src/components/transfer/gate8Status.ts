/**
 * The Gate-8 payload shape and the pure verdict helpers over it.
 *
 * These live apart from the card so the card module exports a component and
 * nothing else — a module mixing the two cannot Fast Refresh, and a stale
 * hot update of the card used to take Transfer Studio down.
 */
import type { Gate8ReconciliationPayload } from "../../lib/types";

export type Gate8Reconciliation = Gate8ReconciliationPayload;
export type Gate8Identity = NonNullable<Gate8ReconciliationPayload["identity"]>;
export type Gate8SampleMismatch = NonNullable<
  NonNullable<Gate8ReconciliationPayload["sample_compare"]>["mismatches"]
>[number];

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
