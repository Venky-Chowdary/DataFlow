/**
 * The Run step's result header: one job status, one verification verdict and
 * the row identity, all in the product's own words.
 *
 * The verdict is the Trust Report's (GET /jobs/{id}/parity-report, decided by
 * apps/api/services/parity_report.py). Nothing here decides whether a run
 * passed. When the report cannot be read, the only verdict stated locally is
 * the one the API itself returns for a job with no Gate-8 reconciliation
 * (unverifiable); anything else is "not loaded", never a guess.
 *
 * Colour rule: green only for proven / explained. A finished job is a fact,
 * not a pass, so "Completed" alone is neutral.
 */

import { isDestMeasured, readConservationLedger, removedOnRead, type LedgerCarrier } from "./conservationLedger";
import { unverifiableNotice, type ParityReport, type ParityVerdict } from "./parity";
import { jobStatusLabel } from "./uiUtils";

export type RunTone = "success" | "warning" | "danger" | "info" | "neutral";

/** The five verdicts, as docs/use/verification.md names them (sentence case). */
export const VERDICT_WORDS: Record<ParityVerdict, string> = {
  proven: "Proven",
  explained: "Explained",
  no_difference_found: "No difference found",
  unexplained: "Unexplained",
  unverifiable: "Unverifiable",
};

export type VerificationLoad =
  | { kind: "loading" }
  | {
    kind: "ready";
    report: Pick<ParityReport, "verdict" | "verdict_reason" | "parity" | "confidence"> & {
      evidence: Pick<ParityReport["evidence"], "reconciliation_passed">;
    };
  }
  | { kind: "error"; message: string };

export interface RunVerification {
  /** Null while the report loads or when it could not be read. */
  verdict: ParityVerdict | null;
  /** The word shown after "Verification:". */
  word: string;
  tone: RunTone;
  /** One sentence: why this verdict (the API's reason, or what is missing). */
  reason: string;
  /** True only for proven and explained: the one case a success colour is allowed. */
  supportsSuccess: boolean;
}

/** Proven / explained are the only green verdicts; a failed Gate-8 is red even when no row is named. */
export function verdictTone(verdict: ParityVerdict, reconciliationPassed?: boolean | null): RunTone {
  if (verdict === "proven" || verdict === "explained") return "success";
  if (verdict === "unexplained") return "danger";
  if (verdict === "unverifiable" && reconciliationPassed === false) return "danger";
  return "neutral";
}

const STOPPED = new Set(["failed", "cancelled", "canceled"]);

function hasReconciliation(job: { reconciliation?: unknown }): boolean {
  const recon = job.reconciliation;
  return Boolean(recon) && typeof recon === "object" && Object.keys(recon as object).length > 0;
}

/** What the API would say when Gate-8 recorded nothing: the one verdict safe to state without the report. */
function noReconciliationReason(status: string): string {
  const s = String(status || "").toLowerCase();
  if (STOPPED.has(s)) {
    return `This run ${s === "failed" ? "failed" : "was cancelled"} before verification ran, so the source and destination were never compared.`;
  }
  return "Gate-8 recorded no reconciliation for this job, so nothing was compared.";
}

export function presentRunVerification(
  load: VerificationLoad,
  job: { status?: string | null; reconciliation?: unknown },
): RunVerification {
  const status = String(job.status || "");
  if (load.kind === "ready") {
    const { report } = load;
    const verdict = report.verdict;
    const word = VERDICT_WORDS[verdict] ?? VERDICT_WORDS.unverifiable;
    const reason = (unverifiableNotice(report, status) ?? report.verdict_reason ?? "").trim();
    const tone = verdictTone(verdict, report.evidence?.reconciliation_passed);
    return { verdict, word, tone, reason, supportsSuccess: tone === "success" };
  }
  if (!hasReconciliation(job)) {
    // Same rule as parity_report._verdict: no reconciliation → unverifiable.
    return {
      verdict: "unverifiable",
      word: VERDICT_WORDS.unverifiable,
      tone: "neutral",
      reason: noReconciliationReason(status),
      supportsSuccess: false,
    };
  }
  if (load.kind === "loading") {
    return { verdict: null, word: "Checking…", tone: "neutral", reason: "Reading the Trust Report for this run.", supportsSuccess: false };
  }
  return {
    verdict: null,
    word: "Not loaded",
    tone: "neutral",
    reason: `The Trust Report could not be loaded${load.message ? ` (${load.message.replace(/\.$/, "")})` : ""}. Retry, or open it from Jobs.`,
    supportsSuccess: false,
  };
}

export interface RunStatusPresentation {
  /** The shared status label (lib/uiUtils jobStatusLabel). */
  label: string;
  /** Headline for the result header. */
  title: string;
  tone: RunTone;
}

export function presentRunStatus(status: string | null | undefined): RunStatusPresentation {
  const s = String(status || "");
  const label = jobStatusLabel(s);
  switch (s) {
    case "completed":
    case "success":
    case "succeeded":
      return { label, title: "Transfer completed", tone: "neutral" };
    case "completed_with_quarantine":
      return { label, title: "Transfer completed with quarantine", tone: "warning" };
    case "completed_with_warnings":
      return { label, title: "Transfer completed with warnings", tone: "warning" };
    case "failed":
      return { label, title: "Transfer failed", tone: "danger" };
    case "cancelled":
    case "canceled":
      return { label: jobStatusLabel("cancelled"), title: "Transfer cancelled", tone: "neutral" };
    default:
      return { label, title: label, tone: "neutral" };
  }
}

export interface ReadTerm {
  label: string;
  /** Formatted count, or a phrase when the count is unknown. */
  value: string;
  tone: RunTone;
}

export interface ReadStatement {
  /** read = landed + held + skipped (+ removed on read), as terms in order; the first is "Read". */
  terms: ReadTerm[];
  /** Plain-text form for screen readers and copy. */
  text: string;
  /** True when the destination was counted independently (not the writer's own report). */
  measured: boolean;
  /** True when the terms close as read = landed + held + skipped; false prints them as a list. */
  identity: boolean;
  /** Writer-reported rows when the destination was not counted. */
  writerReported: number | null;
}

const fmt = (n: number | null | undefined) =>
  n == null || !Number.isFinite(Number(n)) ? null : Number(n).toLocaleString("en-US");

/**
 * The row identity in plain words: read = landed + held + skipped. Engine
 * numbers only; when the destination was not counted, "landed" says so and
 * the writer's own count is named as such, never used as the landed figure.
 */
export function runReadStatement(
  job: LedgerCarrier & { rejected_rows?: number | null; total_rows?: number | null },
): ReadStatement | null {
  const ledger = readConservationLedger(job);
  if (!ledger) {
    const written = fmt(job.records_processed);
    if (written == null) return null;
    const held = Math.max(0, Number(job.rejected_rows ?? 0) || 0);
    const terms: ReadTerm[] = [
      { label: "Read", value: fmt(job.total_rows) ?? "not reported", tone: "neutral" },
      { label: "Landed", value: "not counted", tone: "neutral" },
      { label: "Held", value: held.toLocaleString("en-US"), tone: held > 0 ? "warning" : "neutral" },
    ];
    return {
      terms,
      text: `${termsText(terms, false)} · the writer reported ${written}`,
      measured: false,
      identity: false,
      writerReported: Number(job.records_processed),
    };
  }
  const measured = isDestMeasured(ledger);
  const kind = ledger.conservation_kind;
  // Keyed upserts and multi-stream roll-ups do not close as read = landed + held + skipped.
  const closes = measured && !NON_IDENTITY_KINDS.has(kind);
  const landedCount = kind === "mirror"
    ? ledger.active_count
    : kind === "append_delta" || kind === "keyed"
      ? (ledger.dest_delta ?? ledger.rows_written)
      : kind === "job_rollup"
        ? (ledger.active_count ?? ledger.dest_count)
        : ledger.dest_count;
  const writer = ledger.writer_ack ?? ledger.rows_written ?? (job.records_processed ?? null);
  const held = ledger.rows_quarantined;
  const removed = removedOnRead(ledger);
  const terms: ReadTerm[] = [
    { label: "Read", value: fmt(ledger.rows_read) ?? "not reported", tone: "neutral" },
    {
      label: "Landed",
      value: measured ? (fmt(landedCount) ?? "not counted") : "not counted",
      tone: "neutral",
    },
    { label: "Held", value: held.toLocaleString("en-US"), tone: held > 0 ? "warning" : "neutral" },
    { label: "Skipped", value: ledger.rows_skipped.toLocaleString("en-US"), tone: "neutral" },
  ];
  if (removed > 0) terms.push({ label: "Removed on read", value: removed.toLocaleString("en-US"), tone: "neutral" });
  const writerText = !measured && fmt(writer) ? ` · the writer reported ${fmt(writer)}` : "";
  return {
    terms,
    text: `${termsText(terms, closes)}${writerText}`,
    measured,
    identity: closes,
    writerReported: measured || writer == null ? null : Number(writer),
  };
}

const NON_IDENTITY_KINDS = new Set(["keyed", "job_rollup", "write_refused"]);

function termsText(terms: ReadTerm[], identity: boolean): string {
  if (!identity) return terms.map((t) => `${t.label} ${t.value}`).join(" · ");
  const [first, ...rest] = terms;
  return `${first.label} ${first.value} = ${rest.map((t) => `${t.label.toLowerCase()} ${t.value}`).join(" + ")}`;
}

/**
 * The header's accent: a failure (job or verification) is red, held rows are
 * amber, and green appears only when the verdict is proven or explained.
 */
export function runHeadlineTone(status: RunStatusPresentation, verification: RunVerification): RunTone {
  if (status.tone === "danger" || verification.tone === "danger") return "danger";
  if (status.tone === "warning") return "warning";
  if (verification.supportsSuccess) return "success";
  return "neutral";
}
