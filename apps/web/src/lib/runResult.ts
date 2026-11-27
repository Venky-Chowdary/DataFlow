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

import {
  isDestMeasured,
  readConservationLedger,
  removedOnRead,
  type ConservationLedger,
  type LedgerCarrier,
} from "./conservationLedger";
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
  /** The operator printed before this term ("=", "+", "−" or "·"); absent on the first term. */
  op?: "=" | "+" | "−" | "·";
}

/** Whether the ledger's identity closes, as the API's ledger itself reports it. */
export type ReadBalance = "balanced" | "unbalanced" | "open" | "unmeasured";

export interface ReadStatement {
  /** The terms in order, each carrying the operator before it; the first is "Read" (or the keyed "Dest change"). */
  terms: ReadTerm[];
  /** Plain-text form for screen readers and copy. */
  text: string;
  /** True when the destination was counted independently (not the writer's own report). */
  measured: boolean;
  /**
   * True only when an equation is printed: the ledger says it balances AND the
   * numbers shown add up. Never true for numbers that do not satisfy it.
   */
  identity: boolean;
  /** The ledger's own verdict: balanced, unbalanced (unaccounted rows), open (not proven) or unmeasured. */
  balance: ReadBalance;
  /** The ledger's ``unaccounted`` (read side minus accounted side), when it reports one. */
  unaccounted: number | null;
  /** One sentence about the balance when it is not a printed equation, else null. */
  balanceText: string | null;
  /** Writer-reported rows when the destination was not counted. */
  writerReported: number | null;
}

const fmt = (n: number | null | undefined) =>
  n == null || !Number.isFinite(Number(n)) ? null : Number(n).toLocaleString("en-US");

const count = (n: number) => n.toLocaleString("en-US");

const isNum = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

type Side = { label: string; value: number | null; tone?: RunTone }[];

/**
 * The identity each conservation kind closes on, as apps/api/services/row_conservation.py
 * states it. ``lhs`` and ``rhs`` add up when the ledger balances; keyed is
 * dest change = new keys − deletes.
 */
interface KindIdentity {
  lhs: Side;
  rhs: Side;
  /** Signs of the rhs terms after the first (keyed subtracts deletes). */
  rhsOps?: ("+" | "−")[];
  /** Terms shown after the equation as context, never part of it. */
  context?: Side;
}

function heldTone(n: number | null): RunTone {
  return (n ?? 0) > 0 ? "warning" : "neutral";
}

function identityFor(ledger: ConservationLedger, job: LedgerCarrier): KindIdentity {
  const kind = ledger.conservation_kind;
  const held = ledger.rows_quarantined;
  const skipped = ledger.rows_skipped;
  const removed = removedOnRead(ledger);
  const expanded = Math.max(Number((job.row_accounting as Record<string, unknown> | null | undefined)?.rows_expanded ?? 0) || 0, 0);
  const tail: Side = [
    { label: "Held", value: held, tone: heldTone(held) },
    { label: "Skipped", value: skipped },
    ...(removed > 0 ? [{ label: "Removed on read", value: removed }] : []),
  ];
  if (kind === "keyed") {
    return {
      lhs: [{ label: "Dest change", value: ledger.dest_delta }],
      rhs: [{ label: "New keys", value: ledger.inserts }, { label: "Deletes", value: ledger.deletes }],
      rhsOps: ["−"],
      context: [
        { label: "Read", value: ledger.rows_read },
        ...(ledger.updates != null ? [{ label: "Updated", value: ledger.updates }] : []),
        { label: "Held", value: held, tone: heldTone(held) },
        { label: "Skipped", value: skipped },
      ],
    };
  }
  if (kind === "mirror" || kind === "scd2") {
    // read + retained = active (current) + held + skipped + removed. The ledger
    // reports unaccounted = read + retained − accounted, so retained follows from it exactly.
    const landed = kind === "mirror" ? ledger.active_count : (ledger.rows_written ?? ledger.current_count ?? ledger.dest_count);
    const accounted = isNum(landed) ? landed + held + skipped + removed : null;
    const retained = isNum(ledger.unaccounted) && isNum(ledger.rows_read) && accounted != null
      ? ledger.unaccounted - ledger.rows_read + accounted
      : null;
    return {
      lhs: [
        { label: "Read", value: ledger.rows_read },
        ...(retained != null && retained > 0 ? [{ label: "Retained", value: retained }] : []),
      ],
      rhs: [{ label: kind === "mirror" ? "Active" : "Current", value: landed }, ...tail],
    };
  }
  const landed = kind === "append_delta"
    ? (ledger.dest_delta ?? ledger.rows_written)
    : kind === "vector"
      ? (ledger.identity_count ?? ledger.dest_count)
      : kind === "job_rollup"
        ? (ledger.rows_written ?? ledger.active_count ?? ledger.dest_count)
        : ledger.dest_count;
  return {
    lhs: [
      { label: "Read", value: ledger.rows_read },
      ...(expanded > 0 ? [{ label: "Added by recipe", value: expanded }] : []),
    ],
    rhs: [{ label: "Landed", value: landed }, ...tail],
  };
}

/** True when every value is known and the two sides add up with their signs. */
function adds(identity: KindIdentity): boolean {
  const all = [...identity.lhs, ...identity.rhs];
  if (!all.every((t) => isNum(t.value))) return false;
  const left = identity.lhs.reduce((acc, t) => acc + (t.value as number), 0);
  const right = identity.rhs.reduce((acc, t, i) => {
    const sign = i > 0 && identity.rhsOps?.[i - 1] === "−" ? -1 : 1;
    return acc + sign * (t.value as number);
  }, 0);
  return left === right;
}

function streamBalanceText(ledger: ConservationLedger): string | null {
  const per = ledger.per_stream;
  if (!per || per.length === 0) return null;
  const open = per.filter((s) => !s.measured || !s.balanced).length;
  if (open === 0) return `Every one of the ${count(per.length)} streams balances on its own ledger; their counts are not summed here.`;
  return `Does not balance: ${count(open)} of ${count(per.length)} streams do not close.`;
}

function balanceSentence(ledger: ConservationLedger, printed: boolean): { balance: ReadBalance; text: string | null } {
  if (ledger.balanced) {
    if (printed) return { balance: "balanced", text: null };
    if (ledger.conservation_kind === "job_rollup") {
      return { balance: "balanced", text: streamBalanceText(ledger) ?? "The ledger balances." };
    }
    return { balance: "balanced", text: "The ledger balances on terms this summary does not show; open Row accounting for them." };
  }
  const n = ledger.unaccounted;
  if (isNum(n) && n !== 0) {
    return {
      balance: "unbalanced",
      text: n > 0
        ? `Does not balance: ${count(n)} unaccounted`
        : `Does not balance: ${count(Math.abs(n))} more in the destination than were read`,
    };
  }
  if (ledger.conservation_kind === "job_rollup") {
    return { balance: "unbalanced", text: streamBalanceText(ledger) ?? "Does not balance: a stream ledger is open." };
  }
  return { balance: "open", text: "Not proven: the row ledger could not close this identity." };
}

function asTerms(identity: KindIdentity, equation: boolean): ReadTerm[] {
  const sep = (op: ReadTerm["op"]) => (equation ? op : "·");
  const show = (t: Side[number]): Omit<ReadTerm, "op"> => ({
    label: t.label,
    value: fmt(t.value) ?? "not counted",
    tone: t.tone ?? "neutral",
  });
  const out: ReadTerm[] = [];
  identity.lhs.forEach((t, i) => out.push({ ...show(t), ...(i > 0 ? { op: sep("+") } : {}) }));
  identity.rhs.forEach((t, i) => out.push({
    ...show(t),
    op: i === 0 ? sep("=") : sep(identity.rhsOps?.[i - 1] ?? "+"),
  }));
  for (const t of identity.context ?? []) out.push({ ...show(t), op: "·" });
  return out;
}

function termsText(terms: ReadTerm[]): string {
  return terms
    .map((t, i) => {
      const body = `${i === 0 || t.op === "·" ? t.label : t.label.toLowerCase()} ${t.value}`;
      if (i === 0) return body;
      return t.op === "·" ? ` · ${body}` : ` ${t.op} ${body}`;
    })
    .join("");
}

/**
 * The row identity the engine ledger reports for this run's conservation kind:
 * read (+ retained / added by recipe) = landed + held + skipped (+ removed on
 * read), or the keyed dest change = new keys − deletes. An equation is printed
 * only when the ledger says it balances and the numbers shown add up; an
 * unbalanced ledger says so with its unaccounted rows. When the destination was
 * not counted, "landed" says so and the writer's own count is named as such.
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
      { label: "Landed", value: "not counted", tone: "neutral", op: "·" },
      { label: "Held", value: count(held), tone: heldTone(held), op: "·" },
    ];
    return {
      terms,
      text: `${termsText(terms)} · the writer reported ${written}`,
      measured: false,
      identity: false,
      balance: "unmeasured",
      unaccounted: null,
      balanceText: null,
      writerReported: Number(job.records_processed),
    };
  }
  const measured = isDestMeasured(ledger);
  const identity = identityFor(ledger, job);
  if (!measured) {
    // Never a landed figure from the writer: the destination count is named as missing.
    for (const t of identity.rhs) if (t.label === "Landed" || t.label === "Active" || t.label === "Current" || t.label === "New keys") t.value = null;
  }
  const printed = measured && ledger.balanced && adds(identity);
  const terms = asTerms(identity, printed);
  const writer = ledger.writer_ack ?? ledger.rows_written ?? (job.records_processed ?? null);
  const writerText = !measured && fmt(writer) ? ` · the writer reported ${fmt(writer)}` : "";
  // A multi-stream job whose streams do not all close is not "measured" as a
  // whole, but its per-stream ledgers still say how many streams are open.
  const perStreamJob = ledger.conservation_kind === "job_rollup" && Boolean(ledger.per_stream?.length);
  const { balance, text: balanceText } = measured || perStreamJob
    ? balanceSentence(ledger, printed)
    : { balance: "unmeasured" as ReadBalance, text: null };
  return {
    terms,
    text: `${termsText(terms)}${writerText}${balanceText ? ` · ${balanceText}` : ""}`,
    measured,
    identity: printed,
    balance,
    unaccounted: isNum(ledger.unaccounted) ? ledger.unaccounted : null,
    balanceText,
    writerReported: measured || writer == null ? null : Number(writer),
  };
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
