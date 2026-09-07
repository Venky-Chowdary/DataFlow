/**
 * Gate-8 population view for Theater — dest COUNT + checksum + Validate run_id.
 *
 * Dest count is dest-engine read-back only. Writer ack is never a population.
 * `population_proof` is never invented here; the engine stamps that field.
 */

import type { ConservationLedger } from "./conservationLedger";
import type { Gate8ReconciliationPayload, LineageEvent, PreflightResult } from "./types";

export type Gate8PopulationInput = {
  row_accounting?: ConservationLedger | Record<string, unknown> | null;
  reconciliation?: Gate8ReconciliationPayload | null;
  preflight?: Pick<PreflightResult, "run_id"> | null;
};

export type Gate8PopulationView = {
  destCount: number | null;
  destCountBefore: number | null;
  destChecksum: string;
  coverage: string;
  sourceChecksumProvenance: string;
  validateRunId: string;
  source: string;
};

function finiteInt(value: unknown): number | null {
  if (value == null || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.trunc(n);
}

function text(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

/**
 * Prefer dest-engine COUNT on the conservation ledger, then Gate-8
 * `target_rows`, then `dest_readback.dest_count`. Never close dest with
 * records_processed / writer ack.
 */
export function readGate8Population(input: Gate8PopulationInput): Gate8PopulationView {
  const rec = input.reconciliation;
  const rb = rec?.dest_readback;
  const ledger = input.row_accounting && typeof input.row_accounting === "object"
    ? input.row_accounting as Record<string, unknown>
    : null;
  const destCount =
    finiteInt(ledger?.dest_count)
    ?? finiteInt(rec?.target_rows)
    ?? finiteInt(rb?.dest_count);
  const destCountBefore =
    finiteInt(ledger?.dest_count_before)
    ?? finiteInt(rec?.target_rows_before)
    ?? finiteInt(rb?.dest_count_before);
  const destChecksum = text(rb?.dest_checksum) || text(rec?.target_checksum);
  const coverage = text(rb?.coverage) || text(rec?.coverage);
  return {
    destCount,
    destCountBefore,
    destChecksum,
    coverage,
    sourceChecksumProvenance: text(rec?.source_checksum_provenance),
    validateRunId: text(input.preflight?.run_id),
    source: text(rb?.source) || (destCount != null ? "gate8_dest_readback" : ""),
  };
}

export function formatProofScope(view: Gate8PopulationView): string {
  const coverage = view.coverage || "unmeasured";
  const provenance = view.sourceChecksumProvenance || "—";
  return `${coverage} · ${provenance}`;
}

export type ControlTotalRowView = {
  column: string;
  sourceSum: string;
  destSum: string;
  proven: boolean;
  matched: boolean;
  reason: string;
};

export type ControlTotalsView = {
  declared: boolean;
  /** exact | sampled | unmeasured — anything but `exact` is not proof. */
  evidence: string;
  proven: boolean;
  mismatch: boolean;
  rows: ControlTotalRowView[];
};

/**
 * The G21 ledger as Theater shows it: declared money columns with both
 * independent SUMs and whether they matched.
 *
 * Sums stay strings all the way to the DOM — `Number("618.75")` is a float,
 * and a float is exactly the evidence G21 refuses. A column with no sums is
 * rendered as unproven with its reason, never as a zero balance.
 */
export function readControlTotals(
  reconciliation: Gate8ReconciliationPayload | null | undefined,
): ControlTotalsView {
  const ct = reconciliation?.control_totals;
  const columns = Array.isArray(ct?.columns) ? ct.columns : [];
  const rows: ControlTotalRowView[] = columns.map((c) => ({
    column: text(c?.source) || text(c?.target),
    sourceSum: text(c?.source_sum),
    destSum: text(c?.dest_sum),
    proven: c?.proven === true,
    matched: c?.matched === true,
    reason: text(c?.reason),
  }));
  const declared = ct?.declared === true && rows.length > 0;
  return {
    declared,
    evidence: text(ct?.evidence) || "unmeasured",
    // Proven only when the engine said `exact` and every column proved.
    proven: declared && text(ct?.evidence) === "exact" && rows.every((r) => r.proven),
    mismatch: ct?.any_mismatch === true,
    rows,
  };
}

/**
 * What the engine's `evidence` token means to an operator.
 *
 * `unmeasured` is the engine's word for "no exact population proof", and it
 * is also what a *mismatch* carries — two cent-exact sums that disagree were
 * certainly measured, so echoing the raw token next to them reads as a
 * contradiction. The raw token stays available for the auditor.
 */
export function controlTotalEvidenceLabel(view: ControlTotalsView): string {
  return CONTROL_TOTAL_EVIDENCE[evidenceKey(view)].label;
}

/**
 * The engine's own token, said in a way that cannot contradict the panel:
 * a mismatch carries `unmeasured`, so the tooltip has to explain that the
 * token is about *proof*, not about whether anything was summed.
 */
export function controlTotalEvidenceTitle(view: ControlTotalsView): string {
  const { evidence } = view;
  return `engine evidence token: ${evidence || "unmeasured"} — ${CONTROL_TOTAL_EVIDENCE[evidenceKey(view)].why}`;
}

type EvidenceKey = "exact" | "sampled" | "mismatch" | "none";

const CONTROL_TOTAL_EVIDENCE: Record<EvidenceKey, { label: string; why: string }> = {
  exact: {
    label: "population SUM, exact",
    why: "every row was summed on both sides with exact decimal arithmetic",
  },
  sampled: {
    label: "sample SUM — not proof",
    why: "only a sample was summed, so the population total is unproven",
  },
  mismatch: {
    label: "measured, sums disagree",
    why: "both sums were measured and they disagree, so nothing is proven",
  },
  none: {
    label: "no exact population SUM",
    why: "no exact population SUM was recorded for this run",
  },
};

function evidenceKey(view: ControlTotalsView): EvidenceKey {
  if (view.evidence === "exact") return "exact";
  if (view.evidence === "sampled") return "sampled";
  return view.mismatch ? "mismatch" : "none";
}

export type LineageEventView = {
  eventType: string;
  timestamp: string;
  summary: string;
};

export function readJobLineage(events: LineageEvent[] | null | undefined): LineageEventView[] {
  if (!Array.isArray(events)) return [];
  return events
    .filter((e): e is LineageEvent => Boolean(e) && typeof e === "object")
    .map((event) => {
      const payload = event.payload && typeof event.payload === "object" ? event.payload : {};
      const eventType = text(event.event_type) || "event";
      const parts: string[] = [];
      if (payload.source_count != null) parts.push(`src ${payload.source_count}`);
      if (payload.target_count != null) parts.push(`dest ${payload.target_count}`);
      if (payload.quarantine_count != null) parts.push(`q ${payload.quarantine_count}`);
      if (payload.checksum_ok === true) parts.push("checksum ok");
      if (payload.checksum_ok === false) parts.push("checksum mismatch");
      if (payload.cdc_lag_seconds != null) parts.push(`lag ${payload.cdc_lag_seconds}s`);
      if (payload.cdc_lag_basis) parts.push(String(payload.cdc_lag_basis));
      return {
        eventType,
        timestamp: text(event.timestamp),
        summary: parts.join(" · ") || eventType.replace(/_/g, " "),
      };
    });
}
