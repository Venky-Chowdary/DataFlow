/**
 * Historical success process metric — one owner for Validate.
 *
 * A rate is published only when the engine stamped measured=true from load
 * history. Unmeasured never invents 0% / 99% / a percent of any kind.
 * Transform qualityScore() is sample hygiene — not this metric.
 */

export const HISTORICAL_SUCCESS_METRIC_VERSION = "historical_success_metric.v1";

export type HistoricalSuccessEvidence = {
  measured?: boolean;
  success_rate?: number | null;
  runs_observed?: number;
  rows_written_total?: number;
  rows_rejected_total?: number;
  note?: string;
  never_invented?: boolean;
};

export type HistoricalSuccessMetric = {
  measured: boolean;
  successRate: number | null;
  runsObserved: number;
  rowsWritten: number;
  rowsRejected: number;
  rowsKept: number;
  /** Operator headline. Contains `%` only when measured. */
  headline: string;
  badge: string;
  keptLabel: string;
  rejectedLabel: string;
  neverInvented: true;
  hasPercent: boolean;
  version: typeof HISTORICAL_SUCCESS_METRIC_VERSION;
};

function asInt(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

export function buildHistoricalSuccessMetric(
  raw: HistoricalSuccessEvidence | null | undefined,
): HistoricalSuccessMetric {
  const measured = raw?.measured === true;
  const rate = measured && typeof raw?.success_rate === "number" ? raw.success_rate : null;
  const runs = asInt(raw?.runs_observed);
  const written = asInt(raw?.rows_written_total);
  const rejected = asInt(raw?.rows_rejected_total);
  const kept = written > 0 ? Math.max(0, written - rejected) : 0;

  if (!measured || rate == null) {
    return {
      measured: false,
      successRate: null,
      runsObserved: runs,
      rowsWritten: written,
      rowsRejected: rejected,
      rowsKept: kept,
      headline: "Historical success unmeasured — no invented rate",
      badge: "Unmeasured",
      keptLabel: "Kept unmeasured",
      rejectedLabel: "Rejected unmeasured",
      neverInvented: true,
      hasPercent: false,
      version: HISTORICAL_SUCCESS_METRIC_VERSION,
    };
  }

  const pct = `${(rate * 100).toFixed(1)}%`;
  return {
    measured: true,
    successRate: rate,
    runsObserved: runs,
    rowsWritten: written,
    rowsRejected: rejected,
    rowsKept: kept,
    headline: `Historical success measured: ${pct} over ${runs} load(s)`,
    badge: `${pct} measured`,
    keptLabel: `${kept.toLocaleString()} kept`,
    rejectedLabel: `${rejected.toLocaleString()} rejected`,
    neverInvented: true,
    hasPercent: true,
    version: HISTORICAL_SUCCESS_METRIC_VERSION,
  };
}

/** Chrome contract: unmeasured copy must never contain a percent sign. */
export function historicalSuccessHeadlineHasPercent(headline: string): boolean {
  return headline.includes("%");
}
