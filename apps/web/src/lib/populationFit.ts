/**
 * Operator wording for the population fit gate (`g3f_population_fit`).
 *
 * The one rule this file exists to keep: a clean *sample* is never phrased as
 * population proof. Only `evidence: "exact"` may say "every row".
 */

import type { PreflightResult } from "./types";

export type PopulationFit = NonNullable<PreflightResult["population_fit"]>;

export interface PopulationFitSummary {
  /** Short line for the Coverage honesty list. */
  headline: string;
  /** True only when every source row was scanned with no unfit value. */
  proven: boolean;
  /** Columns with values the destination carrier cannot hold. */
  offenders: Array<{
    column: string;
    targetType: string;
    rows: number;
    exampleRows: number[];
    exampleValues: string[];
    abortsJob: boolean;
  }>;
}

function fmt(n: number): string {
  return n.toLocaleString();
}

export function populationFitSummary(
  fit: PopulationFit | null | undefined,
): PopulationFitSummary | null {
  if (!fit || typeof fit !== "object") return null;

  const offenders = (fit.findings ?? []).map((f) => ({
    column: String(f.source ?? f.target ?? ""),
    targetType: String(f.target_type ?? ""),
    rows: Number(f.unfit_rows ?? 0),
    exampleRows: (f.example_rows ?? []).map((r) => Number(r)).filter((r) => r > 0),
    exampleValues: (f.example_values ?? []).map((v) => String(v)),
    abortsJob: Boolean(f.aborts_job),
  }));

  const scanned = Number(fit.rows_scanned ?? 0);
  const total = Number(fit.rows_total ?? 0);
  const bounded = (fit.bounded_columns ?? []).length;
  const evidence = fit.evidence ?? "unmeasured";

  if (offenders.length > 0) {
    const rows = offenders.reduce((sum, o) => sum + o.rows, 0);
    const where = offenders
      .map((o) => `${o.column} → ${o.targetType}`)
      .join(", ");
    const scope =
      evidence === "exact" ? `all ${fmt(scanned)} row(s)` : `${fmt(scanned)} scanned row(s)`;
    return {
      headline: `${fmt(rows)} value(s) in ${scope} cannot fit ${where}`,
      proven: false,
      offenders,
    };
  }

  if (bounded === 0) {
    return {
      headline:
        "No mapped column can exceed its destination carrier by declaration — no value scan needed",
      proven: true,
      offenders,
    };
  }

  if (evidence === "exact") {
    return {
      headline: `Every value in ${fmt(scanned)} source row(s) fits ${bounded} bounded column(s)`,
      proven: true,
      offenders,
    };
  }
  if (evidence === "partial") {
    return {
      headline: `Clean for ${fmt(scanned)} of ${fmt(total || scanned)} row(s) — the scan stopped at its budget, so the rest is unproven`,
      proven: false,
      offenders,
    };
  }
  if (evidence === "sampled") {
    return {
      headline: `Clean on ${fmt(scanned)} preview row(s) — population fit unproven for ${bounded} bounded column(s)`,
      proven: false,
      offenders,
    };
  }
  return {
    headline: `Population fit unmeasured — ${bounded} bounded column(s) stay unproven until the write-time checks run`,
    proven: false,
    offenders,
  };
}
