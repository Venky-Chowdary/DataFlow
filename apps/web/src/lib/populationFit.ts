/**
 * Operator wording for the population fit gate (`g3f_population_fit`).
 *
 * The one rule this file exists to keep: a clean *sample* is never phrased as
 * population proof. Only `evidence: "exact"` may say "every row".
 */

import type { PreflightResult, ValidationSuggestedAction } from "./types";

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
    suggestedTargetType: string;
    suggestedFix: string;
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
    suggestedTargetType: String(f.suggested_target_type ?? "").trim(),
    suggestedFix: String(f.suggested_fix ?? "").trim(),
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

function widenActionsOf(raw: unknown): ValidationSuggestedAction[] {
  if (!Array.isArray(raw)) return [];
  return raw.filter((a): a is ValidationSuggestedAction => (
    Boolean(a)
    && typeof a === "object"
    && (a as ValidationSuggestedAction).kind === "change_target_type"
    && Boolean((a as ValidationSuggestedAction).to_type)
    && (a as ValidationSuggestedAction).requires_ddl !== true
    && (a as ValidationSuggestedAction).mapping_applyable !== false
    && (a as ValidationSuggestedAction).apply_proven !== false
  ));
}

/** Proven create-new NUMBER/DECIMAL/VARCHAR widens — Apply updates CREATE, not live DDL. */
export function createNewFitWidenActions(
  preflight: PreflightResult | null | undefined,
): ValidationSuggestedAction[] {
  if (!preflight) return [];
  const gates = preflight.gates ?? [];
  const g3f = gates.find((g) => String(g.id) === "g3f_population_fit");
  const details = (g3f?.details ?? {}) as Record<string, unknown>;
  const fromGate = widenActionsOf(details.suggested_actions);
  const fromBlocker = (preflight.blockers ?? []).flatMap((b) => {
    if (String(b.id) !== "g3f_population_fit") return [];
    const top = widenActionsOf(b.suggested_actions);
    if (top.length) return top;
    const nested = widenActionsOf(
      (b.details as { suggested_actions?: unknown } | undefined)?.suggested_actions,
    );
    if (nested.length) return nested;
    return widenActionsOf(b.guidance?.suggested_actions);
  });
  const actions = fromGate.length ? fromGate : fromBlocker;
  const createNew = details.create_new_table === true
    || actions.length > 0 && actions.every((a) => a.requires_ddl !== true);
  if (!createNew || !actions.length) return [];
  const seen = new Set<string>();
  return actions.filter((a) => {
    const key = `${a.column || ""}|${a.target || ""}|${a.to_type || ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function createNewFitWidenLabel(actions: ValidationSuggestedAction[]): string {
  if (!actions.length) return "";
  if (actions.length === 1) {
    return actions[0].label || `Widen ${actions[0].column} to ${actions[0].to_type}`;
  }
  return `Widen CREATE types to fit this source (${actions.length} columns)`;
}
