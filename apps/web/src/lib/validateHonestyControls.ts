/**
 * Module 16 — Validate Honesty Controls SSOT.
 *
 * Surfaces what Validate actually proved (sample vs population RI, ConversionClass)
 * so operators never confuse sample gates with migration_proven / RI proven.
 */
import type { PreflightResult } from "./types.js";

export interface ReferentialIntegrityHonesty {
  proven: boolean;
  coverage: string;
  sampleRan: boolean;
  populationRan: boolean;
  populationCount: number | null;
  note: string;
  /** Operator-facing one-liner — never claims population from sample. */
  headline: string;
}

export interface ConversionClassHonesty {
  counts: Record<string, number>;
  needsApproval: number;
  unsupported: number;
  lossless: number;
  columns: Array<{
    source?: string;
    target?: string;
    conversion_class?: string;
    invents_capacity?: boolean;
    requires_risk_contract?: boolean;
  }>;
  headline: string;
  note: string;
}

export interface ValidateHonestyControls {
  referentialIntegrity: ReferentialIntegrityHonesty;
  conversionClasses: ConversionClassHonesty;
  /** Opt-in population orphan scan — expensive; default false. */
  populationScanRequested: boolean;
  migrationProven: boolean;
  ddlIdentityHash: string | null;
  /** Module 17 — measured route success or explicitly unmeasured. */
  historicalSuccess: {
    measured: boolean;
    successRate: number | null;
    runsObserved: number;
    headline: string;
  };
  note: string;
}

export function buildReferentialIntegrityHonesty(
  preflight: PreflightResult | null | undefined,
): ReferentialIntegrityHonesty {
  const ri = preflight?.referential_integrity;
  const proven = Boolean(ri?.proven);
  const coverage = String(ri?.coverage || "none");
  const sampleRan = Boolean(ri?.sample_orphan_probe_ran);
  const populationRan = Boolean(ri?.population_orphan_probe_ran);
  const populationCount =
    typeof ri?.population_orphan_count === "number" ? ri.population_orphan_count : null;
  const note = String(
    ri?.note
      || "Referential integrity not proven — sample orphan probe never equals population RI.",
  );

  let headline: string;
  if (proven) {
    headline = "RI proven — population orphan scan complete with zero orphans";
  } else if (populationRan) {
    headline = "Population orphan scan ran — RI not proven (orphans or incomplete)";
  } else if (sampleRan) {
    headline = "Sample orphan probe only — population RI not proven";
  } else {
    headline = "No orphan probe — population RI not proven";
  }

  return {
    proven,
    coverage,
    sampleRan,
    populationRan,
    populationCount,
    note,
    headline,
  };
}

export function buildConversionClassHonesty(
  preflight: PreflightResult | null | undefined,
): ConversionClassHonesty {
  const raw = preflight?.proof_bundle?.conversion_contract;

  const columns = (raw?.columns ?? []).map((c) => ({
    source: c.source != null ? String(c.source) : undefined,
    target: c.target != null ? String(c.target) : undefined,
    conversion_class: c.conversion_class != null ? String(c.conversion_class) : undefined,
    invents_capacity: Boolean(c.invents_capacity),
    requires_risk_contract: Boolean(c.requires_risk_contract),
  }));

  const counts: Record<string, number> = {};
  for (const c of columns) {
    const key = c.conversion_class || "unclassified";
    counts[key] = (counts[key] || 0) + 1;
  }

  const needsApproval = counts["needs_user_approval"] || 0;
  const unsupported = counts["unsupported"] || 0;
  const lossless = counts["lossless"] || 0;

  let headline: string;
  if (columns.length === 0) {
    headline = "No conversion-class stamp on this Validate (re-run after Map)";
  } else if (needsApproval > 0) {
    headline = `${needsApproval} column(s) need Risk Contract / user approval`;
  } else if (unsupported > 0) {
    headline = `${unsupported} unsupported conversion(s) — remap required`;
  } else {
    headline = `${lossless} lossless · ${columns.length} column(s) classified`;
  }

  return {
    counts,
    needsApproval,
    unsupported,
    lossless,
    columns,
    headline,
    note: "ConversionClass is charter taxonomy — invent (p,s)/FSP/TZ never silent green.",
  };
}

export function buildValidateHonestyControls(
  preflight: PreflightResult | null | undefined,
  opts?: {
    populationScanRequested?: boolean;
  },
): ValidateHonestyControls {
  const ri = buildReferentialIntegrityHonesty(preflight);
  const conversionClasses = buildConversionClassHonesty(preflight);
  const migrationProven = Boolean(preflight?.proof_bundle?.migration_proven);
  const ddlIdentityHash =
    preflight?.proof_bundle?.ddl_identity?.ddl_identity_hash || null;
  const hsRaw =
    (preflight?.proof_bundle as Record<string, unknown> | undefined)?.historical_success
    ?? (preflight as Record<string, unknown> | null | undefined)?.historical_success;
  const hs = (hsRaw && typeof hsRaw === "object")
    ? (hsRaw as {
      measured?: boolean;
      success_rate?: number | null;
      runs_observed?: number;
    })
    : undefined;
  const hsMeasured = Boolean(hs?.measured);
  const hsRate = typeof hs?.success_rate === "number" ? hs.success_rate : null;
  const hsRuns = typeof hs?.runs_observed === "number" ? hs.runs_observed : 0;
  const historicalSuccess = {
    measured: hsMeasured,
    successRate: hsMeasured ? hsRate : null,
    runsObserved: hsRuns,
    headline: hsMeasured && hsRate != null
      ? `Historical success measured: ${(hsRate * 100).toFixed(1)}% over ${hsRuns} load(s)`
      : "Historical success unmeasured — no invented rate",
  };

  return {
    referentialIntegrity: ri,
    conversionClasses,
    populationScanRequested: Boolean(opts?.populationScanRequested),
    migrationProven,
    ddlIdentityHash,
    historicalSuccess,
    note: [
      "Sample Validate never claims population correctness.",
      "RI proven requires opt-in population orphan scan with zero orphans.",
      "Execute-ready is not migration_proven.",
      "Historical success is measured or unmeasured — never invented.",
    ].join(" "),
  };
}
