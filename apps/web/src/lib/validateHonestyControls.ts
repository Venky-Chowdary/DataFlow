/**
 * Module 16 — Validate Honesty Controls SSOT.
 *
 * Surfaces what Validate actually proved (sample vs population RI, ConversionClass)
 * so operators never confuse sample gates with migration_proven / RI proven.
 */
import { buildHistoricalSuccessMetric, type HistoricalSuccessMetric } from "./historicalSuccessMetric.js";
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

export interface DecisionArtifactHonesty {
  present: boolean;
  contentHash: string | null;
  schemaVersion: string | null;
  headline: string;
  note: string;
}

export interface ValidateHonestyControls {
  referentialIntegrity: ReferentialIntegrityHonesty;
  conversionClasses: ConversionClassHonesty;
  /** Phase C11/C12 — Decision Artifact authority from Validate. */
  decisionArtifact: DecisionArtifactHonesty;
  /** Opt-in population orphan scan — expensive; default false. */
  populationScanRequested: boolean;
  migrationProven: boolean;
  ddlIdentityHash: string | null;
  /** Module 17 — measured route success or explicitly unmeasured. */
  historicalSuccess: HistoricalSuccessMetric;
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
  const lossless =
    (counts["lossless"] || 0)
    + (counts["identity"] || 0)
    + (counts["equivalent"] || 0)
    + (counts["widening"] || 0)
    + (counts["representation"] || 0)
    + (counts["normalization"] || 0);

  let headline: string;
  if (columns.length === 0) {
    headline = "No conversion-class stamp on this Validate (re-run after Map)";
  } else if (needsApproval > 0) {
    headline = `${needsApproval} column(s) need Risk Contract / user approval`;
  } else if (unsupported > 0) {
    headline = `${unsupported} unsupported conversion(s) — remap required`;
  } else {
    headline = `${lossless} safe-path · ${columns.length} column(s) classified`;
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
  const art = preflight?.proof_bundle?.decision_artifact;
  const artHash =
    preflight?.proof_bundle?.decision_artifact_hash
    || art?.content_hash
    || null;
  const artPresent = Boolean(artHash && String(artHash).length === 64);
  const decisionArtifact: DecisionArtifactHonesty = {
    present: artPresent,
    contentHash: artHash ? String(artHash) : null,
    schemaVersion: art?.schema_version ? String(art.schema_version) : null,
    headline: artPresent
      ? `Decision Artifact stamped (${String(artHash).slice(0, 12)}…)`
      : "Decision Artifact missing — re-run Validate before Execute",
    note:
      "Execute consumes this immutable artifact hash — UI never re-derives invent/risk.",
  };
  const hsRaw =
    (preflight?.proof_bundle as Record<string, unknown> | undefined)?.historical_success
    ?? (preflight as Record<string, unknown> | null | undefined)?.historical_success;
  const historicalSuccess = buildHistoricalSuccessMetric(
    hsRaw && typeof hsRaw === "object"
      ? (hsRaw as {
        measured?: boolean;
        success_rate?: number | null;
        runs_observed?: number;
        rows_written_total?: number;
        rows_rejected_total?: number;
      })
      : undefined,
  );

  return {
    referentialIntegrity: ri,
    conversionClasses,
    decisionArtifact,
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

export type SchemaEvolutionStamp = {
  action?: string;
  should_pause?: boolean;
  compatibility?: string;
  compatibility_note?: string;
  hard_breaking?: unknown[];
  soft_net_additive?: unknown[];
};

/**
 * Acknowledge-this-run is only valid for manual_review additive/soft drift.
 * Hard-breaking (Confluent NONE) must remap or re-sign — Airbyte "approve myself"
 * silently continued on column drop; we refuse that here.
 */
export function schemaDriftAllowsAcknowledge(
  details?: Record<string, unknown> | null,
): boolean {
  if (!details) return false;
  if (details.remediation_kind !== "acknowledge_schema_drift") return false;
  if (details.ack_required === false) return false;
  const evolution = schemaEvolutionFromDetails(details);
  if (evolution.should_pause) return false;
  if ((evolution.hard_breaking?.length ?? 0) > 0) return false;
  if (evolution.compatibility === "none") return false;
  return true;
}

export function schemaEvolutionFromDetails(
  details?: Record<string, unknown> | null,
): SchemaEvolutionStamp {
  const raw = details?.schema_evolution;
  if (!raw || typeof raw !== "object") return {};
  const evo = raw as SchemaEvolutionStamp;
  return {
    action: typeof evo.action === "string" ? evo.action : undefined,
    should_pause: Boolean(evo.should_pause),
    compatibility: typeof evo.compatibility === "string" ? evo.compatibility : undefined,
    compatibility_note:
      typeof evo.compatibility_note === "string" ? evo.compatibility_note : undefined,
    hard_breaking: Array.isArray(evo.hard_breaking) ? evo.hard_breaking : undefined,
    soft_net_additive: Array.isArray(evo.soft_net_additive) ? evo.soft_net_additive : undefined,
  };
}

export function schemaDriftCompatibilityHeadline(
  details?: Record<string, unknown> | null,
): string {
  const evo = schemaEvolutionFromDetails(details);
  const compat = evo.compatibility;
  if (!compat) return "";
  if (evo.compatibility_note) return `Compatibility ${compat} — ${evo.compatibility_note}`;
  return `Compatibility ${compat}`;
}

export interface NumberLocaleValidateAction {
  decision: "set_locale";
  columns: string[];
  message: string;
}

/** Validate next action when Auto cannot parse 1,234 vs 1.234. */
export function numberLocaleValidateAction(
  preflight: PreflightResult | null | undefined,
): NumberLocaleValidateAction | null {
  const report = preflight?.number_locale_report;
  if (!report || typeof report !== "object") return null;
  if (String(report.decision || "") !== "set_locale") return null;
  const columns = (report.ambiguous_columns || [])
    .map((row) => String(row?.column || "").trim())
    .filter(Boolean);
  const named = columns.slice(0, 6).join(", ") || "amount";
  return {
    decision: "set_locale",
    columns,
    message:
      `${named}: grouping is ambiguous (1,234 vs 1.234). ` +
      "Set number locale US or EU in Destination → Advanced — Auto will not guess.",
  };
}

export type DateLocaleValidateAction = NumberLocaleValidateAction;

/** Validate next action when Auto cannot tell Jan 2 from Feb 1. */
export function dateLocaleValidateAction(
  preflight: PreflightResult | null | undefined,
): DateLocaleValidateAction | null {
  const report = preflight?.date_locale_report;
  if (!report || typeof report !== "object") return null;
  if (String(report.decision || "") !== "set_locale") return null;
  const columns = (report.ambiguous_columns || [])
    .map((row) => String(row?.column || "").trim())
    .filter(Boolean);
  const named = columns.slice(0, 6).join(", ") || "event_date";
  return {
    decision: "set_locale",
    columns,
    message:
      `${named}: day/month order is ambiguous (01/02/2024 is Jan 2 or Feb 1). ` +
      "Set date locale DMY or MDY in Destination → Advanced — Auto will not guess.",
  };
}

/** Hard-breaking drift (Confluent NONE) — remap / re-sign, never acknowledge. */
export function schemaDriftRequiresRemap(
  details?: Record<string, unknown> | null,
): boolean {
  if (!details) return false;
  const evo = schemaEvolutionFromDetails(details);
  if (evo.should_pause) return true;
  if ((evo.hard_breaking?.length ?? 0) > 0) return true;
  return evo.compatibility === "none";
}
