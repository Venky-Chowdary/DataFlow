/**
 * Measured evidence for public-facing pages.
 *
 * Every row here comes from a live matrix run recorded in
 * `docs/CLIENT_READINESS_REPORT.md` — a real transfer through the product path
 * against a real engine, with the destination re-read afterwards. Marketing
 * copy renders these rows instead of writing its own numbers, so a claim on the
 * site cannot outrun the artifact that backs it.
 *
 * Rules for editing:
 * - A row may only be added once its artifact exists and the run is recorded.
 * - `cases` is the number of scenarios actually executed, never a target.
 * - Anything not measured belongs in `NOT_PROVEN`, phrased as the reason.
 */

export interface EvidenceRow {
  /** What was proven, in operator language. */
  claim: string;
  /** Engines the matrix actually ran against. */
  engines: string;
  /** Scenarios executed. */
  cases: number;
  /** Outcome exactly as the artifact records it. */
  result: string;
  /** Artifact filename in the readiness report. */
  artifact: string;
}

export interface UnprovenRow {
  area: string;
  /** `planned`, `blocked` (needs something we do not have) or `unaudited`. */
  status: "planned" | "blocked" | "unaudited";
  /** Why it is not proven — never a promise. */
  reason: string;
}

/** Date the numbers below were last measured. */
export const EVIDENCE_AS_OF = "August 2026";

export const PROVEN_EVIDENCE: EvidenceRow[] = [
  {
    claim: "Schema drift — widen, narrow-refuse, NOT NULL, defaults, concurrency, resume, case variants",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 48,
    result: "48 recorded, 0 violations",
    artifact: "drift_live_results.json, drift_live_multi_results.json",
  },
  {
    claim: "Identity / sequence generator carried on create-new, re-read from the destination catalog and proven by a post-cutover insert without a key",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 16,
    result: "14 carried, 2 declared unsupported (never silently dropped)",
    artifact: "identity_live_results.json",
  },
  {
    claim: "Foreign keys carried with dependency-ordered multi-table create, each proven by the destination rejecting an orphan row",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 11,
    result: "11 ok",
    artifact: "foreign_key_live_results.json, fk_single_table_live_results.json",
  },
  {
    claim: "CHECK constraint carry, proven by the destination rejecting a violating row",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 16,
    result: "16 ok",
    artifact: "check_carry_live_results.json",
  },
  {
    claim: "Secondary index carry",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 16,
    result: "16 ok",
    artifact: "secondary_index_live_results.json",
  },
  {
    claim: "Physical placement — partitioning, tablespace, filegroup — carried or explicitly refused, never invented",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 10,
    result: "10 ok",
    artifact: "physical_placement_live_results.json",
  },
  {
    claim: "Validate→Execute DDL identity: append, incremental delta, duplicate re-run refusal, narrowed-DDL refusal",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle, SQLite",
    cases: 5,
    result: "5 engines, all ok — refusals write zero rows",
    artifact: "ddl_identity_matrix_results.json",
  },
  {
    claim: "30-source-column into a 20-column destination: unmapped columns block pre-write unless declared",
    engines: "PostgreSQL, MySQL, Oracle",
    cases: 36,
    result: "12 scenarios × 3 engines",
    artifact: "migration_scenario_matrix_results.json",
  },
  {
    claim: "Retry safety: retry-from-start is refused when the failed attempt committed rows under a non-convergent mode, and for a cancelled run",
    engines: "PostgreSQL (live scheduler)",
    cases: 14,
    result: "14/14 recorded",
    artifact: "schedule_live_results.json",
  },
  {
    claim: "Destination privilege probe before write",
    engines: "PostgreSQL, MySQL, SQL Server, Oracle",
    cases: 7,
    result: "7 ok",
    artifact: "grants_live_results.json",
  },
  {
    claim: "Incremental transforms load by column name, refusing unmatched columns",
    engines: "PostgreSQL, MySQL, SQL Server",
    cases: 33,
    result: "33 ok",
    artifact: "transform_live_results.json",
  },
];

/** Backend suite as last measured — see the readiness report for the shard log. */
export const BACKEND_SUITE = {
  passed: 13244,
  failed: 0,
  skipped: 1515,
} as const;

/**
 * Drivers with `PRODUCTION_SKU` evidence. Catalog tiles are a larger number and
 * are never presented as live capability.
 */
export const TRANSFER_READY_DRIVERS = 44;

export const NOT_PROVEN: UnprovenRow[] = [
  {
    area: "Snowflake, BigQuery, S3 / ADLS / GCS",
    status: "blocked",
    reason: "No credentials provisioned to the build machine, so no live matrix has been run.",
  },
  {
    area: "Salesforce, Stripe, Shopify, HubSpot",
    status: "planned",
    reason: "Implemented and unit-tested; no integration user exists yet for a live certification run.",
  },
  {
    area: "Exactly-once change delivery",
    status: "planned",
    reason: "CDC is at-least-once idempotent upsert. Exactly-once is not claimed for any route.",
  },
  {
    area: "Scheduled backfill / catch-up",
    status: "planned",
    reason: "Missed windows are counted and surfaced. Historical windows are never silently replayed.",
  },
  {
    area: "Multi-replica scheduler failover on MongoDB",
    status: "planned",
    reason: "Same-schedule and connector-pair overlap are proven; conditional claims under real replica failover are not.",
  },
  {
    area: "Transform row ledger and quarantine",
    status: "planned",
    reason:
      "Transform loads are proven column-correct on three engines; a per-row read/written/quarantined account inside a transform does not exist yet.",
  },
  {
    area: "Contracts and Proofs surfaces",
    status: "unaudited",
    reason: "Implemented and unit-tested, not yet examined at the live-matrix bar.",
  },
  {
    area: "SOC 2 / ISO 27001 certification",
    status: "planned",
    reason: "Controls are implemented; no third-party audit has been completed, so no certificate exists.",
  },
];
