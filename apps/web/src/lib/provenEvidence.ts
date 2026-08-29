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
 * Unique duplex/source drivers from `transfer_live_driver_types()` after the
 * honesty filter (preflight required except file_source; email stays demoted
 * as write-only with no read-back, SFTP earned its place in
 * test_sftp_live_transfer.py against a real server).
 * This is the count **when optional packages are present**. A given host may
 * report fewer via `GET /capabilities` `unique_transfer_drivers`. Catalog tiles
 * are a larger number and are never presented as live capability.
 * Regenerate by: `python -c "from src.transfer.connector_capabilities import transfer_live_driver_types; print(len(transfer_live_driver_types()))"`
 */
export const TRANSFER_READY_DRIVERS = 43;

/**
 * Catalog slots the desktop lab can bind and exercise as source + dest.
 * This is an operator option, not 80 unique engines and not catalog tile count.
 */
export const DESKTOP_LAB_CATALOG_SLOTS = 80;

/**
 * Operator-facing honesty line. Catalog tile count is never a live-driver count.
 * Hosts without optional warehouse/SaaS packages report fewer via capabilities.
 */
export function catalogHonestyLead(readyCount: number = TRANSFER_READY_DRIVERS): string {
  return (
    `Catalog tiles are not transfer-live. ${readyCount} drivers are TRANSFER_READY ` +
    `when optional packages are present. A given host may report fewer via GET /capabilities.`
  );
}

/** Studio-aligned family badge. Mixed = at least one name is Planned or package-gated. */
export type CatalogFamilyBadge = "Certified" | "Mixed" | "Planned";

/**
 * Internal operator ledger — not rendered on marketing pages.
 * Reasons stay factual; they never promise a date.
 */
export const NOT_PROVEN: UnprovenRow[] = [
  {
    area: "Snowflake, BigQuery, S3 / ADLS / GCS",
    status: "planned",
    reason:
      "Warehouse and object-store connectors ship in the catalog. Shared-sandbox live-matrix certification is completed on the customer tenant during onboarding.",
  },
  {
    area: "Salesforce, Stripe, Shopify, HubSpot",
    status: "planned",
    reason:
      "Application connectors are implemented and unit-tested. Live certification uses the customer integration user during a guided rollout.",
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
    status: "unaudited",
    reason:
      "Named sqlite fixture test_transform_quarantine_dlq_write_path persists failing not_null findings to the same DLQ and stamps row_accounting. Live warehouse transform-run quarantine is not yet matrix-measured.",
  },
  {
    area: "Contracts and Proofs surfaces",
    status: "unaudited",
    reason:
      "Named sqlite fixture test_contract_enforce_sqlite_fixture blocks missing required columns on a SIGNED file→sqlite contract. Shared-sandbox live-matrix certification is not claimed.",
  },
  {
    area: "CDC snapshot+LSN live-matrix",
    status: "planned",
    reason:
      "Postgres WAL, MySQL ROW binlog (test_cdc_mysql_binlog_transfer_e2e), SQL Server native CDC (test_cdc_sqlserver_native_transfer_e2e), and Oracle LogMiner (test_oracle_logminer_transfer_snapshot_resume_delete; dest {1:99, 3:30}, id=2 gone) are measured snapshot+resume+delete on named tests. Unparsed SQL_REDO is quarantined (test_oracle_logminer_unparsed_quarantine). CDC default remains at-least-once upsert.",
  },
  {
    area: "Iceberg leftover MERGE",
    status: "unaudited",
    reason:
      "SqlCatalog leftover MERGE is measured on test_iceberg_sql_catalog_leftover_merge_deletes_extra_and_count_is_snapshot_len (dest 4→3). REST warehouse leftover MERGE is measured on test_iceberg_rest_catalog_leftover_merge_deletes_extra_and_count_is_snapshot_len (dest 4→3, incremental no-op, file-footer COUNT) plus composite PK. Glue and Nessie stay Planned. Hadoop catalog is fail-closed in pyiceberg 0.11 (no SqlCatalog fallback).",
  },
  {
    area: "SOC 2 / ISO 27001 certification",
    status: "planned",
    reason: "Controls are implemented; no third-party audit has been completed, so no certificate exists.",
  },
];

/**
 * Public catalog families — product language, never CI blockers.
 * Names may include Planned tiles. `note` must say so; `badge` matches Studio.
 */
export const MARKETING_STACK: ReadonlyArray<{
  family: string;
  items: string;
  badge: CatalogFamilyBadge;
  note: string;
}> = [
  {
    family: "Warehouses",
    items: "Snowflake, BigQuery, Redshift, Databricks",
    badge: "Mixed",
    note:
      "Snowflake and BigQuery are TRANSFER_READY when their packages are installed. Redshift and Databricks stay Planned until a named PRODUCTION_SKU matrix. Every warehouse load still maps, gates, quarantines, and checksums.",
  },
  {
    family: "Object storage",
    items: "Amazon S3, Azure Data Lake Storage, Google Cloud Storage",
    badge: "Mixed",
    note:
      "S3, ADLS, and GCS are TRANSFER_READY when their packages are installed. Catalog tiles without a named matrix stay Planned. Writes still quarantine and account rows.",
  },
  {
    family: "Databases",
    items: "PostgreSQL, MySQL, SQL Server, Oracle, MongoDB",
    badge: "Mixed",
    note:
      "PostgreSQL, MySQL, and MongoDB are certified full transfer. SQL Server and Oracle are TRANSFER_READY when their packages are installed. Schema carry, identity, and checksum reconcile stay on every cutover.",
  },
  {
    family: "Applications",
    items: "Salesforce, Stripe, Shopify, HubSpot",
    badge: "Mixed",
    note:
      "Salesforce and HubSpot are certified reverse-ETL. Stripe and Shopify stay Planned until PRODUCTION_SKU. Connecting a catalog tile is not a live transfer.",
  },
];

export const MARKETING_PROOF_HIGHLIGHTS = [
  {
    title: "Schema changes stay under control",
    body: "Widen safely, refuse unsafe narrows, and carry nullability and defaults — measured on PostgreSQL, MySQL, SQL Server, and Oracle.",
    stat: "48 live cases",
  },
  {
    title: "Identity and keys survive cutover",
    body: "Sequences, foreign keys, and CHECK constraints are carried or explicitly refused — never silently dropped.",
    stat: "43 live cases",
  },
  {
    title: "Retries cannot corrupt a load",
    body: "A failed attempt that already committed rows cannot be blindly restarted under a non-convergent mode.",
    stat: "14 scheduler cases",
  },
  {
    title: "Transforms load by column name",
    body: "Unmatched columns are refused. Incremental transforms stay column-correct on PostgreSQL, MySQL, and SQL Server.",
    stat: "33 live cases",
  },
] as const;
