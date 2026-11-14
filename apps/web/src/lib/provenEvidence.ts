/**
 * Public connector claims, generated from CI evidence.
 *
 * Every number and badge here derives from `CONNECTOR_CLAIMS`
 * (`connectorClaims.generated.ts`), which
 * `apps/api/scripts/connector_claims_report.py` writes from
 * `apps/api/data/connector_ci_evidence.json`: the per-engine result of the CI
 * jobs that prove an engine (`services/connector_ci_evidence.py`). An engine
 * counts as proven only when its proving jobs ran green with none of its tests
 * skipped. A test fails when the generated file drifts from the evidence.
 *
 * The hand-written evidence rows this file used to carry named artifacts that
 * no longer ship and could not be re-run (CLM-5); they are gone. Nothing here
 * is typed in by hand.
 */
import { CONNECTOR_CLAIMS } from "./connectorClaims.generated";

export { CONNECTOR_CLAIMS };

export interface EvidenceRow {
  /** What was proven, in operator language. */
  claim: string;
  /** Engine the CI jobs proved. */
  engines: string;
  /** Tests the proving jobs executed (passed + failed; zero failed and skipped). */
  cases: number;
  /** Outcome as the evidence records it. */
  result: string;
  /** CI jobs that proved it. */
  artifact: string;
}

export interface UnprovenRow {
  area: string;
  /** `planned`, `blocked` (needs something we do not have) or `unaudited`. */
  status: "planned" | "blocked" | "unaudited";
  /** Why it is not proven — never a promise. */
  reason: string;
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

function monthYear(iso: string): string {
  const match = /^(\d{4})-(\d{2})/.exec(iso || "");
  if (!match) return "";
  return `${MONTHS[Number(match[2]) - 1]} ${match[1]}`;
}

/** Month the CI evidence was recorded (empty when no run is recorded). */
export const EVIDENCE_AS_OF = monthYear(CONNECTOR_CLAIMS.evidence_generated_at);

/** The CI run the evidence came from (its GitHub Actions run id). */
export const EVIDENCE_RUN_ID: string = CONNECTOR_CLAIMS.evidence_run_id;

/** One row per engine a CI run proved: green, nothing skipped. */
export const PROVEN_EVIDENCE: EvidenceRow[] = CONNECTOR_CLAIMS.proof_rows.map((row) => ({
  claim: row.emulated
    ? "Proving CI jobs ran green against the engine's emulator, with no skipped test"
    : "Proving CI jobs ran green against the live engine, with no skipped test",
  engines: row.name,
  cases: row.executed,
  result: "green",
  artifact: row.jobs.join(", "),
}));

/** Transfer drivers a green CI run proves. */
export const PROVEN_DRIVERS: number = CONNECTOR_CLAIMS.proven_count;

/** Transfer drivers that are implemented and still running their CI proof. */
export const PENDING_DRIVERS: number = CONNECTOR_CLAIMS.pending_count;

/** CDC sources whose engine a green CI run proves. */
export const PROVEN_CDC_SOURCES: string[] = CONNECTOR_CLAIMS.cdc_sources
  .filter((s) => s.proven)
  .map((s) => s.name);

/** "N drivers proven in CI" — the one public driver count. */
export function provenDriversPhrase(count: number = PROVEN_DRIVERS): string {
  return `${count} ${count === 1 ? "driver" : "drivers"} proven in CI`;
}

/**
 * Public catalog lead: proven vs implemented, both derived from evidence
 * (CLM-6). Catalog tile count is never presented as a driver count.
 */
export function catalogHonestyLead(
  provenCount: number = PROVEN_DRIVERS,
  pendingCount: number = PENDING_DRIVERS,
): string {
  // Not "each verifies the load": file exports and several destinations have no
  // independent read-back (CLM-6). The claim is the CI run, and only that.
  const proven = `${provenDriversPhrase(provenCount)}: each one's CI jobs ran green with no skipped test.`;
  return pendingCount > 0
    ? `${proven} ${pendingCount} more are implemented and running their CI proof.`
    : proven;
}

/** Public family badge, derived from which members CI proves. */
export type CatalogFamilyBadge = "Proven in CI" | "Partly proven in CI" | "Implemented";

/**
 * Internal operator ledger — not rendered on marketing pages.
 * Reasons stay factual; they never promise a date.
 */
export const NOT_PROVEN: UnprovenRow[] = [
  {
    area: "Snowflake, BigQuery, S3 / ADLS / GCS",
    status: "unaudited",
    reason:
      "Emulator leftover MERGE is measured on test_warehouse_emulator_leftover_merge_* (moto/MinIO/fake-gcs/Azurite dest 4→3, incremental no-op, dest-engine GET COUNT; fakesnow + BigQuery emulator dest-engine DELETE). Customer-tenant Snowflake/BQ/S3 PRODUCTION_SKU is not claimed.",
  },
  {
    area: "Salesforce, Stripe, Shopify, HubSpot",
    status: "planned",
    reason:
      "Salesforce and HubSpot are certified reverse-ETL. Stripe is a named incremental created-cursor SKU (sqlite dest COUNT, not a Stripe tenant). Shopify stays Planned until PRODUCTION_SKU. Connecting a catalog tile is not a live transfer.",
  },
  {
    area: "Exactly-once change delivery",
    status: "planned",
    reason:
      "CDC default is at-least-once upsert. One named dest-owned route (MySQL CDC → Postgres) has a crash-replay artifact when Postgres is reachable; PLATFORM_EXACTLY_ONCE_CLAIMED stays False.",
  },
  {
    area: "Scheduled backfill / catch-up",
    status: "unaudited",
    reason: "Operator-approved bounded replay is measured: POST /schedules/{id}/catch-up queues up to 24 runs — one per missed window, oldest first, each tagged catch_up (test_schedule_catch_up.py). Historical windows are still never silently replayed. Warehouse-scale backfill certification is not claimed.",
  },
  {
    area: "Multi-replica scheduler failover on MongoDB",
    status: "unaudited",
    reason: "Same-schedule and connector-pair overlap are proven; the exact-claim CAS stale-reclaim race is measured on live MongoDB (test_scheduler_replica_failover.py — live-claim block, exact-match reclaim, two-beats-one-winner, fresh-claim-not-stolen, counter survival). A real Railway replica crash mid-transfer is not a test fixture.",
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
      "Postgres WAL, MySQL ROW binlog (test_cdc_mysql_binlog_transfer_e2e), SQL Server native CDC (test_cdc_sqlserver_native_transfer_e2e), and Oracle LogMiner (test_oracle_logminer_transfer_snapshot_resume_delete; dest {1:99, 3:30}, id=2 gone) are measured snapshot+resume+delete. MySQL→PostgreSQL dest-owned DELETE + replay is measured on test_mysql_binlog_postgres_dest_owned_delete_replay (dest {1:99, 3:30}, replay 0, stale LSN skipped). CDC default remains at-least-once upsert. Not dest-owned exactly-once.",
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

const FAMILY_NOTES: Record<string, string> = {
  Databases:
    "Identity, foreign keys, CHECK constraints, and indexes carry on create-new, and CDC reads the database log where the engine offers one.",
  "Warehouses & lakehouse":
    "Create-new tables, MERGE upserts, and post-write verification. Snowflake connects with password or key-pair auth; Iceberg connects to a REST catalog or a filesystem warehouse.",
  "Object storage":
    "S3-compatible endpoints, s3:// URLs and prefixes, with decimals landing exact and every row accounted for.",
  Files: "Typed reads and exports for the common file formats, through the same map and gates.",
  "Cache & search": "Key-value and document stores, written through the same map, gates, and read-back.",
  Streaming:
    "Read topics as a source with one exact type per field across the run; write topics append-only.",
  "Vector databases":
    "Embeddings from the provider you choose for the destination, with source columns kept typed beside them.",
  "Applications & APIs":
    "Reverse-ETL into Salesforce and HubSpot, incremental Stripe reads on the created cursor, and REST sources with the same map, gates, and verification.",
};

function familyBadge(proven: number, total: number): CatalogFamilyBadge {
  if (total > 0 && proven === total) return "Proven in CI";
  if (proven > 0) return "Partly proven in CI";
  return "Implemented";
}

/**
 * Public catalog families. Members, the proven subset, and the badge all come
 * from `CONNECTOR_CLAIMS`; only the family note is prose.
 */
export const MARKETING_STACK: ReadonlyArray<{
  family: string;
  items: string;
  proven: string;
  badge: CatalogFamilyBadge;
  note: string;
}> = CONNECTOR_CLAIMS.families.map((f) => {
  const provenNames = f.items.filter((i) => i.proven).map((i) => i.name);
  return {
    family: f.family,
    items: f.items.map((i) => i.name).join(", "),
    proven: provenNames.join(", "),
    badge: familyBadge(provenNames.length, f.items.length),
    note: FAMILY_NOTES[f.family] ?? "",
  };
});

/** What every load does — product behaviour, no unrecorded figures (CLM-5). */
export const MARKETING_PROOF_HIGHLIGHTS = [
  {
    title: "Schema changes stay under control",
    body: "Widen safely, refuse unsafe narrows, and carry nullability and defaults — Validate decides before anything is written.",
    stat: "Validate gate",
  },
  {
    title: "Identity and keys survive cutover",
    body: "Sequences, foreign keys, and CHECK constraints carry to the destination on create-new and are read back from its catalog.",
    stat: "Create-new",
  },
  {
    title: "Retries keep a load correct",
    body: "Before any retry, Datawrap checks what the failed attempt already committed and restarts only on a path that converges.",
    stat: "Retry check",
  },
  {
    title: "Transforms load by column name",
    body: "Incremental transforms match every column by name and refuse columns that do not match.",
    stat: "By name",
  },
] as const;
