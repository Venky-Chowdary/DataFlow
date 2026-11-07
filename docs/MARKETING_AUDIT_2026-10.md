# Marketing audit — October 2026

Internal working document. It records every public marketing claim on the
Datawrap site, checks each one against the code on
`origin/feature/fix-qa-bug-fixes` (production plus PR #359), and states the
action taken on branch `claude/marketing-refresh`. It is not linked from any
page.

- Base commit: `9c75ada4` (QA rounds 7–10, PR #359)
- Copy policy for this pass (owner, 2026-10-31): marketing pages carry
  positive, specific claims only. No disclaimers, "not yet", "beta",
  "limitations" or self-deprecating comparisons on a page. Anything that is
  not real is simply left out; the list of what was left out lives only in
  this document ("Not claimed on purpose", below).
- Status key: **current** (true and well put) · **stale** (was true, the
  product has moved on) · **wrong** (overclaim or factually off) ·
  **negative** (true, but a disclaimer or knock the new policy removes) ·
  **missing** (a shipped capability the site did not mention).
- Evidence paths are relative to the repo root. `api/` means `apps/api/`,
  `web/` means `apps/web/src/`.

## 1. Marketing surfaces and routes

All public routes are hash routes resolved by `web/lib/publicNavigation.ts`
(`BASE_HASH_TO_ROUTE`) and rendered by `web/pages/marketing/MarketingSite.tsx`.

| Route | Surface | Source |
|---|---|---|
| `#/` | Home | `web/pages/LandingPage.tsx`, `web/components/landing/{TrustSection,ProofEvidenceSection,ObservabilityInAction,AlgorithmCinema}.tsx` |
| `#/pricing` | Pricing | `web/pages/marketing/MarketingSubpage.tsx` `PricingPage` |
| `#/enterprise` | Enterprise | `MarketingSubpage.tsx` `EnterprisePage` |
| `#/customers` | Customers / proof | `MarketingSubpage.tsx` `CustomersPage` |
| `#/security` | Security | `MarketingSubpage.tsx` `SecurityPage` |
| `#/integrations` (`#/connectors`, `#/catalog`) | Connectors | `MarketingSubpage.tsx` `IntegrationsPage` |
| `#/contact` | Contact sales | `web/pages/marketing/ContactSalesPage.tsx` |
| `#/product/transfer`, `/jobs`, `/pipelines`, `/query`, `/pilot`, `/mcp` | Product pages | `web/pages/marketing/ProductSurfaces.tsx`, `productPageShared.tsx` |
| `#/solutions/migrations`, `/warehouse`, `/sync` | Solution pages | `ProductSurfaces.tsx` |
| `#/help` (`#/docs`) | Docs landing | `web/pages/marketing/DocsPortal.tsx` |
| `#/help/<slug>` | Help articles | `web/lib/helpDocs.ts` (operator documentation, reviewed for overclaims only) |
| `#/privacy`, `#/terms` | Legal | `MarketingSubpage.tsx` `LegalPage` (legal text, not rewritten) |
| all | Nav + footer | `web/components/marketing/MarketingChrome.tsx` |
| all | Shared evidence data | `web/lib/provenEvidence.ts` |
| all | Titles / meta | `web/lib/publicNavigation.ts` `PUBLIC_PAGE_META`, `web/lib/seo.ts` `PAGE_META`/`DEFAULT_*` |
| `/` (static) | Meta, OG, Twitter, JSON-LD | `apps/web/index.html` |
| static | Sitemap / robots | `apps/web/public/sitemap.xml`, `apps/web/public/robots.txt` |
| in-app, noindex | Docs / Benchmarks | `web/pages/DocsPage.tsx`, `web/pages/BenchmarksPage.tsx` (signed-in only, not marketing; checked for brand leaks) |

There are no `components/marketing/*` copy files beyond the chrome and hero
helpers, and no separate marketing copy JSON. Brand check: no user-facing
"DataFlow" string exists in any of the surfaces above (`grep -rn DataFlow
apps/web/src` returns one code comment, `components/transfer/ValidateDashboard.tsx:1636`).

## 2. Ground truth used

| Fact | Evidence |
|---|---|
| 46 transfer-ready drivers (re-measured on this base) | `api/src/transfer/connector_capabilities.py` `transfer_live_driver_types()` → 46: adls, avro, bigquery, couchbase, csv, docx, dynamodb, elasticsearch, excel, fixed_width, gcs, generic_sql, html, hubspot, iceberg, influxdb, json, jsonl, kafka, milvus, mongodb, mysql, ndjson, neo4j, oracle, orc, parquet, pdf, pgvector, pinecone, postgresql, qdrant, redis, rest_api, s3, salesforce, sftp, singer_tap, snowflake, sqlite, sqlserver, stripe, tsv, weaviate, xml, yaml |
| Redshift, Databricks, ClickHouse, MariaDB, CockroachDB, Shopify are catalog `planned` | `docs/CONNECTOR_READINESS_MATRIX.md:333,363,377,378` (status column) |
| Preflight gates G1–G9 (plus G13–G15 coverage/requirements gates) | `packages/preflight/src/preflight/models.py:15-28` |
| Gate-8 post-write read-back with value and key census | `api/src/transfer/reconcile_step.py:3604` (`_apply_n5_gate8_extensions`), `:4216` (`_census_join`), `:4366` (`_generic_key_census`); `docs/PROOF_POST_WRITE_CONTRACT.md` |
| Signed proof packs, hash-chained | `api/services/signed_proof_pack.py:1-6` |
| Tamper-evident audit chain with its own key and per-record key id (D221) | `api/services/audit_chain_keys.py:1-12,63` (`key_id_for`); `api/src/routers/audit_router.py:427` (verify) |
| Audit filters (level, since/until, action) and export (CSV/JSON/NDJSON/CEF) | `api/src/routers/audit_router.py:178-190,222-230` |
| Quarantine DLQ with replay closure | `api/services/quarantine_dlq.py:1-17` |
| Signed migration risk contracts; CAST_AND_CONTINUE governs database-refused cells (D214) | `api/services/migration_risk_contract.py:1-8,32,43`; `docs/MIGRATION_RISK_CONTRACT.md` |
| Schema drift with manual review read from Map | `api/services/schema_drift_map_review.py:1-10`, `api/services/drift_approval.py` |
| Mirror soft delete by default (`_deleted`, `_deleted_at`), hard delete explicit and warned | `api/services/mirror_engine.py:1-8`; `api/services/mirror_delete_notice.py` |
| CDC: SQL Server native, PostgreSQL logical (WAL), MySQL binlog, Oracle LogMiner | `api/connectors/sqlserver_cdc_native.py`, `api/connectors/postgresql_cdc_transport.py`, `api/connectors/oracle_logminer.py`, `api/services/cdc_engine.py`; tests named in `web/lib/provenEvidence.ts` `NOT_PROVEN` CDC row (test_cdc_mysql_binlog_transfer_e2e, test_cdc_sqlserver_native_transfer_e2e, test_oracle_logminer_transfer_snapshot_resume_delete) |
| Schedules, incremental watermarks typed to the cursor column, zone-aware cursors | PR #359 "fix(incremental): bind the watermark as a value of the cursor column's type (D207)", "zone-bearing cursors compare, order and store by instant"; `api/services/schedule_runner.py` |
| Unicode-capable text lands on NVARCHAR on SQL Server (D212) | `api/services/type_system.py:4447,4635` |
| Collation-aware key checks (PAD SPACE / `_ci` merges refused by name; created keys carry source equality) | `api/services/created_key_collation.py:1-8`, `api/services/collation_carry.py:1-8` |
| Timestamp precision: microseconds into SQLite (D224), DATETIME2(7) seventh digit | `api/services/carrier_instant.py:137,239` |
| Encoding detection with a visible, recorded guess (cp1252 sniff, cp1251 refused until declared) | `api/services/source_encoding_guess.py:1-8,39,61` |
| Kafka source, one exact numeric type per field | `api/connectors/kafka_reader.py`, `api/src/transfer/engine_kafka.py`, PR #359 D203 |
| pgvector typed tables by default; Milvus, Pinecone, Qdrant, Weaviate drivers | `api/services/pgvector_typed_embedding.py:1-8`; driver list above |
| Datawrap Pilot runs on its own local engine, no cloud key needed | `api/src/ai/copilot/pilot_agent.py:1413-1417`, `api/src/ai/copilot/tools.py:3060-3066` |
| MCP tools under workspace RBAC, deny-by-default allow-list | `docs/MCP.md:100-116`, `api/src/routers/mcp_router.py` |
| SSO: OIDC and SAML, email-domain routing | `api/src/routers/auth_router.py:36,78-86`, `api/services/sso_routing.py:1-7`, `api/src/routers/workspace_router.py:99` |
| SCIM 2.0 Users/Groups, group → workspace role | `api/services/scim_service.py:648,752`, `docs/SCIM.md` |
| RBAC; API keys capped at the creator's own grants | `api/src/routers/workspace_router.py:490-503` (`enforce_grant_bounds`), `api/src/routers/iam_router.py:128` |
| Workspace isolation at the store layer | `api/src/middleware/tenant_scope_middleware.py:1-12` |
| Signed webhooks (HMAC-SHA256, per-channel secret, timestamped) | `api/services/webhook_signing.py:1-8`, `docs/WEBHOOK_SIGNING.md` |
| Session idle-timeout policy | `api/services/session_policy.py:1-8` |
| Platform-admin-only deployment settings | `api/src/routers/workspace_router.py:160,175,680,884` |
| Job retention purge (dry run + confirm token) | `api/services/job_retention.py:1-8` |
| BYOK: local, wrapped and AWS KMS keys | `api/services/byok_key_manager.py:14-17,201-205` |
| Data region per workspace; destination in another region refused | `api/src/routers/transfer_router.py:129-148` |
| Per-workspace PII hashing key | PR #359 "hash_pii uses a per-workspace key ... (QA enterprise #19)" |
| MFA enforcement is design only | `docs/MFA_ENFORCEMENT_DESIGN.md`; `api/src/ai/first_party/capability_contract.py:479-481` |
| IP allowlist applies to tenant custom domains only | `api/src/ai/first_party/capability_contract.py:484`, `:923-934` |
| No SOC 2 / ISO / HIPAA certificate | `web/lib/provenEvidence.ts` `NOT_PROVEN` last row; `api/src/ai/first_party/capability_contract.py:715-727` |
| Snowflake | Repo artifacts are emulator-based (`docs/QA_DEFECT_TABLE.md:14`, `emulator-verified only` rows). Owner attests (2026-10-31, relayed by the coordinator) that Snowflake is now proven on a real account as source and destination. Pages present Snowflake as supported on that attestation. **Action for owner: commit the real-account run artifact** so the claim has a file behind it. |

## 3. Per-page audit

### 3.1 Home (`#/`) — `web/pages/LandingPage.tsx`

| Claim | Status | Evidence | Action |
|---|---|---|---|
| Stack strip and marquees show Redshift, ClickHouse | wrong | `CONNECTOR_READINESS_MATRIX.md:363,377` (planned) | Replaced with transfer-ready drivers (SQL Server, Oracle, Kafka, pgvector, Iceberg, ADLS, GCS, HubSpot, Stripe, Qdrant…) — `LandingPage.tsx:23-97` |
| Hero "Move any schema anywhere — proven." + sub | current | Gate-8 read-back, `reconcile_step.py:3604` | Kept headline; sub now names value-by-value verification |
| Hero meta "0 silent drops" | wrong (absolute) | — | Replaced by "verified value by value" |
| "Sync status is not proof" / "Most ELT tools celebrate…" | negative | — | Reframed as "Built for the moments that matter" problem list in positive voice |
| Architecture: Map / Preflight / Prove | current | `models.py:15` | Prove step now says Gate-8 read-back, value and key census |
| "Versus the market" (Fivetran, Airbyte, Informatica, Databricks cards) | negative + unverifiable | competitor behaviour is not in the repo | Replaced with "What sets Datawrap apart" — four Datawrap-only strengths, no named competitors |
| "CDC default is at-least-once upsert until a route proves…" | negative | — | Removed; CDC now stated positively (four native engines, snapshot + resume) |
| "What the engine actually does" — 6 items | stale / missing | — | Expanded to 12: adds verification, quarantine replay + signed contracts, drift review, CDC, Unicode/collation, timestamp precision, encoding, vectors |
| "We do not invent 99% accuracy or 700+ live connectors" | negative | — | Removed |
| Measured tile "13,244 backend tests passed · 0 failed" | stale | `provenEvidence.ts` `BACKEND_SUITE` (Aug 2026); AGENTS.md lists known failures today | Tile replaced with "4 native CDC engines"; constant kept for the internal test only |
| Measured tile "48 live schema-drift cases" | current | `provenEvidence.ts` `PROVEN_EVIDENCE[0]` | Kept |
| "A 96% headline is not how Map works" | negative | — | Removed |
| Use case "Snowflake, BigQuery, Redshift" | wrong (Redshift) | matrix:377 | Now Snowflake, BigQuery, Iceberg, S3/ADLS/GCS |
| Connectors "Catalog tiles that are not certified stay Planned" / "logos include Planned tiles" | negative | — | Removed; marquee now shows only transfer-ready drivers |
| Final CTA "SSO, BYOK, and audit" | current | `byok_key_manager.py`, `auth_router.py` | Kept, adds SCIM |
| Trust panel "SSO · Okta" | wrong (implies a named Okta integration) | generic OIDC/SAML | "SSO · OIDC / SAML" (`TrustSection.tsx`) |
| Trust "Purpose keys stay scoped to the job" | wrong | no purpose-key code; BYOK is per tenant | "Per-workspace keys" |
| Trust "Pin jobs and artifacts to the regions" | wrong | only destination-region check exists, `transfer_router.py:129` | "Workspace data region; destinations in other regions are refused" |
| Trust "Quarantine never silently drops rows" | negative | — | Positive verification line |
| Proof section "measured on live engines as of Aug 2026" | current | `provenEvidence.ts` | Kept |
| Missing: Pilot local engine, MCP, enterprise identity | missing | §2 | Added to engine list and trust copy |

### 3.2 Pricing (`#/pricing`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "Plans that scale with proof — not MAR surprises" + Fivetran/Informatica billing lead | negative / unverifiable | — | Positive headline about verification in every plan |
| Starter features (Studio, G1–G9, checksum, 46 drivers) | current | §2 | Kept; adds Gate-8 verification, quarantine replay |
| Team (Pipelines, Pilot, shared connectors) | current | `schedule_runner.py`, `pilot_agent.py` | Adds CDC + watermarks, Pilot local engine |
| Enterprise "SSO / SAML + BYOK", "MCP", "Dedicated success" | current | §2 | Adds SCIM, audit chain, signed webhooks |
| Compare table | current | — | Adds rows for CDC, SCIM, tamper-evident audit |
| "Versus the market" Fivetran/Informatica table | negative / unverifiable | — | Replaced with a "What every plan includes" Datawrap-only table |
| "Controls exist; no SOC 2 or ISO certificate is claimed…" | negative | — | Removed; CTA offers MSA, DPA and security questionnaire |
| Prices: Free / Custom / Custom | current (owner-owned) | no pricing source in repo | Unchanged — owner decision |

### 3.3 Enterprise (`#/enterprise`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "SAML/OIDC and SCIM-ready roles" | stale | SCIM groups map to roles, `scim_service.py:752` | "SCIM 2.0 group → role provisioning" + email-domain routing |
| "Dedicated tenants, custom domains, and region pinning" | wrong (region pinning) | `transfer_router.py:129` | Workspace isolation + custom domains + data region check |
| "Purpose keys stay scoped to the job" | wrong | `byok_key_manager.py` | BYOK with local, wrapped or AWS KMS keys |
| "Proof ready for SOC review" / "Immutable logs" | wrong (SOC framing; "immutable") | `audit_chain_keys.py` | "Tamper-evident audit chain" with per-record key ids, verify endpoint, filters, export |
| "Informatica-class procurement, Fivetran-class clarity" | negative / comparison | — | Removed |
| Panel "SSO enforced" | wrong | no SSO-only enforcement flag | "OIDC / SAML" |
| "Region pinning for jobs and artifacts" | wrong | as above | Replaced |
| Missing: API keys capped at creator role, signed webhooks, session timeout, platform-admin deployment settings, audit filters, retention purge, workspace isolation | missing | §2 | Added as capability cards + checklist |
| Compliance badges "Encryption at rest", "Regional residency" | wrong (broad) | credentials are encrypted at rest; region is a destination check | "Credentials encrypted at rest", "Workspace data region" |

### 3.4 Customers (`#/customers`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "Every load leaves proof — not a logo wall" / "We do not invent customer marks" | negative | — | Positive headline about proof on your data |
| SLAs incl. "0 Silent drops by design" | wrong (absolute) | — | "Value-by-value verification" |
| "Snowflake, BigQuery, Redshift, Databricks — MERGE + MATCH" | wrong | matrix:377-378 | "Snowflake, BigQuery, Iceberg — MERGE + verified" |
| "At-least-once upsert until a route proves dest-owned EOS" | negative | — | "SQL Server, PostgreSQL, MySQL, Oracle — snapshot, stream, resume" |
| Proof highlights (48 / 43 / 14 / 33 live cases) | current | `provenEvidence.ts` `PROVEN_EVIDENCE` | Kept |
| Stack notes ("stay Planned", "Connecting a catalog tile is not a live transfer") | negative | — | `MARKETING_STACK` rewritten to name only transfer-ready drivers |
| No customer names or quotes | current | test `provenEvidence.test.ts` "no attributed customer quote" | Kept — owner decision if references become available |

### 3.5 Security (`#/security`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "No SOC 2 or ISO certificate is claimed until…" | negative | — | Removed |
| Layers: Isolate / Encrypt / Reside / Prove | partly wrong | as Trust above | Rewritten to code-true wording |
| Badges | partly wrong | as Enterprise | Adjusted |
| Agents "never receive raw destination passwords" | current | `docs/MCP.md` | Kept (positive form) |
| Missing: audit chain, signed webhooks, session timeout, SCIM, API-key caps, per-workspace PII key | missing | §2 | Added a "Controls your admins switch on" block reusing the layer grid |

### 3.6 Connectors (`#/integrations`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "Catalog tiles are not transfer-live … stay Planned" | negative | — | Removed; hero states 46 transfer-ready drivers |
| Strip "Mixed — Warehouse tiles — not all live" | negative | — | Replaced with CDC / Vectors strip items |
| Family badges "Mixed / Planned" | negative | — | Badges now "Transfer-ready" |
| Families list (Warehouses incl. Redshift, Databricks; Apps incl. Shopify) | wrong | matrix | Families: Warehouses & lakehouse, Object storage, Databases, Streaming, Vector databases, Applications, Files |
| "Upserts only where the destination truly supports them" | current | — | Kept, positive voice |

### 3.7 Contact (`#/contact`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "We do not call a green status done" | negative | — | Positive verification line |
| FAQ "CDC stays at-least-once…", "Do you claim 700+ live connectors?" | negative | — | Rewritten: CDC engines; "How many connectors are live?" → 46 transfer-ready |
| FAQ "How is this different from Fivetran…" naming competitor behaviour | unverifiable | — | Answer states Datawrap strengths only |
| Block "What we will not claim on the call" | negative | — | Replaced by "What you will see on the call" |
| Table "Advanced where Fivetran, Airbyte, Informatica, and Databricks stop" | negative / unverifiable | — | Replaced with Datawrap-only "What the pilot proves" table |
| Response time "one business day" | current (owner-owned) | — | Kept |
| Hero note "No SOC 2 certificate is claimed." | negative | — | Removed |
| Form destination chips Redshift, Databricks | wrong (implies support) | matrix:377-378 | Iceberg / S3 and Oracle |

### 3.8 Product and solution pages (`#/product/*`, `#/solutions/*`)

| Claim | Status | Evidence | Action |
|---|---|---|---|
| Transfer Studio "refuses silent data loss", G1–G9, confidence floor 0.85 / 0.72 | current | `productPageShared.tsx` gate table vs `models.py` | Kept; write step names Gate-8 census |
| Job Theater "Zero silent data loss." callout | wrong (absolute) | — | "Every row accounted for" |
| Job Theater reconcile "destination sample" | stale | Gate-8 now reads back values and keys | Updated to value + key census |
| Pipelines — write modes, drift blocking | current | §2 | Adds Mirror soft delete, typed watermarks, CDC engines, manual drift review |
| Pipelines CDC band "Snapshot handoff, then idempotent streaming upserts" | current | `cdc_engine.py` | Names the four engines |
| Query Playground — read-only, "refused" wording | negative (tone) | — | Positive voice, same facts |
| Pilot "Natural-language triage" | stale / missing | `pilot_agent.py:1413` | Adds own local engine, no cloud keys, Confirm before anything moves |
| MCP "Representative tool groups" | current | `docs/MCP.md` | Adds schedules, deny-by-default allow-list, API-key role caps |
| Warehouse solution "Redshift and Databricks stay Planned until…" / "Honest labels — Planned stays Planned" | wrong + negative | matrix | Snowflake, BigQuery, Iceberg, S3/ADLS/GCS; positive voice |
| Migrations "Checksum-signed cutover" | current | `signed_proof_pack.py` | Kept, names signed proof pack |
| Missing: Unicode/collation, DATETIME2(7), encoding detection | missing | §2 | Added to Migrations "real-world schemas" list |

### 3.9 Docs landing (`#/help`) — `DocsPortal.tsx`

| Claim | Status | Evidence | Action |
|---|---|---|---|
| "Catalog families — tiles are not transfer-live" | negative | — | "Catalog families" |
| "What is certified on live engines (Aug 2026)" | wording | `provenEvidence.ts` | "Measured on live engines" |
| Help article: "Settings → Enterprise sets … MFA … IP allowlist" (`helpDocs.ts:314,1259`) | wrong (implies MFA enforcement; allowlist on shared host) | `capability_contract.py:479,484` | MFA removed; IP allowlist scoped to custom domains |
| Help article "Security posture report for SOC 2 / GDPR / HIPAA questionnaires" (`helpDocs.ts:317`) | current (questionnaire support, no certificate) | — | Left as is (operator documentation) |
| Help articles' CDC at-least-once notes | current (technical docs) | — | Left as is: operator docs, not marketing |

### 3.10 Chrome, SEO, static files

| Claim | Status | Evidence | Action |
|---|---|---|---|
| Footer "Catalog tiles are not transfer-live. CDC stays at-least-once…" | negative | — | Footer tagline rewritten |
| Nav "Native drivers + SQLAlchemy generics" | current | — | "46 transfer-ready drivers" |
| `PUBLIC_PAGE_META` titles/descriptions | stale (no verification, CDC, vectors, enterprise identity) | — | Rewritten to match new copy |
| `seo.ts` `DEFAULT_DESCRIPTION`, `landing` | stale | — | Rewritten |
| `index.html` description / OG / Twitter "9 preflight gates … G8 reconcile proof" | stale | — | Rewritten |
| `index.html` JSON-LD block ends `]},` | **wrong — invalid JSON** (trailing comma, crawlers drop the whole graph) | `apps/web/index.html` JSON-LD line | Fixed; featureList updated |
| Runtime JSON-LD description (`web/lib/usePageMeta.ts`) | stale | — | Rewritten |
| `sitemap.xml` | current | lists every marketing route | `lastmod` not used; unchanged |
| `robots.txt` | current | — | Unchanged |

## 4. Not claimed on purpose

These stay off every page. None of them is mentioned on a page, not even as a
"we don't" sentence.

| Item | Why | Evidence |
|---|---|---|
| MFA enforcement | Designed, not built; `mfa_enforced` stays false | `docs/MFA_ENFORCEMENT_DESIGN.md`, `capability_contract.py:479` |
| SOC 2, ISO 27001, HIPAA or any certification | No third-party audit or certificate in the repo | `provenEvidence.ts` `NOT_PROVEN`; guarded by `provenEvidence.test.ts` |
| "Certified" / "production-proven" for engine pairs without real-engine tests | The live matrix covers PostgreSQL, MySQL, SQL Server, Oracle, SQLite (and Mongo, Kafka, SFTP in e2e); other pairs are not claimed as certified | `provenEvidence.ts` `PROVEN_EVIDENCE` |
| Redshift, Databricks, ClickHouse, MariaDB, CockroachDB, Shopify as supported | Catalog status `planned` | `CONNECTOR_READINESS_MATRIX.md` |
| IP allowlist on the shared app host | Applies only to tenant custom domains | `capability_contract.py:484,923` |
| Region pinning of jobs and artifacts | Only a destination-region check exists | `transfer_router.py:129` |
| "Purpose keys" scoped per job | No such mechanism; BYOK is per tenant | `byok_key_manager.py` |
| Azure Key Vault / GCP KMS for BYOK | Marked roadmap in code | `byok_key_manager.py:17` |
| Any throughput or latency number (rows/s, "x faster", "1M rows in N s") | No benchmark artifact committed for marketing; `docs/THROUGHPUT_1M_EVIDENCE.md` is environment-specific | — |
| "Zero data loss" / "0 silent drops" absolutes | Replaced by "verifies every load value by value and tells you if anything does not match" | — |
| Exactly-once CDC as a platform claim | `PLATFORM_EXACTLY_ONCE_CLAIMED` is False | `provenEvidence.ts` `NOT_PROVEN` |
| Backend test count | Last published figure is August 2026 and today's base has known environmental failures (AGENTS.md) | `provenEvidence.ts` `BACKEND_SUITE` |
| Named competitor behaviour (Fivetran, Airbyte, Informatica, Databricks) | Not verifiable from this repo; the new policy prefers Datawrap-only strengths | `docs/COMPETITIVE_ANALYSIS.md` would need sourced citations first |
| Customer names, logos, quotes | None verified | guarded by `provenEvidence.test.ts` |
| Snowflake Secure Data Sharing, Dynamic Tables | Not implemented | `capability_contract.py:269,1401` |

## 5. Owner decisions

1. **Snowflake evidence.** Pages now present Snowflake as a supported source
   and destination on the owner's real-account attestation. Commit the run
   artifact (account edition, routes, rows, Gate-8 verdict) next to
   `docs/QA_DEFECT_TABLE.md` and flip its `emulator-verified only` rows.
2. **Pricing.** Plans stay Free / Custom / Custom. No price points exist in
   the repo.
3. **Customers page.** Still evidence-led with no customer names. Add logos
   or quotes only with written permission (the unit test blocks quotes).
4. **Competitor comparisons.** Removed from Home, Pricing and Contact. If
   wanted back, source each statement in `docs/COMPETITIVE_ANALYSIS.md` first.
5. **Evidence date.** `EVIDENCE_AS_OF` stays "August 2026" because the live
   matrix artifacts were recorded then. Re-run the matrix to refresh it.
6. **Legal pages.** Privacy and Terms say "Last updated August 2026" and
   mention "default 90 days" retention; the retention purge now exists
   (`job_retention.py`). Legal should confirm the default before any change.

## 6. Verification of this branch

- Guards: `web/lib/provenEvidence.test.ts` now pins the policy — no planned
  connector, MFA, IP allowlist or zero-loss absolute on a public page, no
  disclaimer copy, and `MARKETING_STACK` names only transfer-ready drivers.
- Screenshot baselines: the only `toHaveScreenshot` baselines are
  `apps/web/e2e/wizard-parity.spec.ts-snapshots`, `wizard.spec.ts-snapshots`
  and `studio-next.spec.ts-snapshots` (wizard, login, query, settings,
  canvas). None renders a marketing page or a module this branch changed,
  so none needs regenerating. `e2e/prod-smoke.spec.ts:23` asserts the hero
  heading "Move any schema", which is unchanged.
