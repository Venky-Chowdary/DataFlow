# Datawrap — Client Readiness Report

Measured state of the product for a client handover decision. Every claim below
is backed by a named artifact (live run JSON, pytest node ids, or a file in this
repository). Where something is unproven or unmeasured it says so; there are no
invented percentages and no coverage claims derived from connector tile counts.

- Branch: `devin/1786307905-audit-fixes` · PR #34
- Base of comparison: `devin/deep-audit-1784855991`
- Commits on this branch: 46
- Live engines used for proof: PostgreSQL 16, MySQL 8, SQL Server 2022,
  Oracle Free 23 (FREEPDB1), MongoDB 7, SQLite, DuckDB
- Frontend build: `npm run build` exit 0

---

## 1. Verdict in one paragraph

The **migration-assurance core is production-grade and independently
evidenced**: schema and constraint carry, fail-closed preflight, quarantine with
a balanced row ledger, post-write checksum reconciliation against a destination
re-read, and a signed certificate whose verdict is now vetoed by physical-state
evidence rather than by row counts alone. The parts a client will exercise on
day one — relational source → relational destination, create-new or
append-into-existing, incremental append, upsert, overwrite — are proven live on
four engines. **Warehouse and SaaS routes (Snowflake, BigQuery, S3, Salesforce)
are not live-proven** because no credentials have been provisioned to this
environment; they remain Planned/unproven regardless of the code that exists for
them. **Jobs, Schedules, Pipelines, Transforms and the UI have not been audited
to the same bar** — they are implemented and unit-tested, not matrix-proven.

Recommended handover posture: hand over the **relational migration-assurance
workflow** now, with warehouse/SaaS and the operations surfaces explicitly
labelled as the next certification waves.

---

## 2. What is proven, with the artifact that proves it

Every live artifact is a real transfer through the product path against a real
engine, followed by an *independent* re-read of the destination catalog or rows.
Emitted DDL is never accepted as proof.

| Capability | Engines | Cases | Result | Artifact |
|---|---|---|---|---|
| CHECK constraint carry (proven by destination rejecting the row) | PG, MySQL, SQL Server, Oracle | 16 | 16 ok | `check_carry_live_results.json` |
| Create-new fidelity (PK / NOT NULL / DEFAULT / UNIQUE / CHECK / column order) | PG, MySQL, SQL Server, Oracle | 4 | 4 ok | `create_new_fidelity_live_results.json` |
| Secondary index carry | PG, MySQL, SQL Server, Oracle | 16 | 16 ok | `secondary_index_live_results.json` |
| Physical placement (partitioning, tablespace, filegroup) | PG, MySQL, SQL Server, Oracle | 10 | 10 ok | `physical_placement_live_results.json` |
| Physical storage metadata probe | PG, MySQL, SQL Server, Oracle | 12 | 12 ok | `physical_storage_live_results.json` |
| Foreign key carry, multi-table, dependency-ordered | PG, MySQL, SQL Server, Oracle | 9 | 9 ok | `foreign_key_live_results.json` |
| Foreign key carry, **single-table into an existing parent** | PG, MySQL | 2 | 2 ok | `fk_single_table_live_results.json` |
| Validate→Execute DDL identity (append, incremental delta, duplicate re-run refusal, narrowed-DDL refusal) | PG, MySQL, SQL Server, Oracle, SQLite | 5 engines | all ok | `ddl_identity_matrix_results.json` |
| Schema drift (widen, narrow-refuse, NOT NULL, defaults, concurrency, resume, case variants) | PostgreSQL | 12 | 12 recorded, 0 violations | `drift_live_results.json` |
| Destination privilege probe | PG, MySQL, SQL Server, Oracle | 7 | 7 ok | `grants_live_results.json` |
| Oracle catalog identity (quoted/case-folded tables, duplicate probe) | Oracle | 10 | 10 ok | `oracle_live_results.json` |
| Excel/CSV blank-row defect, before/after | Excel → PostgreSQL | — | 95 phantom rows → 0 | `excel_phantom_rows_live_results.json` |
| 30-source-column → 20-column destination scenario matrix | PG, MySQL, Oracle | 12 scenarios × 3 | see §3 | `migration_scenario_matrix_results.json`, `docs/MIGRATION_SCENARIO_MATRIX.md` |

Artifacts live in `/home/ubuntu/repro/` on the build machine and are referenced
from PR #34.

### Behaviour a client can rely on today

- **No silent column loss.** A source column that is neither mapped nor declared
  an intentional omission blocks the run before any write, naming the columns.
- **No silent narrowing.** Lossy type changes fail closed unless an explicit
  Risk Contract is signed; the refusal names the column.
- **Duplicates.** Source duplicate probe and destination key-collision probe run
  *before* the write; a re-run without a cursor delta is refused with 0 rows
  written rather than duplicating history.
- **Row ledger.** `rows_read == rows_written + rows_quarantined + rows_skipped`
  is enforced; an unbalanced ledger downgrades the certificate verdict.
- **Quarantine, not drop.** Rejected rows are durable in the DLQ with the
  original value, expected type, reason and keys, and are replayable.
- **Proof is a re-read.** Gate-8 digests the mapped projection read back from the
  destination; a planned DDL or a catalog count is never accepted as proof.
- **The certificate can say no.** Absent constraints, an incomplete foreign key
  carry, orphan rows, or an identity generator behind the data now veto
  `migration_proven`.

---

## 3. Defects found and fixed in this wave (all previously green-and-wrong)

These were found by live matrices, not by unit tests, and each one had the same
signature: the run reported success while the destination was wrong.

1. **30 source columns into a 20-column destination silently dropped 10 columns**
   and reported green on PG/MySQL/Oracle. Now blocked pre-write; declaring the
   columns as intentional omissions succeeds.
2. **A destination column nothing maps into failed Gate-8 *after* committing
   rows.** Gate-8 now digests the mapped projection, not `SELECT *`.
3. **A destination `NOT NULL` without a default** was caught by the driver at row
   1 instead of predicted at Validate. Now blocked at Validate, naming the
   column — and a column filled by DEFAULT/identity/generated does not false-block.
4. **Oracle created a second table** beside a quoted lower-case one and split a
   job's rows across both, both runs "passing"; the duplicate probe died on
   ORA-22849 and degraded to a skip. One shared catalog-identity resolver now
   serves introspect, writer, collision probe and Gate-8.
5. **MySQL / SQL Server / Oracle create-new never consulted the fidelity
   planner**, so source PK/NOT NULL/DEFAULT/UNIQUE were dropped with no
   certificate saying so.
6. **Uppercase source columns lost PRIMARY KEY, NOT NULL and UNIQUE** on
   create-new — i.e. every Oracle source — while the certificate said `carried`.
7. **Validate→Execute DDL identity mismatch** blocked real jobs (the `users`
   failure): Execute recomputed the fingerprint from live catalog spellings
   (`TIMESTAMP_NTZ(6)` vs `DATETIME(6)`, collation suffixes) instead of the
   approved Map stamp. The same divergence was silently breaking Gate-8
   checksums. One canonical materializer now serves both.
8. **Excel/CSV formatting-only rows** were loaded as all-NULL rows and counted by
   `max_row`, so reconciliation compared against rows nothing read.
9. **MySQL/SQL Server/Oracle introspection never read column defaults**, and the
   Oracle column query selected a column that does not exist in
   `ALL_TAB_COLUMNS` (ORA-00904 on every Oracle), silently falling back to an
   identity-blind query.
10. **Sparse documents** (Mongo/DynamoDB): a missing key became `""` and Validate
    reported a fake "empty value cannot coerce" failure, blocking transfers whose
    write path would have written SQL NULL or omitted the key.
11. **Single-table transfers never carried foreign keys at all** — the child
    landed with the reference dropped and the run went green.
12. **Foreign key catalog probes accepted a blank namespace** and answered
    "measured, no foreign keys": a carried and enforced MySQL key read back as
    not enforced.
13. **Declared source types were discarded** in favour of sample inference on the
    PostgreSQL writer, so a `VARCHAR(40)` source into a `VARCHAR(10)` destination
    quarantined instead of widening.
14. **Numeric width was invented or lost** across introspect → invent → bind:
    SQL Server `int` became `BIGINT`, MySQL `FLOAT` lost its 32-bit width,
    Snowflake `FLOAT` (IEEE-64) was treated as single precision, and Oracle ANSI
    `FLOAT(p)` (NUMBER-backed, ~38 decimal digits) was collapsed to
    `BINARY_DOUBLE`'s 53-bit mantissa.

---

## 4. Area-by-area status

Status vocabulary: **Proven** = live matrix artifact against a real engine with
an independent re-read. **Tested** = unit/integration tests only. **Planned** =
code may exist, capability is not evidenced. **Unaudited** = works as far as we
know; not examined at this bar. **Blocked** = cannot be proven here.

| Area | Status | Evidence / reason |
|---|---|---|
| Relational transfer (PG, MySQL, SQL Server, Oracle, SQLite) | **Proven** | matrices in §2 |
| Create-new DDL: columns, types, NOT NULL, DEFAULT, PK, UNIQUE, CHECK, indexes, placement | **Proven** | §2 rows 1–5 |
| Foreign key carry (multi-table and single-table) | **Proven** | 9/9 and 2/2 live |
| FK cycles | **Planned** | reported and now a blocker; deferred-constraint creation not implemented |
| Preflight / Validate gates (13 rules) | **Proven** on the relational path | gap1/gap3/gap4 matrices |
| Quarantine + replay | **Tested**, exercised in every live matrix | DLQ durability tests |
| Gate-8 reconciliation (row count + checksum + destination re-read) | **Proven** | ddl_identity and drift matrices |
| Migration certificate / audit PDF / signed proof pack | **Tested**; verdict veto newly added | `tests/test_migration_certificate.py`, `tests/test_certificate_pdf.py` |
| Schema drift | **Proven on PostgreSQL** (12 scenarios) | not yet re-run on MySQL/SQL Server/Oracle |
| Identity / sequence high-water marks | **Tested** (read + forward-only repair, vetoes the verdict) | no live matrix yet |
| MongoDB / DynamoDB (sparse documents) | **Tested**; Mongo→Snowflake focused path passes | no live matrix |
| DuckDB / Iceberg / lakehouse MERGE | **Tested** | backfill widening regression is live-proven on DuckDB |
| Snowflake, BigQuery | **Blocked** | no credentials in this environment; emulator only |
| S3 / ADLS / GCS | **Blocked** | no credentials |
| Salesforce, Stripe, Shopify, Airtable, HubSpot | **Planned / Blocked** | Salesforce org needs SSO login; no integration user provisioned |
| CDC (PostgreSQL logical, MySQL binlog, SQL Server CDC, Oracle LogMiner, Mongo change streams) | **Tested**, labelled **at-least-once** | exactly-once is not claimed and not proven |
| Jobs surface | **Unaudited** | implemented, 18 test files touch it |
| Schedules | **Unaudited** | 49 passed / 1 skipped in the focused selection |
| Pipelines / Transforms | **Unaudited** | implemented, not examined at this bar |
| Operations / Contracts / Proofs pages | **Unaudited** | — |
| Web UI | **Builds clean**; not audited | `npm run build` exit 0 |
| Connector catalog | 44 unique drivers with PRODUCTION_SKU evidence across 77 routes | `apps/api/data/proofs/transfer_ready_matrix.json`; tile count is explicitly **not** a live-capability claim |

---

## 5. Known gaps a client must be told about

1. **Warehouse and SaaS destinations are unproven here.** Snowflake/BigQuery/S3/
   Salesforce need credentials before any claim is made. Salesforce also needs a
   dedicated least-privilege integration user or Connected App.
2. **CDC is at-least-once.** Resume and replay upsert; exactly-once is not
   claimed.
3. **Schema drift is proven on PostgreSQL only.** The MySQL / SQL Server /
   Oracle drift matrix has not been re-run since the VM restart.
4. **FK cycles are refused, not resolved.** No deferred-constraint strategy.
5. **Triggers, stored procedures and views are not migrated.** Reported as
   advisory in the physical-state section; recreate them before cutover.
6. **SQL Server partition functions/schemes, Oracle partitioned tables and
   PostgreSQL CLUSTER ordering are not carried** — refused honestly rather than
   invented.
7. **Transfer Studio has no cursor-column field**, so an incremental append is
   configured through the API/contract rather than the UI.
8. **Operations surfaces (Jobs, Schedules, Pipelines, Transforms, UI) have not
   been audited** at the migration-assurance bar.
9. **The existing `excel` table on the client's Railway Postgres still holds the
   95 all-NULL rows** from the load that predates the fix — it must be reloaded
   into a fresh table.

---

## 6. Test evidence

- Focused suites re-run for this wave, all green:
  - `tests/test_migration_certificate.py`, `tests/test_certificate_pdf.py` — 28 passed
  - `tests/test_foreign_key_metadata.py`, `tests/test_foreign_key_carry.py` — 32 passed
  - certificate / proof-pack / reconcile / foreign-key / physical-state selection — 170 passed, 5 skipped
  - stream / transfer-engine / multi-stream selection — 133 passed, 10 skipped
  - schedules selection — 49 passed, 1 skipped
  - sparse-document + type-contract + tracked-execute selection — 1144 passed, 7 skipped
- Frontend: `npm run build` exit 0.
- **Full backend suite: 13159 passed, 0 failed, 1515 skipped** (sharded run,
  `/home/ubuntu/repro/shards/summary.txt`). The 55 failures carried by the base
  branch are now closed; none were closed by weakening an assertion. The three
  classes they fell into:
  - shared product defects — the resume posture reading an unreadable committed
    row count as zero, Validate's coercion report contradicting a blocking gate,
    Snowflake `INT`/`SMALLINT` introspected as 64-bit when they are
    `NUMBER(38,0)`, Oracle `FLOAT` preservation keyed on letter case, source FK
    reads skipping SQL Server / Oracle / SQLite;
  - stale assertions pinned to one historical error string where the engine now
    refuses earlier and for a better-evidenced reason;
  - routes with no evidence available here (uncertified SaaS brands, Snowflake
    under `fakesnow`, which has no `GRANTS` catalog) — these assert the honest
    refusal or skip with a named reason rather than a mocked green.
- Full backend suite is run sharded (one pytest process per 60 files) because a
  single 14k-test process is killed by the OOM killer on this 7 GB VM; the runner
  is `/home/ubuntu/repro/run_sharded.sh` and the aggregate counts are recorded in
  `/home/ubuntu/repro/shards/summary.txt` per run.

---

## 7. Plan to handover

Effort is in **working sessions**, not calendar time, and excludes waits for
credentials.

| # | Work | Sessions | Blocked on |
|---|---|---|---|
| 1 | Re-run the drift matrix on MySQL / SQL Server / Oracle; close whatever it finds | 1 | — |
| 2 | Live matrix for identity/sequence watermarks and their repair | 1 | — |
| 3 | Jobs + Schedules audit at this bar (retry, concurrency, multi-instance, missed windows, backfill) | 1–2 | — |
| 4 | Pipelines + Transforms audit | 1 | — |
| 5 | UI/UX audit against the engine: one primary action per root cause, cursor field in Transfer Studio, no claim the engine does not support | 1–2 | — |
| 6 | Warehouse certification (Snowflake, BigQuery, S3) | 1–2 | credentials |
| 7 | SaaS certification starting with Salesforce | 1–2 | integration user / Connected App |
| 8 | FK cycles via deferred constraints, plus trigger/view reporting in the certificate | 1 | — |
| 9 | Final handover pack: signed proof bundle per certified route, runbook, rollback plan | 1 | items 1–8 |

The critical path to a defensible client handover of the **relational** product
is items 1, 2, 3 and 9 — the rest widens the certified surface.

---

## 8. What this report deliberately does not say

- No overall readiness percentage. A single number over unaudited surfaces would
  be invented.
- No "N connectors live". 44 drivers carry PRODUCTION_SKU evidence; the catalog
  is larger and the difference is not capability.
- No claim that any Snowflake, BigQuery, S3 or Salesforce route works in a
  client environment. They have not been run against a real account here.
