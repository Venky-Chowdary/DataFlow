# Datawrap — Client Readiness Report

> **2026-08-27 Validate≡Execute wave:** use
> [`docs/CLIENT_HANDOVER_VALIDATE_EXECUTE.md`](CLIENT_HANDOVER_VALIDATE_EXECUTE.md)
> for the `flights-1m.csv` → Snowflake incident, operator runbook, and measured
> population-fit proof. This file remains the prior relational-assurance pack.
> Do not mix those live-engine counts with the 2026-08-27 scan matrices.

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
them. **The UI has not been audited to the same bar** — it is implemented and
unit-tested, not matrix-proven. Jobs and Schedules are audited for retry,
overlap, cancellation and cadence (see §4); backfill and multi-instance failover
are not. Transforms are audited for load correctness (33/33 live on three
engines, `docs/TRANSFORM_LAYER_AUDIT.md`); a per-row ledger and quarantine inside
a transform do not exist yet.

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
| Identity / sequence generator carry on create-new (destination catalog re-read + post-cutover client insert without a key) | PG, MySQL, SQL Server, Oracle | 16 routes | 14 carried, 2 declared unsupported | `identity_live_results.json` |
| Non-default key sequence `IDENTITY(1000,10)` carried (seed, increment, and the progression the next client key lands on) | SQL Server → SQL Server / PG / Oracle | 3 | 3 ok | `identity_seed_live_results.json` |
| Schema drift (widen, narrow-refuse, NOT NULL, defaults, concurrency, resume, case variants) | PostgreSQL | 12 | 12 recorded, 0 violations | `drift_live_results.json` |
| Schema drift, same 12 scenarios | MySQL, SQL Server, Oracle | 36 | 36 recorded, 0 violations | `drift_live_multi_results.json` |
| Destination privilege probe | PG, MySQL, SQL Server, Oracle | 7 | 7 ok | `grants_live_results.json` |
| Oracle catalog identity (quoted/case-folded tables, duplicate probe) | Oracle | 10 | 10 ok | `oracle_live_results.json` |
| Incremental transform loads (column order, omitted/extra columns, required columns, `unique_key`, case, first-run duplication) | PG, MySQL, SQL Server | 33 | 33 ok (18 broken before the fix) | `transform_live_results.json`, `transform_live_base_results.json`, `docs/TRANSFORM_LAYER_AUDIT.md` |
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
15. **Create-new never carried the key generator.** A source `SERIAL` /
    `AUTO_INCREMENT` / `IDENTITY` landed as a plain integer column: every row
    reconciled, every checksum matched, and the client's first insert after
    cutover had no key to use (16/16 routes lost the generator). The generator
    is now planned per destination, emitted, and only reported `carried` after
    the destination's own catalog reports it — with a post-cutover insert
    without a key proving the generated value is non-null, above the copied
    maximum, and non-colliding. Oracle → PostgreSQL/MySQL is declared
    **unsupported** rather than silently dropped, and an unsupported or unknown
    generator now vetoes `MIGRATION PROVEN`.
16. **A SQL Server key sequence was reset to `IDENTITY(1,1)`.**
    `sys.identity_columns.seed_value` is `sql_variant`, which pyodbc returns as
    little-endian bytes; `int()` raised and the swallowed error left the column
    looking like a plain `BIGINT`. A table keyed 1000, 1010, 1020 was recreated
    to number new rows 1, 2, 3. Seed and increment are now decoded, carried into
    the destination's own generator syntax, and the forward-only watermark
    repair resumes on the progression (1030) instead of `MAX+1` (1021).
17. **Oracle generator state read as absent.** SQLAlchemy's inspector hides
    tables in the SYSTEM/SYSAUX tablespaces, so the watermark probe reported
    "column is not a GENERATED AS IDENTITY column" for a column that was one.
    One shared catalog fallback now serves the watermark and RI probes.
18. **Every write into a client-created Oracle table failed with `ORA-00904` on
    a column that is plainly there.** Reflection normalises Oracle's stored
    `LABEL` to `label`, and every statement here quotes its identifiers, so the
    writer asked for `"label"` — a different, case-sensitive column. Appends and
    upserts into any table created by ordinary Oracle DDL were refused, drift
    widening emitted `MODIFY ("name" CLOB)` against `NAME`, and `ADD COLUMN`
    added a quoted lower-case `"extra"` beside the folded columns, invisible to
    the client's own `SELECT extra`. Column identity is now resolved from the
    catalog through the dialect's own `denormalize_name`, and an added column
    follows the convention of the table it joins. Only tables our own earlier
    create-new produced (quoted lower-case) kept the route working, which is why
    the create-new matrices stayed green.

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
| Schema drift | **Proven** on PG, MySQL, SQL Server, Oracle (12 scenarios each) | `drift_live_results.json`, `drift_live_multi_results.json` |
| Identity / sequence generator carry + high-water marks | **Proven** on PG / MySQL / SQL Server / Oracle (16 routes, catalog re-read, post-cutover insert); SQLite refused explicitly | `identity_live_results.json`, `identity_seed_live_results.json` |
| MongoDB / DynamoDB (sparse documents) | **Tested**; Mongo→Snowflake focused path passes | no live matrix |
| DuckDB / Iceberg / lakehouse MERGE | **Tested** | backfill widening regression is live-proven on DuckDB |
| Snowflake, BigQuery | **Blocked** | no credentials in this environment; emulator only |
| S3 / ADLS / GCS | **Blocked** | no credentials |
| Salesforce, Stripe, Shopify, Airtable, HubSpot | **Planned / Blocked** | Salesforce org needs SSO login; no integration user provisioned |
| CDC (PostgreSQL logical, MySQL binlog, SQL Server CDC, Oracle LogMiner, Mongo change streams) | **Tested**, labelled **at-least-once** | exactly-once is not claimed and not proven |
| Job retry / resume duplicate safety | **Proven** — retry from start is refused when the failed attempt committed rows under a non-convergent sync, and for a cancelled run; allowed when nothing was committed or the mode converges | `schedule_live_results.json` (14/14, live PG), `tests/test_retry_duplicate_guard.py` |
| Schedule retry durability, overlap, missed windows | **Proven** — a parked retry survives a restart, waits out its backoff, and suppresses the cadence until it runs; a second beat cannot claim a running schedule; skipped windows are counted | `schedule_live_results.json` |
| Jobs surface (cancellation, checkpoints, leases beyond the above) | **Partly audited** | retry/resume/cancel-retry paths audited; worker leases and claim-queue coordination not yet at this bar |
| Schedules — backfill, multi-instance lock under a real Mongo failover | **Unaudited** | single-instance and file-backed paths proven; no backfill API exists yet |
| Transforms — incremental load correctness | **Proven** — the executed load names both column lists, matched to the target by name; a column the target lacks, an unfilled required column, or a `unique_key` missing from either side stops the load before it writes | `transform_live_results.json` (33/33 live PG/MySQL/SQL Server), `transform_live_base_results.json` (18/33 broken before), `docs/TRANSFORM_LAYER_AUDIT.md` |
| Transforms — row ledger, quarantine, type fidelity through a model | **Unaudited** | `rows_affected` is a driver rowcount, not a read/written/quarantined account |
| Transforms on Snowflake / BigQuery / Databricks / Trino | **Unproven** | unit-covered statements only; no credentials for a live run |
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
3. **Oracle concurrent drift can lose one writer's ALTER to a DML lock**
   (`ORA-00054`). The refused writer fails loudly with the lock named and
   nothing is written, so no column is silently lost, but two writers evolving
   the same table concurrently need a retry policy.
4. **FK cycles are recreated after load**, not refused. PostgreSQL and Oracle
   emit `DEFERRABLE INITIALLY DEFERRED` on cycle and self-referential edges;
   the certificate blocks only when a cycle is present and `cycle_resolved`
   is not true. Proven on the named live matrix in
   `apps/api/tests/test_fk_cycle_post_load.py` (PR #94). SQL Server / MySQL
   use the portable post-load `ALTER` path without deferred constraints.
5. **Triggers, stored procedures and views are not migrated.** They are named
   on the certificate as advisory physical-state aspects
   (`cutover_recreate`) so cutover recreates them. Name presence is not a
   body-carried claim; advisory never vetoes `migration_proven`. Views and
   named triggers: PR #95. Dependent procedures / functions: this wave
   (`apps/api/tests/test_routine_cutover_matrix.py`). SQL Server and Oracle
   catalog queries exist; they are not live-proven on this VM.
6. **SQL Server partition functions/schemes, Oracle partitioned tables and
   PostgreSQL CLUSTER ordering are not carried** — refused honestly rather than
   invented.
7. **An incremental cursor's meaning must be declared by the operator.** The
   product refuses to infer it from a column name, so an incremental /
   deduped / CDC run whose cursor carries no declaration is blocked at Validate
   rather than run with an unproven read. `business_date` is refused outright:
   a backdated insert stays behind the watermark permanently. Studio's stream
   table now carries the declaration beside the cursor and shows the engine's
   verdict; `cursor_semantics_live_results.json` records 18/18 on
   PostgreSQL→PostgreSQL and PostgreSQL→MySQL. Other connector families are not
   yet in that matrix.
8. **The rest of the UI has not been audited** at the migration-assurance bar
   — only the cursor / stream contract surface has been.
   Jobs/Schedules are audited for retry, overlap, cancellation and cadence only;
   see below for the transform limits.
   Transforms are audited for load correctness only — a transform failure is
   surfaced but is not replayable per row, and it does not veto the landed
   transfer's own proof. Incremental transforms are refused on Oracle, which has
   no `CREATE TABLE IF NOT EXISTS` to seed them idempotently.
9. **Schedules have no backfill.** A historical window must be run as an
   explicit transfer; there is no scheduled catch-up that replays skipped
   windows — they are counted (`missed_window_count`) and surfaced, not replayed.
10. **A scheduled retry waits for the next beat** (up to `SCHEDULER_CHECK`
    interval, 60s) rather than firing exactly on its backoff, because retries
    are now durable store state instead of an in-process timer.
11. **The existing `excel` table on the client's Railway Postgres still holds the
   95 all-NULL rows** from the load that predates the fix — it must be reloaded
   into a fresh table.

---

## 6. Test evidence

- Focused suites re-run for this wave, all green:
  - `tests/test_migration_certificate.py`, `tests/test_certificate_pdf.py` — 28 passed
  - `tests/test_foreign_key_metadata.py`, `tests/test_foreign_key_carry.py` — 32 passed
  - certificate / proof-pack / reconcile / foreign-key / physical-state selection — 170 passed, 5 skipped
  - stream / transfer-engine / multi-stream selection — 133 passed, 10 skipped
  - schedules + jobs selection — 187 passed, 3 skipped (includes the new
    `tests/test_retry_duplicate_guard.py`, 22 cases)
  - sparse-document + type-contract + tracked-execute selection — 1144 passed, 7 skipped
- Frontend: 351 tests passed, `tsc --noEmit` and `npm run build` exit 0.
- Cursor semantics live matrix: 18 cases, 0 not-ok
  (`/home/ubuntu/repro/cursor_semantics_live_results.json`) covering composite
  bookmark round-trips, an empty incremental poll reconciling as a no-op,
  refusal of an undeclared and of a calendar-date cursor, and acceptance of
  declared insert-only and modification-timestamp cursors, on
  PostgreSQL→PostgreSQL and PostgreSQL→MySQL.
- **Full backend suite: 13270 passed, 0 failed, 1515 skipped** (single run,
  25m26s, after the cursor-semantics wave; 13244 before it, 23m21s; the 13233 before it were a sharded
  run, `/home/ubuntu/repro/shards5/summary.txt`). The 55 failures carried by the base
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
| 3 | ~~Jobs + Schedules retry / overlap / cancellation / missed windows~~ — done, `schedule_live_results.json`; remaining: backfill + multi-instance Mongo failover | 1 | — |
| 4 | ~~Pipelines + Transforms audit~~ — done, `transform_live_results.json`; remaining: transform row ledger/quarantine and warehouse dialects | 1 | credentials for warehouse dialects |
| 5 | UI/UX audit against the engine: one primary action per root cause, no claim the engine does not support. Cursor contract done (cursor + declared meaning + verdict in Studio, `cursor_semantics_live_results.json` 18/18); remaining: the other panels | 1 | — |
| 6 | Warehouse certification (Snowflake, BigQuery, S3) | 1–2 | credentials |
| 7 | SaaS certification starting with Salesforce | 1–2 | integration user / Connected App |
| 8 | ~~FK cycles via deferred constraints, plus trigger/view/routine reporting in the certificate~~ — done: cycles post-load (PR #94), views+triggers named (PR #95), dependent routines named (this wave). SQL Server / Oracle routine catalogs are not live here | — | SQL Server / Oracle credentials for a live routine matrix |
| 9 | Final handover pack: signed proof bundle per certified route, runbook, rollback plan | 1 | items 1–8 |

The critical path to a defensible client handover of the **relational** product
is items 1, 2, 3 and 9 — the rest widens the certified surface.

---

## 8. What this report deliberately does not say

- No overall readiness percentage. A single number over unaudited surfaces would
  be invented.
- No "N connectors live". Unique TRANSFER_READY drivers are 46 when optional
  packages are present (`transfer_live_driver_types` /
  `TRANSFER_LIVE_WHEN_PACKAGES_PRESENT`). PRODUCTION_SKU is 86 routes. Catalog
  tiles are larger and the difference is not capability.
- No claim that any Snowflake, BigQuery, S3 or Salesforce route works in a
  client environment. They have not been run against a real account here.
