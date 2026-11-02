# Migration Scenario Matrix — measured behaviour

Every verdict below was produced by running the product path
(`UniversalTransferEngine.execute_tracked`, i.e. preflight gates → write →
Gate-8) against **live** engines: PostgreSQL 16 (`:5433`), MySQL 8 (`:3307`)
and Oracle Free (`:1521/FREEPDB1`), source always PostgreSQL.

Harness: `/home/ubuntu/repro/migration_scenario_matrix.py`
Artifact: `/home/ubuntu/repro/migration_scenario_matrix_results.json`
Contract matrix (append / delta / duplicate / DDL drift, 5 engines):
`/home/ubuntu/repro/ddl_identity_matrix.py` →
`/home/ubuntu/repro/ddl_identity_matrix_results.json`

Nothing here is aspirational. Where measured behaviour differs from the
contract the product should honour, the row says **GAP** and the gap is listed
again at the bottom with its severity. No scenario is rounded to green.

## Legend

| Term | Meaning |
|------|---------|
| `blocked pre-write` | Refused by a gate; **0 rows** reached the destination |
| `blocked at write` | The database rejected it; the job fails, but only the engine caught it |
| `written` | Rows committed and the job reported success |
| GAP | Measured behaviour is not the behaviour we should ship |

---

## 1. Column-count scenarios (the 30 → 20 case)

Source table: `id` + `c1..c30`. Destination: existing table with `id` + `c1..c20`.

| # | Scenario | PostgreSQL | MySQL | Oracle | Verdict |
|---|----------|-----------|-------|--------|---------|
| 1.1 | 20 of 30 source columns mapped, remaining 10 **not mentioned at all** | blocked pre-write | blocked pre-write | blocked pre-write | GAP-1 **fixed** |
| 1.2 | Same, remaining 10 marked `intentional_omit` | written, 5 rows | written, 5 rows | written, 5 rows | GAP-2 **fixed** |
| 1.3 | Destination has a `NOT NULL` column with no default and no mapping | blocked pre-write | blocked pre-write | blocked pre-write | GAP-3 **fixed** |
| 1.3b | Destination `NOT NULL` columns filled by DEFAULT / identity / generated | 5 rows written, job passed | same | same | no false block |
| 1.4 | Destination has an extra **nullable** column nothing maps into | 5 rows written, job passed | same | same | GAP-4 **fixed** |

Detail:

- **1.1** is the exact case in the question ("source has 30, destination has
  20"). It used to write 20 columns and report green. Gate `g13_source_coverage`
  now refuses before any write and names the columns:
  `10 source column(s) are neither mapped nor declared omitted: c21, c22, c23,
  c24, c25, c26, c27, c28 (+2 more) — Datawrap will not drop them silently.`
  Every source column must be a write mapping, an explicit `intentional_omit`,
  or a blocker.
- **1.2** declaring the omission — the honest operator action — used to be
  punished with `Sample transform / cast failures: 4 column(s) fail write-path
  transforms on the Validate sample (c21, c22, c23, c24)`. A declared omission
  has no destination carrier, so it is now excluded from the transform dry run,
  quarantine-cell preview, coercion probe, type-coercion validation and the
  Gate-8 read-back projection, and recorded as the operator's decision in the
  proof bundle (`source_coverage.omitted`) instead. Live re-run:
  `/home/ubuntu/repro/gap1_source_coverage_live_results.json`.
- **1.3** the destination's own `NOT NULL` used to be what stopped the write
  (`ORA-01400`, MySQL `1364`, PG `not-null constraint`): safe — 0 rows land —
  but the operator learned it from a driver error after approving. Gate
  `g14_destination_requirements` now refuses at Validate:
  `1 destination column(s) are NOT NULL with no default and no source mapping:
  tenant_id — the write would be rejected row 1.` The engine's own rejection
  stays as the fallback. A column is only treated as filled when the catalog
  proves a filler: a write mapping, a `DEFAULT`, an identity column or a
  generated column — an unreadable nullability catalog reports *unmeasured*,
  never pass. Collecting that proof exposed two further defects: MySQL, SQL
  Server and Oracle introspection never read `COLUMN_DEFAULT` / `DATA_DEFAULT`
  at all, and the Oracle column query selected `VIRTUAL_COLUMN` from
  `ALL_TAB_COLUMNS`, which does not have that column — the query failed with
  ORA-00904 on every Oracle and silently degraded to an identity-blind
  fallback, so virtual and identity columns looked like ordinary insertable
  ones. Live re-run:
  `/home/ubuntu/repro/gap3_destination_requirements_live_results.json`.
- **1.3b** guards the other direction: a destination whose required columns are
  filled by `DEFAULT`, identity or generated values must still run. 5 rows
  written on all three engines.
- **1.4** the untouched nullable column was read back by Gate-8 on the
  destination side but had no counterpart on the source side, so the checksums
  differed and the job was marked failed **after** committing 5 rows — a false
  failure with a real write behind it. Every read-back issues `SELECT *` and
  used the returned cursor description as the digest columns; it now projects
  to the mapped target columns through `services/readback_projection.py`
  before hashing, on all thirteen SQL/warehouse read-back paths. A mapped
  column the destination did **not** return is left unprojected so the
  mismatch still surfaces — narrowing the digest to hide it would be the same
  silent-drop failure. Live re-run:
  `/home/ubuntu/repro/gap4_mapped_projection_live_results.json`.

## 2. Type and fidelity scenarios

| # | Scenario | PostgreSQL | MySQL | Oracle | Verdict |
|---|----------|-----------|-------|--------|---------|
| 2.1 | `VARCHAR(255)` (120-char values) → `VARCHAR(10)` | blocked pre-write | blocked pre-write | blocked pre-write | correct — fails closed, no truncation |
| 2.2 | `DECIMAL(12,4)` → `DECIMAL(6,1)` | blocked pre-write | blocked pre-write | blocked pre-write | correct — precision collapse refused |
| 2.3 | `TIMESTAMPTZ` → naive `TIMESTAMP` / `DATETIME(6)` | blocked pre-write | blocked pre-write | blocked pre-write | correct as fail-closed; **needs a Risk Contract path** |
| 2.4 | `BOOLEAN` → `BOOLEAN` / `TINYINT(1)` / `NUMBER(1)`, incl. NULL | written `True/False/NULL` | written `1/0/NULL` | written `1/0/NULL` | correct |
| 2.5 | `JSONB` → `JSONB` / `JSON` | written, structure preserved | written, structure preserved | n/a | correct |
| 2.6 | `JSONB` → Oracle `CLOB` | — | — | blocked pre-write | fail-closed; **GAP-5** — CLOB is a legitimate JSON carrier on Oracle |
| 2.7 | Unicode / zero-width / newline / emoji `TEXT` → `TEXT`,`VARCHAR(400)`,`VARCHAR2(400)` | blocked pre-write | blocked pre-write | blocked pre-write | **GAP-6** — PG `TEXT`→`TEXT` is not a narrowing |

2.1–2.3 are the behaviour we want: unsafe narrowing never silently truncates
or rounds, and no rows are committed. 2.3 is defensible (an offset is lost)
but the operator has no in-product way to say "store UTC, I accept it" — the
Risk Contract exists for exactly this and is not wired to this verdict.

2.7 is a false positive: `TEXT → TEXT` on the same engine cannot collapse
fidelity. On PostgreSQL it trips a single gate; on MySQL and Oracle it is
reported as a narrowing to a bounded type, which is at least arguable there.

## 3. Identity, duplicates and existing data

| # | Scenario | PostgreSQL | MySQL | SQL Server | Oracle | SQLite |
|---|----------|-----------|-------|-----------|--------|--------|
| 3.1 | Append into an existing table | written 5 | written 5 | written 5 | written 5 | written 5 |
| 3.2 | Second incremental append with a cursor delta | written 5 (dest 10) | same | same | same | same |
| 3.3 | Re-run **without** a delta — every key already at rest | blocked pre-write, dest unchanged | same | same | same | same |
| 3.4 | Source itself contains a duplicate identity | blocked pre-write | blocked pre-write | — | blocked pre-write | — |
| 3.5 | Map stamp narrowed after Validate, empty destination | blocked pre-write, DDL identity named | same | same | same | same |
| 3.6 | Destination created with quoted mixed-case identifiers | written | written | — | written | — |

3.3 and 3.5 are distinct verdicts with distinct messages
(`Duplicate identity keys …` vs `Decision Artifact DDL identity diverged …`),
which was the point of the drift fixture: with rows at rest the duplicate gate
fires first, so the drift case only proves the DDL gate on an empty
destination.

3.6 on Oracle is the case that previously created a *second* table beside the
quoted one and split the job's rows across both; writer, introspection,
collision probe and Gate-8 read-back now resolve one catalog identity.

## 4. Sync-mode contract

| Mode | Behaviour today |
|------|-----------------|
| `incremental_append` | Appends; re-run without a delta is refused by the duplicate gate, not silently upserted |
| `full_refresh_append` | Appends |
| overwrite / upsert / merge | Require an explicit sync contract; append never silently becomes one |
| resume | Needs a committed checkpoint; a failed run offers Validate, not Resume |

## 4b. Physical placement on create-new

Measured live through the real writer path on PostgreSQL 16, MySQL 8, SQL
Server 2022 and Oracle Free; every verdict below is decided by **re-reading the
destination catalog after the write**, never by the SQL the planner emitted.
Artifact: `physical_placement_live_results.json` (10/10).

| # | Source → destination | Source placement (measured) | Certificate | Destination re-read |
|---|----------------------|------------------------------|-------------|---------------------|
| 4b.1 | PG → PG | RANGE on `created`, 2 partitions | `carried` | partitioned, 2 children, rows in them, an out-of-bound row rejected |
| 4b.2 | PG → PG, PK omits the partition key | RANGE on `created` | `unsupported`, names `PRIMARY KEY` | unpartitioned, PK intact |
| 4b.3 | PG → PG, secondary tablespace | `df_fast` | `carried` | table is on `df_fast` |
| 4b.4 | PG → PG, tablespace absent on destination | `no_such_space` | `unsupported` — "create it on the destination first" | default tablespace, rows written |
| 4b.5 | PG → PG, plain table | measured, unpartitioned, default | `skipped` (measured absence) | matches |
| 4b.6 | MySQL → MySQL | RANGE on `yr`, 3 partitions incl. `MAXVALUE` | `carried` | partitioned, 3 partitions, rows present |
| 4b.7 | PG → MySQL | RANGE on `created` | `unsupported` — bounds do not translate | unpartitioned |
| 4b.8 | PG → Oracle | RANGE on `created` | `unsupported` | unpartitioned |
| 4b.9 | SQL Server → SQL Server, secondary filegroup | `df_fg` | `carried` | table is on `df_fg` |
| 4b.10 | PG → SQL Server | RANGE on `created` | `unsupported` | unpartitioned, on `PRIMARY` |

Rules this pins:

- a partition scheme is only carried when the source **bounds** were read as
  well: a partitioned parent with no children accepts no row;
- PostgreSQL requires every unique constraint to contain the partition key, so
  a scheme that would cost the PRIMARY KEY is refused rather than carried;
- cross-engine partitioning is never invented — bounds and strategy semantics
  do not translate, and the certificate says so instead of reporting a heap as
  faithful;
- a tablespace/filegroup is only named on CREATE when the **destination**
  catalog lists it; when the destination catalog cannot be read the aspect is
  `unknown`, never "absent";
- `carried` is written only after the destination catalog re-read agrees with
  the source; a re-read that disagrees downgrades to `unsupported`, and a
  re-read that fails downgrades to `unknown`.

Two defects this matrix caught, both invisible to unit tests: child partitions
were created under the *source* child names, which `CREATE TABLE IF NOT EXISTS`
turns into a silent no-op when source and destination share a schema (parent
with zero partitions, every row quarantined); and the SQL Server probe reported
"default filegroup" for any table not on a partition scheme, so a table
deliberately placed on a secondary filegroup was never carried.

Still not carried on create-new: SQL Server partition functions/schemes, Oracle
partitioned tables and per-partition tablespaces, PostgreSQL `CLUSTER`
ordering. Those report `unsupported` with the reason, which is honest but not
finished.

## 5. What the operator is shown

Produced today: mapping pairs with confidence and fidelity risk per column,
coercion preview with per-column sampled/NULLed/failed counts, 13 validation
gates with scope (sample vs full-selected), Decision Artifact hash, Map→DDL
fingerprint, pre-write Gate-8 simulation explicitly marked "not migration
proven", post-write row-count + checksum proof, signed proof pack and audit
PDF, and quarantine/rejected-row detail.

Not produced today, and needed for the scenarios above:

- a **mapping-shape panel**: source column count vs destination column count,
  mapped pairs, declared omissions, destination-only columns and how each will
  be filled (default / generated / NULL / blocked);
- a Risk-Contract path for accepted-lossy conversions such as 2.3.

## 6. Gap list (ordered by severity)

| ID | Gap | Severity | Where |
|----|-----|----------|-------|
| GAP-1 | ~~Unmapped source columns are dropped and the run is green~~ **fixed** — `g13_source_coverage` hard gate | Was critical — silent data loss | `services/source_coverage_gate.py` |
| GAP-4 | ~~Untouched destination columns break Gate-8 checksum *after* rows are committed~~ **fixed** — read-back digests the mapped projection | Was high | `services/readback_projection.py` |
| GAP-2 | ~~Declared `intentional_omit` columns are transform-checked and refuse the run~~ **fixed** — omissions excluded from every write-path probe | Was high | `services/mapping_constraints.write_mappings` call sites |
| GAP-3 | ~~Destination `NOT NULL`-without-default is caught by the engine, not predicted~~ **fixed** — `g14_destination_requirements` hard gate | Was medium | `services/destination_requirements_gate.py` |
| GAP-6 | `TEXT → TEXT` reported as fidelity collapse | Medium — false positive erodes trust | type fidelity classifier |
| GAP-5 | Oracle `CLOB` not accepted as a JSON carrier | Low | type compatibility table |
| GAP-7 | No Risk-Contract path for accepted timezone loss | Low | conversion contract wiring |

None of these are route-specific: every one reproduces identically on
PostgreSQL, MySQL and Oracle, which is why they are listed as algorithm gaps
rather than connector bugs.

## 7. Not yet measured

Scenarios deliberately absent from this run, so they must not be claimed:
generated/identity destination columns, CHECK-constraint rejection on append,
sequence high-water marks,
SQL Server and SQLite for sections 1–2, warehouse
and SaaS destinations (credentials not provisioned), and quarantine/replay
row-count invariants under partial failure.


## 8. Foreign keys across a multi-table transfer (measured 2026-08-10)

Harness `foreign_key_live.py`, artifact
`/home/ubuntu/repro/foreign_key_live_results.json` — 9/9 cases on live
PostgreSQL 16, MySQL 8, SQL Server 2022 and Oracle Free.

| Case | Engine | Verdict |
|------|--------|---------|
| Parent/child, child listed first | PG | parents loaded first; key `carried` after catalog re-read; orphan INSERT rejected |
| Three-level chain | PG | order `customers → orders → lines`; both keys carried |
| Self-referential key | PG | carried; the self-reference does not affect ordering |
| Child rows with no parent | PG | `ALTER` rejected → `integrity_violation`, verdict `referential_integrity_violated` |
| Referenced table neither transferred nor present | PG | `unsupported`, naming the missing table |
| Mutual cycle A→B→A | PG | reported as a cycle; no invented order |
| Parent/child | MySQL | carried; orphan INSERT rejected (errno 1452) |
| Parent/child | SQL Server | carried; orphan INSERT rejected (msg 547) |
| Parent/child | Oracle | carried; orphan INSERT rejected (ORA-02291) |

`carried` is written only after re-reading the destination catalog, and the
`ALTER TABLE … ADD CONSTRAINT` is issued after the load so the engine validates
the rows that were actually written — a row count and a checksum both report
green on an orphaned child table.

Two defects this matrix found, both invisible to unit tests:

- **Constraint names are schema-scoped** on MySQL, SQL Server and Oracle, so
  reusing the source constraint name failed the `ALTER` (`errno 1826`, msg
  2714) whenever source and destination shared a schema. Destination names are
  now derived from the destination table and key columns.
- **Uppercase source columns lost their PRIMARY KEY and NOT NULL on
  create-new** — every Oracle source. The fidelity planner case-folded the
  column names it planned (`ID` → `id`), the writer created them verbatim, and
  the writer's exact-match guard then dropped the constraints while the
  certificate still read `carried` (proved by ORA-02270 when the foreign key
  looked for the parent key). The planner now preserves case, and the writer
  resolves plan names against the columns it is creating.

Still not carried: cyclic graphs are reported, not resolved with deferred
constraints; referenced-column mapping assumes the parent key keeps its source
spelling; cross-schema references land in the child's schema; SQLite cannot
take a post-load `ALTER` and is refused.
